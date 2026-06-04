//! Batch writer for the `samples` hypertable (scalar numeric data).
//!
//! Handles 95% of all writes: NTScalar (numeric), NTEnum, NTAggregate.
//!
//! # Write Strategy
//!
//! All writes use `COPY FROM STDIN` binary protocol via dedicated
//! `tokio-postgres` connections. Bypasses SQL parsing entirely.
//! When multiple connections are available, chunks are sent in parallel -
//! each on its own TCP socket with its own fsync.
//!
//! # Backpressure
//!
//! Hard memory limit (`max_buffer_bytes`, default 64 MB) prevents OOM.
//! `push()` returns [`PushResult`] to signal the caller.
//!
//! # Binary Row Layout (on the wire)
//!
//! ```text
//! num_cols(2) + [len(4)+time(8)] + [len(4)+pv_id(4)] + [len(4)+value(8)]
//!             + [len(4)+severity(2)] + [len(4)+status(2)] + [len(4)+reason(2)]
//! = 2 + 12 + 8 + 12 + 6 + 6 + 6 = 52 bytes per row
//! ```

use chrono::{DateTime, Utc};
use std::fmt;

use aura_core::error::{AuraError, AuraResult};
use aura_core::sample::StoreReason;

/// Default maximum buffer memory (64 MB).
const DEFAULT_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Exact in-memory size of ScalarRow (pg_us:8 + pv_id:4 + value:8 + severity:2 + status:2 + reason:2 + pad:6 = 32).
const ROW_MEM_SIZE: usize = size_of::<ScalarRow>();

/// Binary COPY: bytes per row on the wire.
const WIRE_ROW_SIZE: usize = 52;

use super::copy_pool::{PG_EPOCH_OFFSET_US, PGCOPY_HEADER, PGCOPY_TRAILER};

/// A single row for the `samples` table.
///
/// Fixed-size, zero heap allocations, `Copy`-able.
#[derive(Debug, Clone, Copy)]
pub struct ScalarRow {
    /// PostgreSQL microseconds (since 2000-01-01). Pre-computed for zero-cost COPY encoding.
    pub pg_us: i64,
    pub pv_id: i32,
    pub value: f64,
    pub severity: i16,
    pub status: i16,
    pub reason: i16,
}

impl ScalarRow {
    #[inline]
    pub fn new(
        time: DateTime<Utc>,
        pv_id: i32,
        value: f64,
        severity: i16,
        status: i16,
        reason: StoreReason,
    ) -> Self {
        Self {
            pg_us: time.timestamp_micros() - PG_EPOCH_OFFSET_US,
            pv_id,
            value,
            severity,
            status,
            reason: reason as i16,
        }
    }

    /// Create from raw Unix epoch (hot path — zero DateTime overhead).
    #[inline]
    pub fn from_epoch(
        seconds: i64,
        nanos: i32,
        pv_id: i32,
        value: f64,
        severity: i16,
        status: i16,
        reason: StoreReason,
    ) -> Self {
        Self {
            pg_us: seconds * 1_000_000 + (nanos / 1000) as i64 - PG_EPOCH_OFFSET_US,
            pv_id,
            value,
            severity,
            status,
            reason: reason as i16,
        }
    }

    /// Construct from pre-computed PostgreSQL microseconds.
    /// Used by the heartbeat tracker to avoid redundant epoch conversion.
    #[inline]
    pub fn from_parts(
        pg_us: i64,
        pv_id: i32,
        value: f64,
        severity: i16,
        status: i16,
        reason: StoreReason,
    ) -> Self {
        Self {
            pg_us,
            pv_id,
            value,
            severity,
            status,
            reason: reason as i16,
        }
    }

    /// Whether the value is NaN or Infinity.
    #[inline]
    pub fn has_non_finite(&self) -> bool {
        !self.value.is_finite()
    }

    /// Approximate in-memory size (constant).
    #[inline]
    pub const fn mem_size() -> usize {
        ROW_MEM_SIZE
    }

    /// Encode this row into the COPY binary format (52 bytes).
    #[inline]
    fn encode_copy(&self, out: &mut [u8; WIRE_ROW_SIZE]) {
        const TEMPLATE: [u8; WIRE_ROW_SIZE] = {
            let mut t = [0u8; WIRE_ROW_SIZE];
            t[0] = 0;
            t[1] = 6; // num_columns = 6
            t[2] = 0;
            t[3] = 0;
            t[4] = 0;
            t[5] = 8; // time len = 8
            t[14] = 0;
            t[15] = 0;
            t[16] = 0;
            t[17] = 4; // pv_id len = 4
            t[22] = 0;
            t[23] = 0;
            t[24] = 0;
            t[25] = 8; // value len = 8
            t[34] = 0;
            t[35] = 0;
            t[36] = 0;
            t[37] = 2; // severity len = 2
            t[40] = 0;
            t[41] = 0;
            t[42] = 0;
            t[43] = 2; // status len = 2
            t[46] = 0;
            t[47] = 0;
            t[48] = 0;
            t[49] = 2; // reason len = 2
            t
        };

        *out = TEMPLATE;
        out[6..14].copy_from_slice(&self.pg_us.to_be_bytes());
        out[18..22].copy_from_slice(&self.pv_id.to_be_bytes());
        out[26..34].copy_from_slice(&self.value.to_be_bytes());
        out[38..40].copy_from_slice(&self.severity.to_be_bytes());
        out[44..46].copy_from_slice(&self.status.to_be_bytes());
        out[50..52].copy_from_slice(&self.reason.to_be_bytes());
    }
}

impl fmt::Display for ScalarRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pv_id={} value={} sev={} reason={}",
            self.pv_id, self.value, self.severity, self.reason
        )
    }
}

pub use super::copy_pool::PushResult;

/// High-throughput batch writer for scalar numeric samples.
///
/// Accumulates [`ScalarRow`]s in memory, then flushes to TimescaleDB via
/// binary `COPY` on dedicated `tokio-postgres` connections. Supports
/// parallel COPY across N connections for N× throughput.
pub struct ScalarWriter {
    batch_size: usize,
    max_buffer_bytes: usize,
    pub(crate) buffer: Vec<ScalarRow>,
    copy_pool: Option<super::copy_pool::CopyPool>,

    total_written: u64,
    total_flushes: u64,
    total_backpressure: u64,
    pub(crate) total_non_finite: u64,
    total_build_us: u64,
    total_send_us: u64,
    /// Reusable binary COPY buffer — avoids allocation per flush.
    copy_buf: Vec<u8>,
}

impl ScalarWriter {
    pub fn new(batch_size: usize) -> Self {
        Self::with_limits(batch_size, DEFAULT_MAX_BUFFER_BYTES)
    }

    pub fn with_limits(batch_size: usize, max_buffer_bytes: usize) -> Self {
        let batch_size = batch_size.max(1);
        let max_buffer_bytes = max_buffer_bytes.max(ROW_MEM_SIZE);
        Self {
            batch_size,
            max_buffer_bytes,
            buffer: Vec::with_capacity(batch_size),
            copy_pool: None,
            total_written: 0,
            total_flushes: 0,
            total_backpressure: 0,
            total_non_finite: 0,
            total_build_us: 0,
            total_send_us: 0,
            copy_buf: Vec::with_capacity(batch_size * WIRE_ROW_SIZE + 64),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(500)
    }

    pub fn set_copy_pool(&mut self, pool: super::copy_pool::CopyPool) {
        self.copy_pool = Some(pool);
    }

    /// Push a single row. Returns buffer state for the caller to decide when to flush.
    #[inline]
    pub fn push(&mut self, row: ScalarRow) -> PushResult {
        if !self.buffer.is_empty()
            && self.buffer.len() * ROW_MEM_SIZE + ROW_MEM_SIZE > self.max_buffer_bytes
        {
            self.total_backpressure += 1;
            return PushResult::BackpressureExceeded;
        }

        if row.has_non_finite() {
            self.total_non_finite += 1;
        }
        self.buffer.push(row);

        if self.buffer.len() >= self.batch_size {
            PushResult::Full
        } else {
            PushResult::Accepted
        }
    }

    /// Flush all buffered rows to TimescaleDB via binary COPY.
    pub async fn flush(&mut self) -> AuraResult<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let pool = self
            .copy_pool
            .as_ref()
            .ok_or_else(|| AuraError::database("ScalarWriter: no copy pool configured"))?;
        let count = self.buffer.len();
        let n = pool.len();

        const PARALLEL_THRESHOLD: usize = 10_000;

        if count >= PARALLEL_THRESHOLD && n > 1 {
            let chunk_size = (count + n - 1) / n;

            let t0 = std::time::Instant::now();
            let payloads: Vec<_> = self
                .buffer
                .chunks(chunk_size)
                .map(|chunk| (Self::build_copy_payload(chunk), chunk.len()))
                .collect();
            self.total_build_us += t0.elapsed().as_micros() as u64;

            let t1 = std::time::Instant::now();
            pool.send_parallel(Self::COPY_SQL, payloads).await?;
            self.total_send_us += t1.elapsed().as_micros() as u64;
        } else {
            let t0 = std::time::Instant::now();
            Self::build_copy_into(&mut self.copy_buf, &self.buffer);
            self.total_build_us += t0.elapsed().as_micros() as u64;

            let t1 = std::time::Instant::now();
            let payload = bytes::Bytes::copy_from_slice(&self.copy_buf);
            pool.send_copy_bytes(0, Self::COPY_SQL, payload, count)
                .await?;
            self.total_send_us += t1.elapsed().as_micros() as u64;
        }

        self.total_written += count as u64;
        self.total_flushes += 1;
        self.buffer.clear();
        Ok(count)
    }

    /// Build COPY payload directly into a provided buffer (zero allocation on reuse).
    pub(crate) fn build_copy_into(buf: &mut Vec<u8>, rows: &[ScalarRow]) {
        let total = PGCOPY_HEADER.len() + rows.len() * WIRE_ROW_SIZE + PGCOPY_TRAILER.len();
        buf.clear();
        buf.resize(total, 0);

        buf[..PGCOPY_HEADER.len()].copy_from_slice(&PGCOPY_HEADER);
        let mut off = PGCOPY_HEADER.len();
        for row in rows {
            row.encode_copy((&mut buf[off..off + WIRE_ROW_SIZE]).try_into().unwrap());
            off += WIRE_ROW_SIZE;
        }
        debug_assert_eq!(off + PGCOPY_TRAILER.len(), total);
        buf[off..].copy_from_slice(&PGCOPY_TRAILER);
    }

    /// Allocating version for parallel chunks (each chunk needs its own buffer).
    pub(crate) fn build_copy_payload(rows: &[ScalarRow]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            PGCOPY_HEADER.len() + rows.len() * WIRE_ROW_SIZE + PGCOPY_TRAILER.len(),
        );
        Self::build_copy_into(&mut buf, rows);
        buf
    }

    pub(crate) const COPY_SQL: &'static str = "COPY samples (time, pv_id, value, severity, status, reason) FROM STDIN WITH (FORMAT binary)";

    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.batch_size
    }

    #[inline]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[inline]
    pub fn max_buffer_bytes(&self) -> usize {
        self.max_buffer_bytes
    }

    #[inline]
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len() * ROW_MEM_SIZE
    }

    #[inline]
    pub fn total_written(&self) -> u64 {
        self.total_written
    }

    #[inline]
    pub fn total_flushes(&self) -> u64 {
        self.total_flushes
    }

    #[inline]
    pub fn total_backpressure(&self) -> u64 {
        self.total_backpressure
    }

    #[inline]
    pub fn total_non_finite(&self) -> u64 {
        self.total_non_finite
    }

    #[inline]
    pub fn total_build_us(&self) -> u64 {
        self.total_build_us
    }

    #[inline]
    pub fn total_send_us(&self) -> u64 {
        self.total_send_us
    }

    #[inline]
    pub fn pool_size(&self) -> usize {
        self.copy_pool.as_ref().map_or(0, |p| p.len())
    }

    /// Memory utilization (0.0 - 1.0).
    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 {
            return 0.0;
        }
        self.buffered_bytes() as f64 / self.max_buffer_bytes as f64
    }

    /// Maximum rows before backpressure triggers.
    pub fn max_rows(&self) -> usize {
        self.max_buffer_bytes / ROW_MEM_SIZE
    }

    /// Average rows per flush (0.0 if no flushes yet).
    pub fn avg_batch_size(&self) -> f64 {
        if self.total_flushes == 0 {
            return 0.0;
        }
        self.total_written as f64 / self.total_flushes as f64
    }

    /// Clear the buffer without flushing to the database.
    pub fn discard(&mut self) {
        self.buffer.clear();
    }

    #[inline]
    pub fn buffer_len(&self) -> usize {
        self.len()
    }

    #[inline]
    pub fn buffered(&self) -> usize {
        self.len()
    }

    #[inline]
    pub fn bg_flush_count(&self) -> usize {
        0
    }
}

impl fmt::Debug for ScalarWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarWriter")
            .field("buffered", &self.len())
            .field("batch_size", &self.batch_size)
            .field(
                "pressure",
                &format_args!("{:.1}%", self.memory_pressure() * 100.0),
            )
            .field("written", &self.total_written)
            .field("flushes", &self.total_flushes)
            .field("non_finite", &self.total_non_finite)
            .field("backpressure", &self.total_backpressure)
            .field("pool_size", &self.pool_size())
            .finish()
    }
}

impl fmt::Display for ScalarWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ScalarWriter: {}/{} buffered ({:.1} KB/{:.1} MB, {:.0}% pressure), \
             {} written (avg {:.0}), {} flushes, \
             {} non-finite, {} backpressure, pool={}",
            self.len(),
            self.batch_size,
            self.buffered_bytes() as f64 / 1024.0,
            self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
            self.memory_pressure() * 100.0,
            self.total_written,
            self.avg_batch_size(),
            self.total_flushes,
            self.total_non_finite,
            self.total_backpressure,
            self.pool_size(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(secs: i64, us: u32) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, us * 1000).unwrap()
    }

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    fn row(pv: i32, val: f64) -> ScalarRow {
        ScalarRow::new(now(), pv, val, 0, 0, StoreReason::ValueChanged)
    }

    fn row_full(
        time: DateTime<Utc>,
        pv: i32,
        val: f64,
        sev: i16,
        st: i16,
        reason: StoreReason,
    ) -> ScalarRow {
        ScalarRow::new(time, pv, val, sev, st, reason)
    }

    #[test]
    fn row_new() {
        let r = row(42, 4.217);
        assert_eq!(r.pv_id, 42);
        assert_eq!(r.value, 4.217);
        assert_eq!(r.reason, StoreReason::ValueChanged as i16);
    }

    #[test]
    fn row_reason_mapping() {
        for reason in StoreReason::ALL {
            let r = row_full(now(), 1, 0.0, 0, 0, reason);
            assert_eq!(r.reason, reason as i16);
        }
    }

    #[test]
    fn row_non_finite() {
        assert!(!row(1, 0.0).has_non_finite());
        assert!(row(1, f64::NAN).has_non_finite());
        assert!(row(1, f64::INFINITY).has_non_finite());
        assert!(row(1, f64::NEG_INFINITY).has_non_finite());
    }

    #[test]
    fn row_mem_size() {
        assert_eq!(ScalarRow::mem_size(), ROW_MEM_SIZE);
        assert!(ROW_MEM_SIZE >= 26);
    }

    #[test]
    fn row_is_copy() {
        let a = row(1, 10.0);
        let b = a;
        assert_eq!(a.pv_id, b.pv_id);
    }

    #[test]
    fn row_display() {
        assert!(row(42, 4.217).to_string().contains("pv_id=42"));
    }

    #[test]
    fn push_result_semantics() {
        assert!(!PushResult::Accepted.needs_flush());
        assert!(PushResult::Accepted.is_accepted());
        assert!(PushResult::Full.needs_flush());
        assert!(PushResult::Full.is_accepted());
        assert!(PushResult::BackpressureExceeded.needs_flush());
        assert!(!PushResult::BackpressureExceeded.is_accepted());
    }

    #[test]
    fn new_defaults() {
        let w = ScalarWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.batch_size(), 500);
        assert_eq!(w.max_buffer_bytes(), DEFAULT_MAX_BUFFER_BYTES);
        assert_eq!(w.total_written(), 0);
        assert_eq!(w.total_flushes(), 0);
    }

    #[test]
    fn min_batch_size_clamped() {
        assert_eq!(ScalarWriter::new(0).batch_size(), 1);
    }
    #[test]
    fn min_buffer_clamped() {
        assert_eq!(
            ScalarWriter::with_limits(10, 0).max_buffer_bytes(),
            ROW_MEM_SIZE
        );
    }

    #[test]
    fn push_accepted() {
        let mut w = ScalarWriter::with_defaults();
        assert_eq!(w.push(row(1, 10.0)), PushResult::Accepted);
        assert_eq!(w.len(), 1);
    }

    #[test]
    fn push_until_full() {
        let mut w = ScalarWriter::new(3);
        w.push(row(1, 1.0));
        w.push(row(2, 2.0));
        assert_eq!(w.push(row(3, 3.0)), PushResult::Full);
    }

    #[test]
    fn push_tracks_non_finite() {
        let mut w = ScalarWriter::with_defaults();
        w.push(row(1, 1.0));
        w.push(row(2, f64::NAN));
        w.push(row(3, f64::INFINITY));
        assert_eq!(w.total_non_finite(), 2);
    }

    #[test]
    fn backpressure_triggered() {
        let mut w = ScalarWriter::with_limits(100, ROW_MEM_SIZE * 2);
        w.push(row(1, 1.0));
        w.push(row(2, 2.0));
        assert_eq!(w.push(row(3, 3.0)), PushResult::BackpressureExceeded);
        assert_eq!(w.len(), 2);
        assert_eq!(w.total_backpressure(), 1);
    }

    #[test]
    fn backpressure_first_row_always_accepted() {
        let mut w = ScalarWriter::with_limits(100, 1);
        assert!(w.push(row(1, 1.0)).is_accepted());
    }

    #[test]
    fn backpressure_clears_after_discard() {
        let mut w = ScalarWriter::with_limits(100, ROW_MEM_SIZE * 2);
        w.push(row(1, 1.0));
        w.push(row(2, 2.0));
        w.discard();
        assert_eq!(w.push(row(3, 3.0)), PushResult::Accepted);
    }

    #[test]
    fn memory_pressure_fraction() {
        let mut w = ScalarWriter::with_limits(100, ROW_MEM_SIZE * 10);
        w.push(row(1, 1.0));
        assert!((w.memory_pressure() - 0.1).abs() < 0.01);
    }

    #[test]
    fn discard_clears_buffer() {
        let mut w = ScalarWriter::with_defaults();
        w.push(row(1, 1.0));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.total_written(), 0);
    }

    #[test]
    fn compatibility_aliases() {
        let mut w = ScalarWriter::with_defaults();
        w.push(row(1, 1.0));
        assert_eq!(w.buffer_len(), w.len());
        assert_eq!(w.buffered(), w.len());
        assert_eq!(w.bg_flush_count(), 0);
    }

    #[test]
    fn payload_header_and_trailer() {
        let buf = ScalarWriter::build_copy_payload(&[row(1, 42.0)]);
        assert_eq!(&buf[..11], b"PGCOPY\n\xff\r\n\x00");
        assert_eq!(&buf[buf.len() - 2..], &PGCOPY_TRAILER);
    }

    #[test]
    fn payload_exact_size() {
        for n in [1, 10, 100, 1000] {
            let rows: Vec<_> = (0..n).map(|i| row(i as i32, i as f64)).collect();
            let buf = ScalarWriter::build_copy_payload(&rows);
            assert_eq!(
                buf.len(),
                PGCOPY_HEADER.len() + n * WIRE_ROW_SIZE + PGCOPY_TRAILER.len()
            );
        }
    }

    #[test]
    fn payload_empty() {
        let buf = ScalarWriter::build_copy_payload(&[]);
        assert_eq!(buf.len(), PGCOPY_HEADER.len() + PGCOPY_TRAILER.len());
    }

    #[test]
    fn payload_encodes_timestamp() {
        let time = ts(1781617845, 123456);
        let r = row_full(time, 1, 0.0, 0, 0, StoreReason::Initial);
        let buf = ScalarWriter::build_copy_payload(&[r]);
        let pg_us = i64::from_be_bytes(buf[25..33].try_into().unwrap());
        assert_eq!(pg_us, time.timestamp_micros() - PG_EPOCH_OFFSET_US);
    }

    #[test]
    fn payload_encodes_pv_id() {
        let r = row_full(now(), 12345, 0.0, 0, 0, StoreReason::Initial);
        let buf = ScalarWriter::build_copy_payload(&[r]);
        assert_eq!(i32::from_be_bytes(buf[37..41].try_into().unwrap()), 12345);
    }

    #[test]
    fn payload_encodes_value() {
        let r = row_full(now(), 1, std::f64::consts::PI, 0, 0, StoreReason::Initial);
        let buf = ScalarWriter::build_copy_payload(&[r]);
        assert_eq!(
            f64::from_be_bytes(buf[45..53].try_into().unwrap()),
            std::f64::consts::PI
        );
    }

    #[test]
    fn payload_encodes_severity_status_reason() {
        let r = row_full(now(), 1, 0.0, 3, 7, StoreReason::AlarmChange);
        let buf = ScalarWriter::build_copy_payload(&[r]);
        assert_eq!(i16::from_be_bytes(buf[57..59].try_into().unwrap()), 3);
        assert_eq!(i16::from_be_bytes(buf[63..65].try_into().unwrap()), 7);
        assert_eq!(
            i16::from_be_bytes(buf[69..71].try_into().unwrap()),
            StoreReason::AlarmChange as i16
        );
    }

    #[test]
    fn payload_multi_row_continuity() {
        let rows: Vec<_> = (0..3).map(|i| row(i, i as f64 * 10.0)).collect();
        let buf = ScalarWriter::build_copy_payload(&rows);
        for (i, expected_pv) in [0i32, 1, 2].iter().enumerate() {
            let base = PGCOPY_HEADER.len() + i * WIRE_ROW_SIZE;
            assert_eq!(
                i32::from_be_bytes(buf[base + 18..base + 22].try_into().unwrap()),
                *expected_pv
            );
        }
    }

    #[test]
    fn encode_copy_deterministic() {
        let r = row_full(ts(1700000000, 0), 42, 3.14, 2, 1, StoreReason::Heartbeat);
        let mut a = [0u8; WIRE_ROW_SIZE];
        let mut b = [0u8; WIRE_ROW_SIZE];
        r.encode_copy(&mut a);
        r.encode_copy(&mut b);
        assert_eq!(a, b);
    }

    #[test]
    fn constants_sane() {
        assert_eq!(PGCOPY_HEADER.len(), 19);
        assert_eq!(PGCOPY_TRAILER, [0xff, 0xff]);
        assert_eq!(WIRE_ROW_SIZE, 52);
        assert_eq!(PG_EPOCH_OFFSET_US, 946_684_800 * 1_000_000);
    }

    #[test]
    fn large_batch_payload() {
        let rows: Vec<_> = (0..10_000).map(|i| row(i % 100, i as f64)).collect();
        let buf = ScalarWriter::build_copy_payload(&rows);
        assert_eq!(buf.len(), 19 + 10_000 * 52 + 2);
    }

    #[test]
    fn display() {
        let s = ScalarWriter::with_defaults().to_string();
        assert!(s.contains("0/500") && s.contains("0 backpressure"));
    }

    #[test]
    fn display_with_stats() {
        let mut w = ScalarWriter::with_defaults();
        w.total_written = 10_000;
        w.total_flushes = 20;
        let s = w.to_string();
        assert!(s.contains("10000 written"));
        assert!(s.contains("20 flushes"));
    }

    #[test]
    fn debug_output() {
        assert!(format!("{:?}", ScalarWriter::with_defaults()).contains("ScalarWriter"));
    }
}