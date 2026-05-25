//! Batch writer for the `samples_string` hypertable.
//!
//! Handles NTScalar with String values. Separated from numeric scalars
//! because TEXT columns use LZ4 compression (vs gorilla for floats).
//!
//! # Write Strategy
//!
//! Binary COPY via the shared [`CopyPool`](CopyPool).
//! Variable-length TEXT values are encoded as `len(4) + utf8_bytes`.
//! No text escaping needed — binary format handles all byte values.
//!
//! # Binary Row Layout (on the wire)
//!
//! ```text
//! num_cols(2) + [4+time(8)] + [4+pv_id(4)] + [4+value(N)] + [4+sev(2)] + [4+status(2)]
//! = 38 + N bytes per row (N = string length in bytes)
//! ```

use chrono::{DateTime, Utc};
use std::fmt;

use super::copy_pool::{CopyPool, PG_EPOCH_OFFSET_US, PGCOPY_HEADER, PGCOPY_TRAILER, PushResult};
use aura_core::error::{AuraError, AuraResult};

/// Default maximum buffer memory.
const DEFAULT_MAX_BUFFER_BYTES: usize = 32 * 1024 * 1024;

/// Number of columns in the COPY.
const NUM_COLUMNS: i16 = 5;

/// Threshold for parallel flush: above this, split across pool connections.
const PARALLEL_THRESHOLD: usize = 10_000;

/// COPY SQL for binary format.
const COPY_SQL: &str =
    "COPY samples_string (time, pv_id, value, severity, status) FROM STDIN WITH (FORMAT binary)";

/// A single row for the `samples_string` table.
#[derive(Debug, Clone)]
pub struct StringRow {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub value: String,
    pub severity: i16,
    pub status: i16,
}

impl StringRow {
    #[inline]
    pub fn new(time: DateTime<Utc>, pv_id: i32, value: String, severity: i16, status: i16) -> Self {
        Self {
            time,
            pv_id,
            value,
            severity,
            status,
        }
    }

    #[inline]
    pub fn value_len(&self) -> usize {
        self.value.len()
    }
    #[inline]
    pub fn is_value_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Approximate heap memory used by this row.
    #[inline]
    pub fn mem_size(&self) -> usize {
        40 + self.value.len()
    }

    /// Wire size for binary COPY: 38 fixed + string length.
    #[inline]
    fn wire_size(&self) -> usize {
        38 + self.value.len()
    }

    /// Encode this row into COPY binary format.
    ///
    /// Appends directly to `buf` — no intermediate allocation.
    #[inline]
    fn encode_copy(&self, buf: &mut Vec<u8>) {
        let pg_us = self.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
        let val_bytes = self.value.as_bytes();

        // num_columns
        buf.extend_from_slice(&NUM_COLUMNS.to_be_bytes());
        // time: len=8, value=i64
        buf.extend_from_slice(&8i32.to_be_bytes());
        buf.extend_from_slice(&pg_us.to_be_bytes());
        // pv_id: len=4, value=i32
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf.extend_from_slice(&self.pv_id.to_be_bytes());
        // value: len=N, value=utf8 bytes (TEXT in binary COPY)
        buf.extend_from_slice(&(val_bytes.len() as i32).to_be_bytes());
        buf.extend_from_slice(val_bytes);
        // severity: len=2, value=i16
        buf.extend_from_slice(&2i32.to_be_bytes());
        buf.extend_from_slice(&self.severity.to_be_bytes());
        // status: len=2, value=i16
        buf.extend_from_slice(&2i32.to_be_bytes());
        buf.extend_from_slice(&self.status.to_be_bytes());
    }
}

impl fmt::Display for StringRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.value.len() <= 32 {
            write!(f, "pv_id={} \"{}\"", self.pv_id, self.value)
        } else {
            write!(
                f,
                "pv_id={} \"{}...\" ({} bytes)",
                self.pv_id,
                &self.value[..32],
                self.value.len()
            )
        }
    }
}

/// High-throughput batch writer for string samples.
///
/// Uses binary COPY via the shared [`CopyPool`]. For workloads above
/// [`PARALLEL_THRESHOLD`], splits across multiple pool connections.
pub struct StringWriter {
    batch_size: usize,
    max_buffer_bytes: usize,
    buffer: Vec<StringRow>,
    current_bytes: usize,

    copy_pool: Option<CopyPool>,

    total_written: u64,
    total_flushes: u64,
    total_value_bytes: u64,
    total_backpressure: u64,
    total_build_us: u64,
    total_send_us: u64,
}

impl StringWriter {
    pub fn new(batch_size: usize) -> Self {
        Self::with_limits(batch_size, DEFAULT_MAX_BUFFER_BYTES)
    }

    pub fn with_limits(batch_size: usize, max_buffer_bytes: usize) -> Self {
        let batch_size = batch_size.max(1);
        let max_buffer_bytes = max_buffer_bytes.max(1024);
        Self {
            batch_size,
            max_buffer_bytes,
            buffer: Vec::with_capacity(batch_size),
            current_bytes: 0,
            copy_pool: None,
            total_written: 0,
            total_flushes: 0,
            total_value_bytes: 0,
            total_backpressure: 0,
            total_build_us: 0,
            total_send_us: 0,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(500)
    }

    /// Set the shared COPY pool.
    pub fn set_copy_pool(&mut self, pool: CopyPool) {
        self.copy_pool = Some(pool);
    }

    #[inline]
    pub fn push(&mut self, row: StringRow) -> PushResult {
        let row_bytes = row.mem_size();

        if !self.buffer.is_empty() && self.current_bytes + row_bytes > self.max_buffer_bytes {
            self.total_backpressure += 1;
            return PushResult::BackpressureExceeded;
        }

        self.current_bytes += row_bytes;
        self.buffer.push(row);

        if self.buffer.len() >= self.batch_size {
            PushResult::Full
        } else {
            PushResult::Accepted
        }
    }

    /// Flush all buffered rows via binary COPY.
    ///
    /// If buffer > [`PARALLEL_THRESHOLD`] and pool has multiple connections,
    /// splits into N chunks for parallel COPY (same strategy as scalar).
    pub async fn flush(&mut self) -> AuraResult<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let pool = self
            .copy_pool
            .as_ref()
            .ok_or_else(|| AuraError::database("StringWriter: no copy pool configured"))?;
        let count = self.buffer.len();
        let n_conn = pool.len();

        let t0 = std::time::Instant::now();

        if count >= PARALLEL_THRESHOLD && n_conn > 1 {
            // Parallel: split across connections.
            let chunk_size = (count + n_conn - 1) / n_conn;
            let payloads: Vec<_> = self
                .buffer
                .chunks(chunk_size)
                .map(|chunk| (Self::build_copy_payload(chunk), chunk.len()))
                .collect();
            self.total_build_us += t0.elapsed().as_micros() as u64;

            let t1 = std::time::Instant::now();
            pool.send_parallel(COPY_SQL, payloads).await?;
            self.total_send_us += t1.elapsed().as_micros() as u64;
        } else {
            // Single connection.
            let payload = Self::build_copy_payload(&self.buffer);
            self.total_build_us += t0.elapsed().as_micros() as u64;

            let t1 = std::time::Instant::now();
            pool.send_copy(0, COPY_SQL, payload, count).await?;
            self.total_send_us += t1.elapsed().as_micros() as u64;
        }

        let value_bytes: u64 = self.buffer.iter().map(|r| r.value.len() as u64).sum();
        self.total_value_bytes += value_bytes;
        self.total_written += count as u64;
        self.total_flushes += 1;
        self.buffer.clear();
        self.current_bytes = 0;
        Ok(count)
    }

    /// Build binary COPY payload from a slice of rows.
    fn build_copy_payload(rows: &[StringRow]) -> Vec<u8> {
        let wire_total: usize = rows.iter().map(|r| r.wire_size()).sum();
        let capacity = PGCOPY_HEADER.len() + wire_total + PGCOPY_TRAILER.len();
        let mut buf = Vec::with_capacity(capacity);

        buf.extend_from_slice(&PGCOPY_HEADER);
        for row in rows {
            row.encode_copy(&mut buf);
        }
        buf.extend_from_slice(&PGCOPY_TRAILER);

        buf
    }

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
        self.current_bytes
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
    pub fn total_value_bytes(&self) -> u64 {
        self.total_value_bytes
    }
    #[inline]
    pub fn total_backpressure(&self) -> u64 {
        self.total_backpressure
    }
    #[inline]
    pub fn total_build_us(&self) -> u64 {
        self.total_build_us
    }
    #[inline]
    pub fn total_send_us(&self) -> u64 {
        self.total_send_us
    }

    // Compatibility aliases
    #[inline]
    pub fn buffered(&self) -> usize {
        self.len()
    }
    #[inline]
    pub fn copy_flushes(&self) -> u64 {
        self.total_flushes
    }
    #[inline]
    pub fn insert_flushes(&self) -> u64 {
        0
    }

    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 {
            return 0.0;
        }
        self.current_bytes as f64 / self.max_buffer_bytes as f64
    }

    pub fn avg_value_len(&self) -> f64 {
        if self.total_written == 0 {
            return 0.0;
        }
        self.total_value_bytes as f64 / self.total_written as f64
    }

    pub fn avg_batch_size(&self) -> f64 {
        if self.total_flushes == 0 {
            return 0.0;
        }
        self.total_written as f64 / self.total_flushes as f64
    }

    pub fn discard(&mut self) {
        self.buffer.clear();
        self.current_bytes = 0;
    }
}


impl fmt::Debug for StringWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StringWriter")
            .field("buffered", &self.len())
            .field("batch_size", &self.batch_size)
            .field(
                "pressure",
                &format_args!("{:.1}%", self.memory_pressure() * 100.0),
            )
            .field("written", &self.total_written)
            .field("flushes", &self.total_flushes)
            .field("backpressure", &self.total_backpressure)
            .finish()
    }
}

impl fmt::Display for StringWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StringWriter: {}/{} buffered ({:.1} KB/{:.1} MB, {:.0}% pressure), \
             {} written (avg {:.0}), {} flushes, avg value {:.0}B, {} backpressure",
            self.len(),
            self.batch_size,
            self.current_bytes as f64 / 1024.0,
            self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
            self.memory_pressure() * 100.0,
            self.total_written,
            self.avg_batch_size(),
            self.total_flushes,
            self.avg_value_len(),
            self.total_backpressure,
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
    fn row(pv: i32, val: &str) -> StringRow {
        StringRow::new(now(), pv, val.to_string(), 0, 0)
    }

    // ── StringRow ────────────────────────────────────────────────

    #[test]
    fn row_new() {
        let r = row(42, "hello");
        assert_eq!(r.pv_id, 42);
        assert_eq!(r.value, "hello");
        assert_eq!(r.value_len(), 5);
        assert!(!r.is_value_empty());
    }

    #[test]
    fn row_empty_value() {
        let r = row(1, "");
        assert!(r.is_value_empty());
        assert_eq!(r.value_len(), 0);
    }

    #[test]
    fn row_mem_size() {
        assert_eq!(row(1, "hello").mem_size(), 45);
    }
    #[test]
    fn row_wire_size() {
        assert_eq!(row(1, "hello").wire_size(), 43);
    } // 38 + 5

    #[test]
    fn row_wire_size_empty() {
        assert_eq!(row(1, "").wire_size(), 38);
    }

    #[test]
    fn row_display_short() {
        let s = row(42, "hello").to_string();
        assert!(s.contains("pv_id=42") && s.contains("hello"));
    }

    #[test]
    fn row_display_long() {
        let s = row(1, &"x".repeat(100)).to_string();
        assert!(s.contains("...") && s.contains("100 bytes"));
    }

    // ── PushResult (from copy_pool) ──────────────────────────────

    #[test]
    fn push_result() {
        assert!(!PushResult::Accepted.needs_flush());
        assert!(PushResult::Full.needs_flush());
        assert!(!PushResult::BackpressureExceeded.is_accepted());
    }

    // ── StringWriter construction ────────────────────────────────

    #[test]
    fn new_defaults() {
        let w = StringWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.batch_size(), 500);
        assert_eq!(w.total_written(), 0);
        assert_eq!(w.total_build_us(), 0);
        assert_eq!(w.total_send_us(), 0);
    }

    #[test]
    fn custom_limits() {
        let w = StringWriter::with_limits(100, 1 << 20);
        assert_eq!(w.batch_size(), 100);
        assert_eq!(w.max_buffer_bytes(), 1 << 20);
    }

    #[test]
    fn min_batch_clamped() {
        assert_eq!(StringWriter::new(0).batch_size(), 1);
    }
    #[test]
    fn min_buffer_clamped() {
        assert_eq!(StringWriter::with_limits(10, 0).max_buffer_bytes(), 1024);
    }

    // ── Push ─────────────────────────────────────────────────────

    #[test]
    fn push_accepted() {
        let mut w = StringWriter::with_defaults();
        assert_eq!(w.push(row(1, "hello")), PushResult::Accepted);
        assert_eq!(w.len(), 1);
        assert_eq!(w.buffered_bytes(), 45);
    }

    #[test]
    fn push_until_full() {
        let mut w = StringWriter::new(3);
        assert_eq!(w.push(row(1, "a")), PushResult::Accepted);
        assert_eq!(w.push(row(2, "b")), PushResult::Accepted);
        assert_eq!(w.push(row(3, "c")), PushResult::Full);
        assert!(w.is_full());
    }

    // ── Backpressure ─────────────────────────────────────────────

    #[test]
    fn backpressure_triggered() {
        let mut w = StringWriter::with_limits(100, 100);
        assert_eq!(w.push(row(1, "hello")), PushResult::Accepted);
        assert_eq!(w.push(row(2, "world")), PushResult::Accepted);
        assert_eq!(w.push(row(3, "!!!!!")), PushResult::BackpressureExceeded);
        assert_eq!(w.len(), 2);
        assert_eq!(w.total_backpressure(), 1);
    }

    #[test]
    fn backpressure_first_always_accepted() {
        let mut w = StringWriter::with_limits(100, 10);
        assert!(w.push(row(1, &"x".repeat(1000))).is_accepted());
    }

    #[test]
    fn backpressure_clears_after_discard() {
        let mut w = StringWriter::with_limits(100, 100);
        w.push(row(1, &"x".repeat(80)));
        w.discard();
        assert_eq!(w.push(row(2, "fresh")), PushResult::Accepted);
    }

    #[test]
    fn memory_pressure() {
        let mut w = StringWriter::with_limits(100, 1000);
        w.push(row(1, &"x".repeat(60)));
        let p = w.memory_pressure();
        assert!(p > 0.09 && p < 0.11, "got {p:.2}");
    }

    // ── Byte tracking ────────────────────────────────────────────

    #[test]
    fn bytes_incremental() {
        let mut w = StringWriter::with_defaults();
        w.push(row(1, "abc")); // 43
        w.push(row(2, "defgh")); // 45
        assert_eq!(w.buffered_bytes(), 43 + 45);
    }

    #[test]
    fn bytes_reset_on_discard() {
        let mut w = StringWriter::with_defaults();
        w.push(row(1, "test"));
        w.discard();
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── Discard ──────────────────────────────────────────────────

    #[test]
    fn discard_clears() {
        let mut w = StringWriter::with_defaults();
        w.push(row(1, "a"));
        w.push(row(2, "b"));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── Statistics ───────────────────────────────────────────────

    #[test]
    fn avg_value_len_computed() {
        let mut w = StringWriter::with_defaults();
        w.total_written = 100;
        w.total_value_bytes = 5_000;
        assert_eq!(w.avg_value_len(), 50.0);
    }

    #[test]
    fn avg_batch_size_computed() {
        let mut w = StringWriter::with_defaults();
        w.total_written = 1000;
        w.total_flushes = 4;
        assert_eq!(w.avg_batch_size(), 250.0);
    }

    // ── Compatibility ────────────────────────────────────────────

    #[test]
    fn aliases() {
        let mut w = StringWriter::with_defaults();
        w.push(row(1, "x"));
        assert_eq!(w.buffered(), w.len());
        assert_eq!(w.insert_flushes(), 0);
    }

    // ── Binary COPY payload ──────────────────────────────────────

    #[test]
    fn payload_header_and_trailer() {
        let rows = vec![row(1, "hello")];
        let buf = StringWriter::build_copy_payload(&rows);
        assert_eq!(&buf[..11], b"PGCOPY\n\xff\r\n\x00");
        assert_eq!(&buf[buf.len() - 2..], &PGCOPY_TRAILER);
    }

    #[test]
    fn payload_empty() {
        let buf = StringWriter::build_copy_payload(&[]);
        assert_eq!(buf.len(), PGCOPY_HEADER.len() + PGCOPY_TRAILER.len());
    }

    #[test]
    fn payload_exact_size() {
        let rows = vec![row(1, "hello"), row(2, "world!")];
        let expected = PGCOPY_HEADER.len()
            + (38 + 5)   // "hello"
            + (38 + 6)   // "world!"
            + PGCOPY_TRAILER.len();
        let buf = StringWriter::build_copy_payload(&rows);
        assert_eq!(buf.len(), expected);
    }

    #[test]
    fn payload_encodes_timestamp() {
        let time = ts(1781617845, 123456);
        let r = StringRow::new(time, 1, "x".into(), 0, 0);
        let buf = StringWriter::build_copy_payload(&[r]);
        // offset: 19 (header) + 2 (ncols) + 4 (time_len) = 25
        let pg_us = i64::from_be_bytes(buf[25..33].try_into().unwrap());
        assert_eq!(pg_us, time.timestamp_micros() - PG_EPOCH_OFFSET_US);
    }

    #[test]
    fn payload_encodes_pv_id() {
        let r = StringRow::new(now(), 42, "x".into(), 0, 0);
        let buf = StringWriter::build_copy_payload(&[r]);
        // offset: 19+2+12+4 = 37
        let pv_id = i32::from_be_bytes(buf[37..41].try_into().unwrap());
        assert_eq!(pv_id, 42);
    }

    #[test]
    fn payload_encodes_string_value() {
        let r = StringRow::new(now(), 1, "hello".into(), 0, 0);
        let buf = StringWriter::build_copy_payload(&[r]);
        // value length at offset 19+2+12+8+4 = 45..49, then bytes 49..54
        let val_len = i32::from_be_bytes(buf[41..45].try_into().unwrap());
        assert_eq!(val_len, 5);
        assert_eq!(&buf[45..50], b"hello");
    }

    #[test]
    fn payload_encodes_empty_string() {
        let r = StringRow::new(now(), 1, "".into(), 0, 0);
        let buf = StringWriter::build_copy_payload(&[r]);
        // value length should be 0
        let val_len = i32::from_be_bytes(buf[41..45].try_into().unwrap());
        assert_eq!(val_len, 0);
    }

    #[test]
    fn payload_encodes_unicode() {
        let s = "température: 4.2°K"; // 20 bytes UTF-8
        let r = StringRow::new(now(), 1, s.into(), 0, 0);
        let buf = StringWriter::build_copy_payload(&[r]);
        let val_len = i32::from_be_bytes(buf[41..45].try_into().unwrap());
        assert_eq!(val_len, s.len() as i32);
    }

    #[test]
    fn payload_encodes_severity_status() {
        let r = StringRow::new(now(), 1, "ab".into(), 3, 7);
        let buf = StringWriter::build_copy_payload(&[r]);
        // value is 2 bytes, so sev at offset 19+2+12+8+(4+2) = 47, +4 = 51
        let base = PGCOPY_HEADER.len() + 2 + 12 + 8 + 4 + 2; // after value bytes
        let sev = i16::from_be_bytes(buf[base + 4..base + 6].try_into().unwrap());
        let status = i16::from_be_bytes(buf[base + 10..base + 12].try_into().unwrap());
        assert_eq!(sev, 3);
        assert_eq!(status, 7);
    }

    #[test]
    fn payload_multi_row() {
        let rows = vec![
            StringRow::new(now(), 1, "a".into(), 0, 0),
            StringRow::new(now(), 2, "bb".into(), 0, 0),
            StringRow::new(now(), 3, "ccc".into(), 0, 0),
        ];
        let buf = StringWriter::build_copy_payload(&rows);
        let expected = PGCOPY_HEADER.len() + (38 + 1) + (38 + 2) + (38 + 3) + PGCOPY_TRAILER.len();
        assert_eq!(buf.len(), expected);
    }

    #[test]
    fn payload_special_chars_no_escape_needed() {
        // Binary COPY doesn't need escaping — tabs, newlines are sent as raw bytes.
        let r = StringRow::new(now(), 1, "a\tb\nc\\d".into(), 0, 0);
        let buf = StringWriter::build_copy_payload(&[r]);
        let val_len = i32::from_be_bytes(buf[41..45].try_into().unwrap());
        assert_eq!(val_len, 7); // raw bytes, no escaping
        assert_eq!(&buf[45..52], b"a\tb\nc\\d");
    }

    // ── Display / Debug ──────────────────────────────────────────

    #[test]
    fn display_empty() {
        let s = StringWriter::with_defaults().to_string();
        assert!(s.contains("0/500") && s.contains("0 backpressure"));
    }

    #[test]
    fn display_with_stats() {
        let mut w = StringWriter::with_defaults();
        w.total_written = 200;
        w.total_value_bytes = 10_000;
        w.total_backpressure = 1;
        let s = w.to_string();
        assert!(s.contains("200 written") && s.contains("1 backpressure"));
    }

    #[test]
    fn debug_output() {
        let d = format!("{:?}", StringWriter::with_defaults());
        assert!(d.contains("StringWriter") && d.contains("pressure"));
    }
}