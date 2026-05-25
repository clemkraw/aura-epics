//! Batch writer for destructured array data (`samples_array_num` / `samples_array_str`).
//!
//! Waveforms and matrices are stored element-per-row for maximum TimescaleDB
//! compression. A waveform of 1024 floats becomes 1024 rows with sequential
//! `idx` values, grouped by `array_id`.
//!
//! # Compression
//!
//! The destructured layout enables gorilla encoding on the `value` column:
//! ```text
//! pv_id:    dictionary  → ~0 bits (same PV for all elements)
//! time:     delta       → ~0 bits (same timestamp for all elements)
//! array_id: delta       → ~0 bits (same capture for all elements)
//! idx:      delta       → ~1 bit  (0,1,2,3... → constant delta=1)
//! value:    gorilla     → ~4-8 bits (actual data)
//! severity: RLE         → ~0 bits
//! status:   RLE         → ~0 bits
//! Total: ~0.6-1.1 bytes/element vs 8 bytes raw
//! ```
//!
//! # Binary Row Layout (numeric, on the wire)
//!
//! ```text
//! num_cols(2) + [4+time(8)] + [4+pv_id(4)] + [4+array_id(8)]
//!             + [4+idx(4)] + [4+value(8)] + [4+sev(2)] + [4+status(2)]
//! = 2 + 12 + 8 + 12 + 8 + 12 + 6 + 6 = 66 bytes per element
//! ```

use chrono::{DateTime, Utc};
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

use super::copy_pool::{CopyPool, PG_EPOCH_OFFSET_US, PGCOPY_HEADER, PGCOPY_TRAILER, PushResult};
use aura_core::error::{AuraError, AuraResult};

/// Default max buffer (64 MB).
const DEFAULT_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Wire size per numeric element.
const NUM_WIRE_SIZE: usize = 66;

/// Wire size per string element (fixed part, + string length).
const STR_WIRE_BASE: usize = 58; // 66 - 12(value f64) + 4(text len prefix)

/// Number of columns for numeric array COPY.
const NUM_COLUMNS_NUM: i16 = 7;

/// Number of columns for string array COPY.
const NUM_COLUMNS_STR: i16 = 7;

/// Parallel threshold.
const PARALLEL_THRESHOLD: usize = 10_000;

const COPY_SQL_NUM: &str = "COPY samples_array_num (time, pv_id, array_id, idx, value, severity, status) FROM STDIN WITH (FORMAT binary)";
const COPY_SQL_STR: &str = "COPY samples_array_str (time, pv_id, array_id, idx, value, severity, status) FROM STDIN WITH (FORMAT binary)";

/// Global array_id counter — monotone across all PVs.
/// Each waveform capture gets a unique ID for reconstruction.
static NEXT_ARRAY_ID: AtomicI64 = AtomicI64::new(1);

fn next_array_id() -> i64 {
    NEXT_ARRAY_ID.fetch_add(1, Ordering::Relaxed)
}

/// A single element from a numeric waveform/matrix.
#[derive(Debug, Clone, Copy)]
pub struct ArrayNumRow {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub array_id: i64,
    pub idx: i32,
    pub value: f64,
    pub severity: i16,
    pub status: i16,
}

impl ArrayNumRow {
    #[inline]
    fn encode_copy(&self, out: &mut [u8; NUM_WIRE_SIZE]) {
        let pg_us = self.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
        let mut o = 0;
        out[o..o + 2].copy_from_slice(&NUM_COLUMNS_NUM.to_be_bytes());
        o += 2;
        out[o..o + 4].copy_from_slice(&8i32.to_be_bytes());
        o += 4;
        out[o..o + 8].copy_from_slice(&pg_us.to_be_bytes());
        o += 8;
        out[o..o + 4].copy_from_slice(&4i32.to_be_bytes());
        o += 4;
        out[o..o + 4].copy_from_slice(&self.pv_id.to_be_bytes());
        o += 4;
        out[o..o + 4].copy_from_slice(&8i32.to_be_bytes());
        o += 4;
        out[o..o + 8].copy_from_slice(&self.array_id.to_be_bytes());
        o += 8;
        out[o..o + 4].copy_from_slice(&4i32.to_be_bytes());
        o += 4;
        out[o..o + 4].copy_from_slice(&self.idx.to_be_bytes());
        o += 4;
        out[o..o + 4].copy_from_slice(&8i32.to_be_bytes());
        o += 4;
        out[o..o + 8].copy_from_slice(&self.value.to_be_bytes());
        o += 8;
        out[o..o + 4].copy_from_slice(&2i32.to_be_bytes());
        o += 4;
        out[o..o + 2].copy_from_slice(&self.severity.to_be_bytes());
        o += 2;
        out[o..o + 4].copy_from_slice(&2i32.to_be_bytes());
        o += 4;
        out[o..o + 2].copy_from_slice(&self.status.to_be_bytes());
    }
}

/// A single element from a string array.
#[derive(Debug, Clone)]
pub struct ArrayStrRow {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub array_id: i64,
    pub idx: i32,
    pub value: String,
    pub severity: i16,
    pub status: i16,
}

impl ArrayStrRow {
    #[inline]
    fn wire_size(&self) -> usize {
        STR_WIRE_BASE + self.value.len()
    }

    fn encode_copy(&self, buf: &mut Vec<u8>) {
        let pg_us = self.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
        let val_bytes = self.value.as_bytes();
        buf.extend_from_slice(&NUM_COLUMNS_STR.to_be_bytes());
        buf.extend_from_slice(&8i32.to_be_bytes());
        buf.extend_from_slice(&pg_us.to_be_bytes());
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf.extend_from_slice(&self.pv_id.to_be_bytes());
        buf.extend_from_slice(&8i32.to_be_bytes());
        buf.extend_from_slice(&self.array_id.to_be_bytes());
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf.extend_from_slice(&self.idx.to_be_bytes());
        buf.extend_from_slice(&(val_bytes.len() as i32).to_be_bytes());
        buf.extend_from_slice(val_bytes);
        buf.extend_from_slice(&2i32.to_be_bytes());
        buf.extend_from_slice(&self.severity.to_be_bytes());
        buf.extend_from_slice(&2i32.to_be_bytes());
        buf.extend_from_slice(&self.status.to_be_bytes());
    }
}

/// A complete waveform/matrix capture to be destructured.
pub struct ArrayCapture {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub severity: i16,
    pub status: i16,
    pub data: ArrayData,
}

/// The array payload — numeric or string.
pub enum ArrayData {
    Numeric(Vec<f64>),
    String(Vec<String>),
}

impl ArrayCapture {
    /// Number of elements.
    pub fn len(&self) -> usize {
        match &self.data {
            ArrayData::Numeric(v) => v.len(),
            ArrayData::String(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// High-throughput destructured array writer.
///
/// Receives complete waveform captures, assigns array_ids, explodes into
/// element-per-row, and flushes via binary COPY through the shared pool.
pub struct ArrayWriter {
    batch_size: usize,
    max_buffer_bytes: usize,

    num_buffer: Vec<ArrayNumRow>,
    str_buffer: Vec<ArrayStrRow>,
    current_bytes: usize,

    copy_pool: Option<CopyPool>,

    total_written: u64,
    total_captures: u64,
    total_elements: u64,
    total_flushes: u64,
    total_backpressure: u64,
    total_build_us: u64,
    total_send_us: u64,
}

impl ArrayWriter {
    pub fn new(batch_size: usize) -> Self {
        Self::with_limits(batch_size, DEFAULT_MAX_BUFFER_BYTES)
    }

    pub fn with_limits(batch_size: usize, max_buffer_bytes: usize) -> Self {
        let batch_size = batch_size.max(1);
        let max_buffer_bytes = max_buffer_bytes.max(1024);
        Self {
            batch_size,
            max_buffer_bytes,
            num_buffer: Vec::with_capacity(batch_size),
            str_buffer: Vec::new(),
            current_bytes: 0,
            copy_pool: None,
            total_written: 0,
            total_captures: 0,
            total_elements: 0,
            total_flushes: 0,
            total_backpressure: 0,
            total_build_us: 0,
            total_send_us: 0,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(10_000)
    }

    pub fn set_copy_pool(&mut self, pool: CopyPool) {
        self.copy_pool = Some(pool);
    }

    /// Push a complete waveform capture. Destructures into element rows.
    pub fn push(&mut self, capture: ArrayCapture) -> PushResult {
        let n = capture.len();
        if n == 0 {
            return PushResult::Accepted;
        }

        let est_bytes = match &capture.data {
            ArrayData::Numeric(_) => n * NUM_WIRE_SIZE,
            ArrayData::String(v) => v.iter().map(|s| STR_WIRE_BASE + s.len()).sum(),
        };

        if !self.num_buffer.is_empty() || !self.str_buffer.is_empty() {
            if self.current_bytes + est_bytes > self.max_buffer_bytes {
                self.total_backpressure += 1;
                return PushResult::BackpressureExceeded;
            }
        }

        let aid = next_array_id();
        self.current_bytes += est_bytes;
        self.total_captures += 1;
        self.total_elements += n as u64;

        match capture.data {
            ArrayData::Numeric(values) => {
                self.num_buffer.reserve(values.len());
                for (idx, value) in values.into_iter().enumerate() {
                    self.num_buffer.push(ArrayNumRow {
                        time: capture.time,
                        pv_id: capture.pv_id,
                        array_id: aid,
                        idx: idx as i32,
                        value,
                        severity: capture.severity,
                        status: capture.status,
                    });
                }
            }
            ArrayData::String(values) => {
                self.str_buffer.reserve(values.len());
                for (idx, value) in values.into_iter().enumerate() {
                    self.str_buffer.push(ArrayStrRow {
                        time: capture.time,
                        pv_id: capture.pv_id,
                        array_id: aid,
                        idx: idx as i32,
                        value,
                        severity: capture.severity,
                        status: capture.status,
                    });
                }
            }
        }

        let total_rows = self.num_buffer.len() + self.str_buffer.len();
        if total_rows >= self.batch_size {
            PushResult::Full
        } else {
            PushResult::Accepted
        }
    }

    /// Flush all buffered element rows via binary COPY.
    pub async fn flush(&mut self) -> AuraResult<usize> {
        let num_count = self.num_buffer.len();
        let str_count = self.str_buffer.len();
        if num_count == 0 && str_count == 0 {
            return Ok(0);
        }

        let pool = self
            .copy_pool
            .as_ref()
            .ok_or_else(|| AuraError::database("ArrayWriter: no copy pool configured"))?;

        let mut total = 0;

        // Flush numeric buffer.
        if num_count > 0 {
            let t0 = std::time::Instant::now();
            let n_conn = pool.len();

            if num_count >= PARALLEL_THRESHOLD && n_conn > 1 {
                let chunk_size = (num_count + n_conn - 1) / n_conn;
                let payloads: Vec<_> = self
                    .num_buffer
                    .chunks(chunk_size)
                    .map(|chunk| (Self::build_num_payload(chunk), chunk.len()))
                    .collect();
                self.total_build_us += t0.elapsed().as_micros() as u64;

                let t1 = std::time::Instant::now();
                pool.send_parallel(COPY_SQL_NUM, payloads).await?;
                self.total_send_us += t1.elapsed().as_micros() as u64;
            } else {
                let payload = Self::build_num_payload(&self.num_buffer);
                self.total_build_us += t0.elapsed().as_micros() as u64;

                let t1 = std::time::Instant::now();
                pool.send_copy(0, COPY_SQL_NUM, payload, num_count).await?;
                self.total_send_us += t1.elapsed().as_micros() as u64;
            }

            total += num_count;
            self.num_buffer.clear();
        }

        // Flush string buffer (rare, single connection).
        if str_count > 0 {
            let t0 = std::time::Instant::now();
            let payload = Self::build_str_payload(&self.str_buffer);
            self.total_build_us += t0.elapsed().as_micros() as u64;

            let t1 = std::time::Instant::now();
            pool.send_copy(0, COPY_SQL_STR, payload, str_count).await?;
            self.total_send_us += t1.elapsed().as_micros() as u64;

            total += str_count;
            self.str_buffer.clear();
        }

        self.total_written += total as u64;
        self.total_flushes += 1;
        self.current_bytes = 0;
        Ok(total)
    }

    /// Build binary COPY payload for numeric elements.
    fn build_num_payload(rows: &[ArrayNumRow]) -> Vec<u8> {
        let capacity = PGCOPY_HEADER.len() + rows.len() * NUM_WIRE_SIZE + PGCOPY_TRAILER.len();
        let mut buf = Vec::with_capacity(capacity);
        buf.extend_from_slice(&PGCOPY_HEADER);
        let mut row_buf = [0u8; NUM_WIRE_SIZE];
        for row in rows {
            row.encode_copy(&mut row_buf);
            buf.extend_from_slice(&row_buf);
        }
        buf.extend_from_slice(&PGCOPY_TRAILER);
        buf
    }

    /// Build binary COPY payload for string elements.
    fn build_str_payload(rows: &[ArrayStrRow]) -> Vec<u8> {
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
        self.num_buffer.len() + self.str_buffer.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_buffer.is_empty() && self.str_buffer.is_empty()
    }
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= self.batch_size
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
    pub fn total_captures(&self) -> u64 {
        self.total_captures
    }
    #[inline]
    pub fn total_elements(&self) -> u64 {
        self.total_elements
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
    pub fn total_build_us(&self) -> u64 {
        self.total_build_us
    }
    #[inline]
    pub fn total_send_us(&self) -> u64 {
        self.total_send_us
    }

    // Compatibility
    #[inline]
    pub fn buffered(&self) -> usize {
        self.len()
    }

    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 {
            return 0.0;
        }
        self.current_bytes as f64 / self.max_buffer_bytes as f64
    }

    pub fn avg_elements_per_capture(&self) -> f64 {
        if self.total_captures == 0 {
            return 0.0;
        }
        self.total_elements as f64 / self.total_captures as f64
    }

    pub fn discard(&mut self) {
        self.num_buffer.clear();
        self.str_buffer.clear();
        self.current_bytes = 0;
    }
}

impl fmt::Debug for ArrayWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayWriter")
            .field("num_buffered", &self.num_buffer.len())
            .field("str_buffered", &self.str_buffer.len())
            .field("captures", &self.total_captures)
            .field("elements", &self.total_elements)
            .field(
                "pressure",
                &format_args!("{:.1}%", self.memory_pressure() * 100.0),
            )
            .finish()
    }
}

impl fmt::Display for ArrayWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArrayWriter: {} num + {} str buffered ({:.1} KB, {:.0}% pressure), \
             {} written, {} captures (avg {:.0} elem), {} flushes",
            self.num_buffer.len(),
            self.str_buffer.len(),
            self.current_bytes as f64 / 1024.0,
            self.memory_pressure() * 100.0,
            self.total_written,
            self.total_captures,
            self.avg_elements_per_capture(),
            self.total_flushes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }
    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn num_capture(pv: i32, values: Vec<f64>) -> ArrayCapture {
        ArrayCapture {
            time: now(),
            pv_id: pv,
            severity: 0,
            status: 0,
            data: ArrayData::Numeric(values),
        }
    }

    fn str_capture(pv: i32, values: Vec<&str>) -> ArrayCapture {
        ArrayCapture {
            time: now(),
            pv_id: pv,
            severity: 0,
            status: 0,
            data: ArrayData::String(values.into_iter().map(String::from).collect()),
        }
    }

    // ── ArrayCapture ─────────────────────────────────────────────

    #[test]
    fn capture_len() {
        assert_eq!(num_capture(1, vec![1.0, 2.0, 3.0]).len(), 3);
        assert_eq!(str_capture(1, vec!["a", "b"]).len(), 2);
    }

    #[test]
    fn capture_empty() {
        assert!(num_capture(1, vec![]).is_empty());
    }

    // ── ArrayWriter construction ─────────────────────────────────

    #[test]
    fn new_defaults() {
        let w = ArrayWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.total_captures(), 0);
        assert_eq!(w.total_elements(), 0);
    }

    // ── Push numeric ─────────────────────────────────────────────

    #[test]
    fn push_numeric() {
        let mut w = ArrayWriter::with_defaults();
        let r = w.push(num_capture(1, vec![1.0, 2.0, 3.0]));
        assert!(r.is_accepted());
        assert_eq!(w.len(), 3);
        assert_eq!(w.total_captures(), 1);
        assert_eq!(w.total_elements(), 3);
    }

    #[test]
    fn push_numeric_full() {
        let mut w = ArrayWriter::new(5);
        w.push(num_capture(1, vec![1.0, 2.0, 3.0]));
        let r = w.push(num_capture(2, vec![4.0, 5.0]));
        assert_eq!(r, PushResult::Full);
        assert_eq!(w.len(), 5);
    }

    // ── Push string ──────────────────────────────────────────────

    #[test]
    fn push_string() {
        let mut w = ArrayWriter::with_defaults();
        let r = w.push(str_capture(1, vec!["hello", "world"]));
        assert!(r.is_accepted());
        assert_eq!(w.len(), 2);
    }

    // ── Push mixed ───────────────────────────────────────────────

    #[test]
    fn push_mixed() {
        let mut w = ArrayWriter::with_defaults();
        w.push(num_capture(1, vec![1.0, 2.0]));
        w.push(str_capture(2, vec!["a", "b", "c"]));
        assert_eq!(w.len(), 5);
        assert_eq!(w.total_captures(), 2);
    }

    // ── Push empty ───────────────────────────────────────────────

    #[test]
    fn push_empty_capture() {
        let mut w = ArrayWriter::with_defaults();
        assert_eq!(w.push(num_capture(1, vec![])), PushResult::Accepted);
        assert_eq!(w.len(), 0);
    }

    // ── Backpressure ─────────────────────────────────────────────

    #[test]
    fn backpressure() {
        let mut w = ArrayWriter::with_limits(100_000, 200);
        w.push(num_capture(1, vec![1.0, 2.0])); // 132 bytes
        let r = w.push(num_capture(2, vec![3.0, 4.0, 5.0])); // 198 more → 330 > 200
        assert_eq!(r, PushResult::BackpressureExceeded);
        assert_eq!(w.total_backpressure(), 1);
    }

    #[test]
    fn backpressure_first_always_accepted() {
        let mut w = ArrayWriter::with_limits(100_000, 1);
        assert!(w.push(num_capture(1, vec![1.0; 100])).is_accepted());
    }

    // ── Discard ──────────────────────────────────────────────────

    #[test]
    fn discard() {
        let mut w = ArrayWriter::with_defaults();
        w.push(num_capture(1, vec![1.0, 2.0]));
        w.push(str_capture(2, vec!["a"]));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── array_id uniqueness ──────────────────────────────────────

    #[test]
    fn array_ids_are_unique() {
        let mut w = ArrayWriter::with_defaults();
        w.push(num_capture(1, vec![1.0]));
        w.push(num_capture(1, vec![2.0]));
        let id1 = w.num_buffer[0].array_id;
        let id2 = w.num_buffer[1].array_id;
        assert_ne!(id1, id2);
    }

    // ── Binary payload — numeric ─────────────────────────────────

    #[test]
    fn num_payload_header_trailer() {
        let rows = vec![ArrayNumRow {
            time: now(),
            pv_id: 1,
            array_id: 1,
            idx: 0,
            value: 42.0,
            severity: 0,
            status: 0,
        }];
        let buf = ArrayWriter::build_num_payload(&rows);
        assert_eq!(&buf[..11], b"PGCOPY\n\xff\r\n\x00");
        assert_eq!(&buf[buf.len() - 2..], &PGCOPY_TRAILER);
    }

    #[test]
    fn num_payload_exact_size() {
        let rows: Vec<_> = (0..100)
            .map(|i| ArrayNumRow {
                time: now(),
                pv_id: 1,
                array_id: 1,
                idx: i,
                value: i as f64,
                severity: 0,
                status: 0,
            })
            .collect();
        let buf = ArrayWriter::build_num_payload(&rows);
        assert_eq!(
            buf.len(),
            PGCOPY_HEADER.len() + 100 * NUM_WIRE_SIZE + PGCOPY_TRAILER.len()
        );
    }

    #[test]
    fn num_payload_encodes_value() {
        let rows = vec![ArrayNumRow {
            time: now(),
            pv_id: 42,
            array_id: 7,
            idx: 3,
            value: std::f64::consts::PI,
            severity: 0,
            status: 0,
        }];
        let buf = ArrayWriter::build_num_payload(&rows);
        // value at offset: 19(hdr) + 2(ncols) + 12(time) + 8(pv_id) + 12(array_id) + 8(idx) + 4(val_len) = 65
        let val = f64::from_be_bytes(buf[65..73].try_into().unwrap());
        assert_eq!(val, std::f64::consts::PI);
    }

    #[test]
    fn num_payload_encodes_array_id() {
        let rows = vec![ArrayNumRow {
            time: now(),
            pv_id: 1,
            array_id: 999,
            idx: 0,
            value: 0.0,
            severity: 0,
            status: 0,
        }];
        let buf = ArrayWriter::build_num_payload(&rows);
        // array_id at offset: 19+2+12+8+4 = 45
        let aid = i64::from_be_bytes(buf[45..53].try_into().unwrap());
        assert_eq!(aid, 999);
    }

    #[test]
    fn num_payload_encodes_idx() {
        let rows = vec![ArrayNumRow {
            time: now(),
            pv_id: 1,
            array_id: 1,
            idx: 42,
            value: 0.0,
            severity: 0,
            status: 0,
        }];
        let buf = ArrayWriter::build_num_payload(&rows);
        // idx at offset: 19+2+12+8+12+4 = 57
        let idx = i32::from_be_bytes(buf[57..61].try_into().unwrap());
        assert_eq!(idx, 42);
    }

    // ── Binary payload — string ──────────────────────────────────

    #[test]
    fn str_payload_size() {
        let rows = vec![ArrayStrRow {
            time: now(),
            pv_id: 1,
            array_id: 1,
            idx: 0,
            value: "hello".into(),
            severity: 0,
            status: 0,
        }];
        let buf = ArrayWriter::build_str_payload(&rows);
        assert_eq!(
            buf.len(),
            PGCOPY_HEADER.len() + STR_WIRE_BASE + 5 + PGCOPY_TRAILER.len()
        );
    }

    // ── Statistics ───────────────────────────────────────────────

    #[test]
    fn avg_elements() {
        let mut w = ArrayWriter::with_defaults();
        w.total_captures = 10;
        w.total_elements = 10240;
        assert_eq!(w.avg_elements_per_capture(), 1024.0);
    }

    // ── Display / Debug ──────────────────────────────────────────

    #[test]
    fn display() {
        let s = ArrayWriter::with_defaults().to_string();
        assert!(s.contains("ArrayWriter") && s.contains("0 num"));
    }

    #[test]
    fn debug() {
        let d = format!("{:?}", ArrayWriter::with_defaults());
        assert!(d.contains("ArrayWriter"));
    }
}