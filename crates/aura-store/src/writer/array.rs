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

const DEFAULT_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const NUM_WIRE_SIZE: usize = 66;
const STR_WIRE_BASE: usize = 58;
const NUM_COLUMNS_STR: i16 = 7;
const PARALLEL_THRESHOLD: usize = 10_000;

pub(crate) const COPY_SQL_NUM: &str = "COPY samples_array_num (time, pv_id, array_id, idx, value, severity, status) FROM STDIN WITH (FORMAT binary)";
pub(crate) const COPY_SQL_STR: &str = "COPY samples_array_str (time, pv_id, array_id, idx, value, severity, status) FROM STDIN WITH (FORMAT binary)";

/// Global array_id counter — monotone across all PVs.
static NEXT_ARRAY_ID: AtomicI64 = AtomicI64::new(1);

fn next_array_id() -> i64 {
    NEXT_ARRAY_ID.fetch_add(1, Ordering::Relaxed)
}

/// A single element from a numeric waveform/matrix.
#[derive(Debug, Clone, Copy)]
pub struct ArrayNumRow {
    pub pg_us: i64,
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
        const TEMPLATE: [u8; NUM_WIRE_SIZE] = {
            let mut t = [0u8; NUM_WIRE_SIZE];
            t[0] = 0;
            t[1] = 7;
            t[2] = 0;
            t[3] = 0;
            t[4] = 0;
            t[5] = 8;
            t[14] = 0;
            t[15] = 0;
            t[16] = 0;
            t[17] = 4;
            t[22] = 0;
            t[23] = 0;
            t[24] = 0;
            t[25] = 8;
            t[34] = 0;
            t[35] = 0;
            t[36] = 0;
            t[37] = 4;
            t[42] = 0;
            t[43] = 0;
            t[44] = 0;
            t[45] = 8;
            t[54] = 0;
            t[55] = 0;
            t[56] = 0;
            t[57] = 2;
            t[60] = 0;
            t[61] = 0;
            t[62] = 0;
            t[63] = 2;
            t
        };
        *out = TEMPLATE;
        out[6..14].copy_from_slice(&self.pg_us.to_be_bytes());
        out[18..22].copy_from_slice(&self.pv_id.to_be_bytes());
        out[26..34].copy_from_slice(&self.array_id.to_be_bytes());
        out[38..42].copy_from_slice(&self.idx.to_be_bytes());
        out[46..54].copy_from_slice(&self.value.to_be_bytes());
        out[58..60].copy_from_slice(&self.severity.to_be_bytes());
        out[64..66].copy_from_slice(&self.status.to_be_bytes());
    }
}

/// A single element from a string array.
#[derive(Debug, Clone)]
pub struct ArrayStrRow {
    pub pg_us: i64,
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
        let val_bytes = self.value.as_bytes();
        buf.extend_from_slice(&NUM_COLUMNS_STR.to_be_bytes());
        buf.extend_from_slice(&8i32.to_be_bytes());
        buf.extend_from_slice(&self.pg_us.to_be_bytes());
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

    pub(crate) num_buffer: Vec<ArrayNumRow>,
    pub(crate) str_buffer: Vec<ArrayStrRow>,
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
        let pg_us = capture.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
        self.current_bytes += est_bytes;
        self.total_captures += 1;
        self.total_elements += n as u64;

        match capture.data {
            ArrayData::Numeric(values) => {
                self.num_buffer.reserve(values.len());
                for (idx, value) in values.into_iter().enumerate() {
                    self.num_buffer.push(ArrayNumRow {
                        pg_us,
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
                        pg_us,
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

        if self.num_buffer.len() + self.str_buffer.len() >= self.batch_size {
            PushResult::Full
        } else {
            PushResult::Accepted
        }
    }

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

    pub(crate) fn build_num_payload(rows: &[ArrayNumRow]) -> Vec<u8> {
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

    pub(crate) fn build_str_payload(rows: &[ArrayStrRow]) -> Vec<u8> {
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
        let avg = if self.total_captures > 0 {
            self.total_elements as f64 / self.total_captures as f64
        } else {
            0.0
        };
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
            avg,
            self.total_flushes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }
    fn now_pg() -> i64 {
        now().timestamp_micros() - PG_EPOCH_OFFSET_US
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

    #[test]
    fn capture_len() {
        assert_eq!(num_capture(1, vec![1.0, 2.0, 3.0]).len(), 3);
        assert_eq!(str_capture(1, vec!["a", "b"]).len(), 2);
        assert!(num_capture(1, vec![]).is_empty());
    }

    #[test]
    fn new_defaults() {
        let w = ArrayWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.total_written(), 0);
    }

    #[test]
    fn push_numeric() {
        let mut w = ArrayWriter::with_defaults();
        assert!(w.push(num_capture(1, vec![1.0, 2.0, 3.0])).is_accepted());
        assert_eq!(w.len(), 3);
    }

    #[test]
    fn push_numeric_full() {
        let mut w = ArrayWriter::new(5);
        w.push(num_capture(1, vec![1.0, 2.0, 3.0]));
        assert_eq!(w.push(num_capture(2, vec![4.0, 5.0])), PushResult::Full);
    }

    #[test]
    fn push_string() {
        let mut w = ArrayWriter::with_defaults();
        assert!(w.push(str_capture(1, vec!["hello", "world"])).is_accepted());
        assert_eq!(w.len(), 2);
    }

    #[test]
    fn push_mixed() {
        let mut w = ArrayWriter::with_defaults();
        w.push(num_capture(1, vec![1.0, 2.0]));
        w.push(str_capture(2, vec!["a", "b", "c"]));
        assert_eq!(w.len(), 5);
    }

    #[test]
    fn push_empty_capture() {
        let mut w = ArrayWriter::with_defaults();
        assert_eq!(w.push(num_capture(1, vec![])), PushResult::Accepted);
        assert_eq!(w.len(), 0);
    }

    #[test] fn backpressure() {
        let mut w = ArrayWriter::with_limits(100_000, 1024);
        w.push(num_capture(1, vec![1.0; 10])); // 10 * 66 = 660 bytes
        assert_eq!(w.push(num_capture(2, vec![3.0; 10])), PushResult::BackpressureExceeded); // 660+660 > 1024
    }

    #[test]
    fn backpressure_first_always_accepted() {
        let mut w = ArrayWriter::with_limits(100_000, 1);
        assert!(w.push(num_capture(1, vec![1.0; 100])).is_accepted());
    }

    #[test]
    fn discard() {
        let mut w = ArrayWriter::with_defaults();
        w.push(num_capture(1, vec![1.0, 2.0]));
        w.push(str_capture(2, vec!["a"]));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
    }

    #[test]
    fn array_ids_unique() {
        let mut w = ArrayWriter::with_defaults();
        w.push(num_capture(1, vec![1.0]));
        w.push(num_capture(1, vec![2.0]));
        assert_ne!(w.num_buffer[0].array_id, w.num_buffer[1].array_id);
    }

    #[test]
    fn num_payload_header_trailer() {
        let rows = vec![ArrayNumRow {
            pg_us: now_pg(),
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
                pg_us: now_pg(),
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
            pg_us: now_pg(),
            pv_id: 42,
            array_id: 7,
            idx: 3,
            value: std::f64::consts::PI,
            severity: 0,
            status: 0,
        }];
        let buf = ArrayWriter::build_num_payload(&rows);
        let val = f64::from_be_bytes(buf[65..73].try_into().unwrap());
        assert_eq!(val, std::f64::consts::PI);
    }

    #[test]
    fn num_payload_encodes_array_id() {
        let rows = vec![ArrayNumRow {
            pg_us: now_pg(),
            pv_id: 1,
            array_id: 999,
            idx: 0,
            value: 0.0,
            severity: 0,
            status: 0,
        }];
        let buf = ArrayWriter::build_num_payload(&rows);
        assert_eq!(i64::from_be_bytes(buf[45..53].try_into().unwrap()), 999);
    }

    #[test]
    fn num_payload_encodes_idx() {
        let rows = vec![ArrayNumRow {
            pg_us: now_pg(),
            pv_id: 1,
            array_id: 1,
            idx: 42,
            value: 0.0,
            severity: 0,
            status: 0,
        }];
        let buf = ArrayWriter::build_num_payload(&rows);
        assert_eq!(i32::from_be_bytes(buf[57..61].try_into().unwrap()), 42);
    }

    #[test]
    fn str_payload_size() {
        let rows = vec![ArrayStrRow {
            pg_us: now_pg(),
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

    #[test]
    fn display() {
        assert!(
            ArrayWriter::with_defaults()
                .to_string()
                .contains("ArrayWriter")
        );
    }

    #[test]
    fn debug() {
        assert!(format!("{:?}", ArrayWriter::with_defaults()).contains("ArrayWriter"));
    }
}