//! Writer for the `samples_image` table (NTNDArray camera/detector frames).
//!
//! Images are the largest per-row data. Variable-length
//! columns (BYTEA, TEXT, INT4[], JSONB) use length-prefixed encoding.
//!
//! Every frame is stored (no deadband on images), so write rate is determined by the camera frame rate.

use chrono::{DateTime, Utc};
use std::fmt;

use super::copy_pool::{CopyPool, PG_EPOCH_OFFSET_US, PGCOPY_HEADER, PGCOPY_TRAILER, PushResult};
use aura_core::error::{AuraError, AuraResult};

pub(crate) const COPY_SQL: &str = "COPY samples_image (time, pv_id, data, codec, compressed_size, uncompressed_size, \
     dimensions, unique_id, attributes, severity, status) FROM STDIN WITH (FORMAT binary)";

const NUM_COLUMNS: i16 = 11;
const INT4_OID: i32 = 23;
const DEFAULT_MAX_BUFFER_BYTES: usize = 128 * 1024 * 1024;

/// Fixed overhead per `ImageRow` beyond the data payload.
const ROW_OVERHEAD: usize = 200;

/// A single row for `samples_image`.
#[derive(Debug, Clone)]
pub struct ImageRow {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub data: Vec<u8>,
    pub codec: String,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
    pub dimensions: Vec<i32>,
    pub unique_id: i32,
    pub attributes: serde_json::Value,
    pub severity: i16,
    pub status: i16,
}

impl ImageRow {
    pub fn new(
        time: DateTime<Utc>,
        pv_id: i32,
        data: Vec<u8>,
        codec: String,
        compressed_size: i64,
        uncompressed_size: i64,
        dimensions: Vec<i32>,
        unique_id: i32,
        severity: i16,
        status: i16,
    ) -> Self {
        Self {
            time,
            pv_id,
            data,
            codec,
            compressed_size,
            uncompressed_size,
            dimensions,
            unique_id,
            attributes: serde_json::Value::Object(Default::default()),
            severity,
            status,
        }
    }

    /// Extract from NTNDArray, moving pixel data out (O(1) via std::mem::take).
    pub fn from_update(
        time: DateTime<Utc>,
        pv_id: i32,
        nt: &mut aura_core::pva::normative::NormativeType,
        severity: i16,
        status: i16,
    ) -> Self {
        use aura_core::pva::normative::NormativeType;
        match nt {
            NormativeType::NTNDArray(img) => {
                let data = match &mut img.value {
                    aura_core::pva::ArrayValue::UByteArray(v) => std::mem::take(v),
                    _ => Vec::new(),
                };
                let dims: Vec<i32> = img.dimension.iter().map(|d| d.size).collect();
                let codec = std::mem::take(&mut img.codec.name);
                Self::new(
                    time,
                    pv_id,
                    data,
                    codec,
                    img.compressed_size,
                    img.uncompressed_size,
                    dims,
                    img.unique_id,
                    severity,
                    status,
                )
            }
            _ => Self::new(
                time,
                pv_id,
                Vec::new(),
                String::new(),
                0,
                0,
                Vec::new(),
                0,
                severity,
                status,
            ),
        }
    }

    /// Total estimated heap memory for this row.
    #[inline]
    pub fn mem_size(&self) -> usize {
        ROW_OVERHEAD + self.data.len() + self.codec.len() + self.dimensions.len() * 4
    }
}

impl fmt::Display for ImageRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pv_id={} uid={} {}×{}px {:.1} KB ({})",
            self.pv_id,
            self.unique_id,
            self.dimensions.first().unwrap_or(&0),
            self.dimensions.get(1).unwrap_or(&0),
            self.data.len() as f64 / 1024.0,
            self.codec
        )
    }
}

/// Batch writer for image samples (NTNDArray).
pub struct ImageWriter {
    batch_size: usize,
    max_buffer_bytes: usize,
    pub(crate) buffer: Vec<ImageRow>,
    current_bytes: usize,
    copy_pool: Option<CopyPool>,

    total_written: u64,
    total_flushes: u64,
    total_bytes: u64,
    total_backpressure: u64,
}

impl ImageWriter {
    pub fn new(batch_size: usize) -> Self {
        Self::with_limits(batch_size, DEFAULT_MAX_BUFFER_BYTES)
    }

    pub fn with_limits(batch_size: usize, max_buffer_bytes: usize) -> Self {
        let batch_size = batch_size.max(1);
        Self {
            batch_size,
            max_buffer_bytes: max_buffer_bytes.max(1024),
            buffer: Vec::with_capacity(batch_size.min(64)),
            current_bytes: 0,
            copy_pool: None,
            total_written: 0,
            total_flushes: 0,
            total_bytes: 0,
            total_backpressure: 0,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(50)
    }

    pub fn set_copy_pool(&mut self, pool: CopyPool) {
        self.copy_pool = Some(pool);
    }

    #[inline]
    pub fn push(&mut self, row: ImageRow) -> PushResult {
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

    pub async fn flush(&mut self) -> AuraResult<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }
        let count = self.buffer.len();

        let pool = self
            .copy_pool
            .as_ref()
            .ok_or_else(|| AuraError::database("ImageWriter: no copy pool configured"))?;

        let payload = Self::build_copy_payload(&self.buffer);
        pool.send_copy(0, COPY_SQL, payload, count).await?;

        for row in &self.buffer {
            self.total_bytes += row.data.len() as u64;
        }
        self.total_written += count as u64;
        self.total_flushes += 1;
        self.buffer.clear();
        self.current_bytes = 0;
        Ok(count)
    }

    pub(crate) fn build_copy_payload(rows: &[ImageRow]) -> Vec<u8> {
        let est_size: usize = rows
            .iter()
            .map(|r| 100 + r.data.len() + r.codec.len() + r.dimensions.len() * 8 + 256)
            .sum();
        let mut buf = Vec::with_capacity(PGCOPY_HEADER.len() + est_size + PGCOPY_TRAILER.len());
        buf.extend_from_slice(&PGCOPY_HEADER);

        for row in rows {
            let pg_us = row.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
            let codec_bytes = row.codec.as_bytes();
            let attr_bytes = serde_json::to_vec(&row.attributes).unwrap_or_else(|_| b"{}".to_vec());

            buf.extend_from_slice(&NUM_COLUMNS.to_be_bytes());
            // time
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&pg_us.to_be_bytes());
            // pv_id
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&row.pv_id.to_be_bytes());
            // data (BYTEA)
            buf.extend_from_slice(&(row.data.len() as i32).to_be_bytes());
            buf.extend_from_slice(&row.data);
            // codec (TEXT)
            buf.extend_from_slice(&(codec_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(codec_bytes);
            // compressed_size
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&row.compressed_size.to_be_bytes());
            // uncompressed_size
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&row.uncompressed_size.to_be_bytes());
            // dimensions (INT4[] — PostgreSQL binary array format)
            let ndims = if row.dimensions.is_empty() {
                0i32
            } else {
                1i32
            };
            let array_len = 4
                + 4
                + 4
                + if ndims > 0 {
                    4 + 4 + row.dimensions.len() * 8
                } else {
                    0
                };
            buf.extend_from_slice(&(array_len as i32).to_be_bytes());
            buf.extend_from_slice(&ndims.to_be_bytes());
            buf.extend_from_slice(&0i32.to_be_bytes()); // has_null
            buf.extend_from_slice(&INT4_OID.to_be_bytes());
            if ndims > 0 {
                buf.extend_from_slice(&(row.dimensions.len() as i32).to_be_bytes());
                buf.extend_from_slice(&1i32.to_be_bytes()); // lower bound
                for &dim in &row.dimensions {
                    buf.extend_from_slice(&4i32.to_be_bytes());
                    buf.extend_from_slice(&dim.to_be_bytes());
                }
            }
            // unique_id
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&row.unique_id.to_be_bytes());
            // attributes (JSONB)
            let jsonb_len = 1 + attr_bytes.len();
            buf.extend_from_slice(&(jsonb_len as i32).to_be_bytes());
            buf.push(0x01);
            buf.extend_from_slice(&attr_bytes);
            // severity
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&row.severity.to_be_bytes());
            // status
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&row.status.to_be_bytes());
        }

        buf.extend_from_slice(&PGCOPY_TRAILER);
        buf
    }

    #[inline]
    pub fn buffered(&self) -> usize {
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
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
    #[inline]
    pub fn total_backpressure(&self) -> u64 {
        self.total_backpressure
    }

    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 {
            return 0.0;
        }
        self.current_bytes as f64 / self.max_buffer_bytes as f64
    }

    pub fn discard(&mut self) {
        self.buffer.clear();
        self.current_bytes = 0;
    }
}

impl fmt::Debug for ImageWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageWriter")
            .field("buffered", &self.buffer.len())
            .field("batch_size", &self.batch_size)
            .field(
                "pressure",
                &format!("{:.1}%", self.memory_pressure() * 100.0),
            )
            .field("written", &self.total_written)
            .field("total_bytes", &self.total_bytes)
            .field("backpressure", &self.total_backpressure)
            .finish()
    }
}

impl fmt::Display for ImageWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ImageWriter: {}/{} ({:.1} MB/{:.0} MB, {:.0}%), \
                {} written ({:.1} MB), {} flushes, {} backpressure",
            self.buffer.len(),
            self.batch_size,
            self.current_bytes as f64 / (1024.0 * 1024.0),
            self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
            self.memory_pressure() * 100.0,
            self.total_written,
            self.total_bytes as f64 / (1024.0 * 1024.0),
            self.total_flushes,
            self.total_backpressure
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_image(pv_id: i32, size: usize, uid: i32) -> ImageRow {
        ImageRow::new(
            Utc::now(),
            pv_id,
            vec![0u8; size],
            "raw".to_string(),
            size as i64,
            size as i64,
            vec![100, 100],
            uid,
            0,
            0,
        )
    }

    #[test]
    fn row_new() {
        let r = make_image(1, 1024, 42);
        assert_eq!(r.pv_id, 1);
        assert_eq!(r.data.len(), 1024);
        assert_eq!(r.unique_id, 42);
        assert_eq!(r.codec, "raw");
        assert_eq!(r.dimensions, vec![100, 100]);
    }

    #[test]
    fn row_mem_size() {
        let small = make_image(1, 100, 1);
        let large = make_image(1, 10_000, 2);
        assert!(large.mem_size() > small.mem_size());
        assert!(small.mem_size() >= ROW_OVERHEAD + 100);
    }

    #[test]
    fn row_display() {
        let s = make_image(42, 10240, 7).to_string();
        assert!(s.contains("pv_id=42") && s.contains("100×100px"));
    }

    #[test]
    fn writer_defaults() {
        let w = ImageWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.batch_size(), 50);
        assert_eq!(w.total_written(), 0);
    }

    #[test]
    fn min_batch_size() {
        assert_eq!(ImageWriter::new(0).batch_size(), 1);
    }
    #[test]
    fn min_buffer_bytes() {
        assert_eq!(ImageWriter::with_limits(10, 0).max_buffer_bytes(), 1024);
    }

    #[test]
    fn push_accepted() {
        let mut w = ImageWriter::with_defaults();
        assert_eq!(w.push(make_image(1, 1000, 1)), PushResult::Accepted);
        assert_eq!(w.buffered(), 1);
        assert!(w.buffered_bytes() >= 1000);
    }

    #[test]
    fn push_until_full() {
        let mut w = ImageWriter::new(3);
        w.push(make_image(1, 100, 1));
        w.push(make_image(2, 100, 2));
        assert_eq!(w.push(make_image(3, 100, 3)), PushResult::Full);
    }

    #[test]
    fn backpressure_triggered() {
        let mut w = ImageWriter::with_limits(100, 1500);
        assert!(w.push(make_image(1, 1000, 1)).is_accepted());
        assert_eq!(
            w.push(make_image(2, 1000, 2)),
            PushResult::BackpressureExceeded
        );
        assert_eq!(w.buffered(), 1);
    }

    #[test]
    fn backpressure_first_always_accepted() {
        let mut w = ImageWriter::with_limits(100, 1);
        assert!(w.push(make_image(1, 10_000, 1)).is_accepted());
    }

    #[test]
    fn backpressure_clears_after_discard() {
        let mut w = ImageWriter::with_limits(100, 1500);
        w.push(make_image(1, 1000, 1));
        assert_eq!(
            w.push(make_image(2, 1000, 2)),
            PushResult::BackpressureExceeded
        );
        w.discard();
        assert!(w.push(make_image(3, 1000, 3)).is_accepted());
    }

    #[test]
    fn discard() {
        let mut w = ImageWriter::with_defaults();
        w.push(make_image(1, 1000, 1));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
    }

    #[test]
    fn display() {
        let s = ImageWriter::with_defaults().to_string();
        assert!(s.contains("0/50") && s.contains("0 backpressure"));
    }

    #[test]
    fn debug() {
        assert!(format!("{:?}", ImageWriter::with_defaults()).contains("ImageWriter"));
    }
}
