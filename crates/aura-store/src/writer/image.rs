//! Writer for the `samples_image` table (NTNDArray camera/detector frames).
//!
//! Images are the largest per-row data. This writer
//! uses individual INSERTs inside a transaction rather than UNNEST —
//! BYTEA columns with variable-length blobs don't benefit from array
//! batching and would double memory (buffer + UNNEST copy).
//!
//! Images bypass the deadband filter (every frame is stored), so the
//! write rate is determined by the camera frame rate, not the filter.

use chrono::{DateTime, Utc};
use std::fmt;

use super::copy_pool::{CopyPool, PushResult};

use aura_core::error::{AuraError, AuraResult};

/// Default max buffer memory (128 MB — images are large).
const DEFAULT_MAX_BUFFER_BYTES: usize = 128 * 1024 * 1024;

/// Fixed overhead per `ImageRow` beyond the data payload:
/// time(12) + pv_id(4) + codec(~24+len) + sizes(16) + dims(~24+n*4)
/// + unique_id(4) + attributes(~64) + severity/status(4) + Vec overhead(24)
/// ≈ 200 bytes conservatively.
const ROW_OVERHEAD: usize = 200;

/// INSERT SQL — static for prepared statement caching.
const INSERT_SQL: &str = r#"
    INSERT INTO samples_image
    (time, pv_id, data, codec, compressed_size, uncompressed_size,
     dimensions, unique_id, attributes, severity, status)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
"#;

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
    /// Create an image row from NTNDArray components.
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

    /// Attach NdAttributes (builder pattern).
    pub fn with_attributes(mut self, attributes: serde_json::Value) -> Self {
        self.attributes = attributes;
        self
    }

    /// Size of the pixel data payload in bytes.
    #[inline]
    pub fn data_size(&self) -> usize {
        self.data.len()
    }

    /// Total estimated heap memory for this row.
    #[inline]
    pub fn mem_size(&self) -> usize {
        ROW_OVERHEAD + self.data.len() + self.codec.len() + self.dimensions.len() * 4
    }

    /// Compression ratio (uncompressed / compressed). Returns 1.0 if raw.
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size <= 0 || self.uncompressed_size <= 0 {
            return 1.0;
        }
        self.uncompressed_size as f64 / self.compressed_size as f64
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
    buffer: Vec<ImageRow>,
    current_bytes: usize,
    copy_pool: Option<CopyPool>,

    total_written: u64,
    total_flushes: u64,
    total_bytes: u64,
    total_uncompressed_bytes: u64,
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
            total_uncompressed_bytes: 0,
            total_backpressure: 0,
        }
    }

    /// Default: batch 50, 128 MB limit.
    pub fn with_defaults() -> Self {
        Self::new(50)
    }

    /// Set the shared COPY pool.
    pub fn set_copy_pool(&mut self, pool: CopyPool) {
        self.copy_pool = Some(pool);
    }

    /// Push an image row with backpressure.
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

    /// Flush all buffered images in a single transaction.
    pub async fn flush(&mut self) -> AuraResult<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }
        let count = self.buffer.len();

        let pool = self
            .copy_pool
            .as_ref()
            .ok_or_else(|| AuraError::database("ImageWriter: no copy pool configured"))?;

        for row in &self.buffer {
            pool.execute(
                0,
                INSERT_SQL,
                &[
                    &row.time as &(dyn tokio_postgres::types::ToSql + Sync),
                    &row.pv_id,
                    &row.data,
                    &row.codec,
                    &row.compressed_size,
                    &row.uncompressed_size,
                    &row.dimensions,
                    &row.unique_id,
                    &row.attributes,
                    &row.severity,
                    &row.status,
                ],
            )
            .await
            .map_err(|e| {
                AuraError::database(format!(
                    "image insert failed (pv_id={}, uid={}): {e}",
                    row.pv_id, row.unique_id
                ))
            })?;
        }

        for row in &self.buffer {
            self.total_bytes += row.data.len() as u64;
            self.total_uncompressed_bytes += row.uncompressed_size.max(0) as u64;
        }
        self.total_written += count as u64;
        self.total_flushes += 1;
        self.buffer.clear();
        self.current_bytes = 0;

        Ok(count)
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
    /// O(1) — tracked incrementally on push/flush.
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
    pub fn total_uncompressed_bytes(&self) -> u64 {
        self.total_uncompressed_bytes
    }
    #[inline]
    pub fn total_backpressure(&self) -> u64 {
        self.total_backpressure
    }

    /// Buffer memory utilization (0.0 to 1.0).
    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 {
            return 0.0;
        }
        self.current_bytes as f64 / self.max_buffer_bytes as f64
    }

    /// Average compressed image size in bytes.
    pub fn avg_image_bytes(&self) -> f64 {
        if self.total_written == 0 {
            return 0.0;
        }
        self.total_bytes as f64 / self.total_written as f64
    }

    /// Average compression ratio across all written images.
    pub fn avg_compression_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            return 1.0;
        }
        self.total_uncompressed_bytes as f64 / self.total_bytes as f64
    }

    /// Clear buffer without flushing.
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
                "bytes",
                &format!("{}/{}", self.current_bytes, self.max_buffer_bytes),
            )
            .field(
                "pressure",
                &format!("{:.1}%", self.memory_pressure() * 100.0),
            )
            .field("total_written", &self.total_written)
            .field("total_bytes", &self.total_bytes)
            .field(
                "compression",
                &format!("{:.1}×", self.avg_compression_ratio()),
            )
            .field("backpressure", &self.total_backpressure)
            .finish()
    }
}

impl fmt::Display for ImageWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ImageWriter: {}/{} ({:.1} MB/{:.0} MB, {:.0}%), \
             {} written ({:.1} MB, avg {:.0} KB, {:.1}× compression), \
             {} flushes, {} backpressure",
            self.buffer.len(),
            self.batch_size,
            self.current_bytes as f64 / (1024.0 * 1024.0),
            self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
            self.memory_pressure() * 100.0,
            self.total_written,
            self.total_bytes as f64 / (1024.0 * 1024.0),
            self.avg_image_bytes() / 1024.0,
            self.avg_compression_ratio(),
            self.total_flushes,
            self.total_backpressure
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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

    fn make_compressed(pv_id: i32, compressed: usize, uncompressed: usize, uid: i32) -> ImageRow {
        ImageRow::new(
            Utc::now(),
            pv_id,
            vec![0u8; compressed],
            "jpeg".to_string(),
            compressed as i64,
            uncompressed as i64,
            vec![640, 480],
            uid,
            0,
            0,
        )
    }

    #[test]
    fn test_row_new() {
        let r = make_image(1, 1024, 42);
        assert_eq!(r.pv_id, 1);
        assert_eq!(r.data_size(), 1024);
        assert_eq!(r.unique_id, 42);
        assert_eq!(r.codec, "raw");
        assert_eq!(r.dimensions, vec![100, 100]);
        assert_eq!(r.severity, 0);
        assert_eq!(r.status, 0);
        assert!(r.attributes.is_object());
    }

    #[test]
    fn test_row_with_attributes() {
        let r = make_image(1, 100, 1).with_attributes(json!({"exposure": 0.001, "gain": 2}));
        assert_eq!(r.attributes["exposure"], 0.001);
        assert_eq!(r.attributes["gain"], 2);
    }

    #[test]
    fn test_row_codec_compressed() {
        let r = make_compressed(1, 50, 1000, 1);
        assert_eq!(r.codec, "jpeg");
        assert_eq!(r.compressed_size, 50);
        assert_eq!(r.uncompressed_size, 1000);
        assert_eq!(r.data_size(), 50);
    }

    #[test]
    fn test_row_mem_size() {
        let small = make_image(1, 100, 1);
        let large = make_image(1, 10_000, 2);
        assert!(large.mem_size() > small.mem_size());
        // mem_size includes ROW_OVERHEAD + data.len() + codec.len() + dims
        assert!(small.mem_size() >= ROW_OVERHEAD + 100);
    }

    #[test]
    fn test_row_mem_size_includes_codec() {
        let short_codec = ImageRow::new(
            Utc::now(),
            1,
            vec![0; 100],
            "raw".to_string(),
            100,
            100,
            vec![10, 10],
            1,
            0,
            0,
        );
        let long_codec = ImageRow::new(
            Utc::now(),
            1,
            vec![0; 100],
            "blosc_lz4_shuffle_level9".to_string(),
            100,
            100,
            vec![10, 10],
            1,
            0,
            0,
        );
        assert!(long_codec.mem_size() > short_codec.mem_size());
    }

    #[test]
    fn test_row_mem_size_includes_dimensions() {
        let d2 = ImageRow::new(
            Utc::now(),
            1,
            vec![0; 100],
            "".to_string(),
            100,
            100,
            vec![10, 10],
            1,
            0,
            0,
        );
        let d3 = ImageRow::new(
            Utc::now(),
            1,
            vec![0; 100],
            "".to_string(),
            100,
            100,
            vec![10, 10, 10],
            1,
            0,
            0,
        );
        assert!(d3.mem_size() > d2.mem_size());
    }

    #[test]
    fn test_row_compression_ratio_raw() {
        let r = make_image(1, 1000, 1); // compressed == uncompressed
        assert!((r.compression_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_row_compression_ratio_compressed() {
        let r = make_compressed(1, 100, 1000, 1);
        assert!((r.compression_ratio() - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_row_compression_ratio_zero_safe() {
        let r = ImageRow::new(Utc::now(), 1, vec![], "".to_string(), 0, 0, vec![], 1, 0, 0);
        assert!((r.compression_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_row_compression_ratio_negative_safe() {
        let r = ImageRow::new(
            Utc::now(),
            1,
            vec![],
            "".to_string(),
            -1,
            -1,
            vec![],
            1,
            0,
            0,
        );
        assert!((r.compression_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_row_clone() {
        let a = make_image(1, 512, 1);
        let b = a.clone();
        assert_eq!(a.data.len(), b.data.len());
        assert_eq!(a.unique_id, b.unique_id);
        assert_eq!(a.mem_size(), b.mem_size());
    }

    #[test]
    fn test_row_display() {
        let s = make_image(42, 10240, 7).to_string();
        assert!(s.contains("pv_id=42"));
        assert!(s.contains("uid=7"));
        assert!(s.contains("100×100px"));
        assert!(s.contains("KB"));
    }

    #[test]
    fn test_row_display_empty_dims() {
        let r = ImageRow::new(Utc::now(), 1, vec![], "".to_string(), 0, 0, vec![], 1, 0, 0);
        let s = r.to_string();
        assert!(s.contains("0×0px"));
    }

    #[test]
    fn test_row_debug() {
        assert!(format!("{:?}", make_image(1, 100, 1)).contains("ImageRow"));
    }

    #[test]
    fn test_push_result_accepted() {
        assert!(!PushResult::Accepted.needs_flush());
        assert!(PushResult::Accepted.is_accepted());
        assert_eq!(PushResult::Accepted.to_string(), "accepted");
    }

    #[test]
    fn test_push_result_full() {
        assert!(PushResult::Full.needs_flush());
        assert!(PushResult::Full.is_accepted());
        assert_eq!(PushResult::Full.to_string(), "full");
    }

    #[test]
    fn test_push_result_backpressure() {
        assert!(PushResult::BackpressureExceeded.needs_flush());
        assert!(!PushResult::BackpressureExceeded.is_accepted());
        assert_eq!(PushResult::BackpressureExceeded.to_string(), "backpressure");
    }

    #[test]
    fn test_push_result_eq() {
        assert_eq!(PushResult::Full, PushResult::Full);
        assert_ne!(PushResult::Full, PushResult::Accepted);
    }

    #[test]
    fn test_writer_defaults() {
        let w = ImageWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.batch_size(), 50);
        assert_eq!(w.max_buffer_bytes(), DEFAULT_MAX_BUFFER_BYTES);
        assert_eq!(w.buffered_bytes(), 0);
        assert_eq!(w.memory_pressure(), 0.0);
        assert_eq!(w.total_written(), 0);
        assert_eq!(w.total_flushes(), 0);
        assert_eq!(w.total_bytes(), 0);
        assert_eq!(w.total_uncompressed_bytes(), 0);
        assert_eq!(w.total_backpressure(), 0);
        assert_eq!(w.avg_image_bytes(), 0.0);
        assert_eq!(w.avg_compression_ratio(), 1.0);
    }

    #[test]
    fn test_writer_custom_limits() {
        let w = ImageWriter::with_limits(10, 1024 * 1024);
        assert_eq!(w.batch_size(), 10);
        assert_eq!(w.max_buffer_bytes(), 1024 * 1024);
    }

    #[test]
    fn test_min_batch_size() {
        assert_eq!(ImageWriter::new(0).batch_size(), 1);
    }

    #[test]
    fn test_min_buffer_bytes() {
        assert_eq!(ImageWriter::with_limits(10, 0).max_buffer_bytes(), 1024);
    }

    #[test]
    fn test_push_accepted() {
        let mut w = ImageWriter::with_defaults();
        assert_eq!(w.push(make_image(1, 1000, 1)), PushResult::Accepted);
        assert_eq!(w.buffered(), 1);
        assert!(w.buffered_bytes() >= 1000);
    }

    #[test]
    fn test_push_until_full() {
        let mut w = ImageWriter::new(3);
        assert_eq!(w.push(make_image(1, 100, 1)), PushResult::Accepted);
        assert_eq!(w.push(make_image(2, 100, 2)), PushResult::Accepted);
        assert_eq!(w.push(make_image(3, 100, 3)), PushResult::Full);
        assert!(w.is_full());
        assert_eq!(w.buffered(), 3);
    }

    #[test]
    fn test_push_bytes_tracked() {
        let mut w = ImageWriter::with_defaults();
        w.push(make_image(1, 1000, 1));
        let b1 = w.buffered_bytes();
        w.push(make_image(2, 2000, 2));
        let b2 = w.buffered_bytes();
        assert!(b2 > b1);
        assert!(b2 >= 3000); // at least the data payload
    }

    // ── Backpressure ─────────────────────────────────────────────────

    #[test]
    fn test_backpressure_triggered() {
        let mut w = ImageWriter::with_limits(100, 2048);
        assert_eq!(w.push(make_image(1, 1000, 1)), PushResult::Accepted);
        let r = w.push(make_image(2, 1000, 2));
        let mut w2 = ImageWriter::with_limits(100, 1500);
        assert!(w2.push(make_image(1, 1000, 1)).is_accepted());
        assert_eq!(
            w2.push(make_image(2, 1000, 2)),
            PushResult::BackpressureExceeded
        );
        assert!(w2.total_backpressure() > 0);
        // Only 1 image buffered (second was rejected)
        assert_eq!(w2.buffered(), 1);
    }

    #[test]
    fn test_backpressure_first_row_always_accepted() {
        let mut w = ImageWriter::with_limits(100, 1); // 1 byte limit
        let big = make_image(1, 10_000, 1); // 10 KB image
        assert!(w.push(big).is_accepted());
        assert_eq!(w.buffered(), 1);
    }

    #[test]
    fn test_backpressure_clears_after_discard() {
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
    fn test_avg_image_bytes() {
        let mut w = ImageWriter::with_defaults();
        w.total_written = 10;
        w.total_bytes = 10_000;
        assert_eq!(w.avg_image_bytes(), 1000.0);
    }

    #[test]
    fn test_avg_image_bytes_empty() {
        assert_eq!(ImageWriter::with_defaults().avg_image_bytes(), 0.0);
    }

    #[test]
    fn test_avg_compression_ratio() {
        let mut w = ImageWriter::with_defaults();
        w.total_bytes = 100;
        w.total_uncompressed_bytes = 1000;
        assert!((w.avg_compression_ratio() - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_compression_ratio_raw() {
        let mut w = ImageWriter::with_defaults();
        w.total_bytes = 500;
        w.total_uncompressed_bytes = 500;
        assert!((w.avg_compression_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_compression_ratio_empty() {
        assert_eq!(ImageWriter::with_defaults().avg_compression_ratio(), 1.0);
    }

    #[test]
    fn test_memory_pressure() {
        let mut w = ImageWriter::with_limits(100, 10_000);
        w.push(make_image(1, 5000, 1));
        let p = w.memory_pressure();
        assert!(p > 0.4 && p < 0.7, "pressure={p}"); // ~5200/10000
    }

    #[test]
    fn test_discard() {
        let mut w = ImageWriter::with_defaults();
        w.push(make_image(1, 1000, 1));
        w.push(make_image(2, 2000, 2));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
    }

    #[test]
    fn test_insert_sql() {
        assert!(INSERT_SQL.contains("samples_image"));
        assert!(INSERT_SQL.contains("$11"));
        assert!(INSERT_SQL.contains("attributes"));
    }

    #[test]
    fn test_display() {
        let mut w = ImageWriter::with_defaults();
        w.push(make_image(1, 10240, 1));
        w.total_written = 100;
        w.total_bytes = 1_000_000;
        w.total_backpressure = 2;
        let s = w.to_string();
        assert!(s.contains("1/50"));
        assert!(s.contains("100 written"));
        assert!(s.contains("2 backpressure"));
    }

    #[test]
    fn test_debug() {
        let d = format!("{:?}", ImageWriter::with_defaults());
        assert!(d.contains("ImageWriter"));
        assert!(d.contains("pressure"));
        assert!(d.contains("compression"));
        assert!(d.contains("backpressure"));
    }
}