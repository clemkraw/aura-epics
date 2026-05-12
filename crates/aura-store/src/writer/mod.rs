//! Batch writer orchestrator — dispatches samples to type-specific writers.
//!
//! The [`BatchWriter`] is the single entry point for all database writes.
//! It receives `PvUpdate` + `StoreReason` pairs from the pipeline,
//! resolves `pv_name → pv_id` via [`PvCache`], converts the Normative Type
//! to the correct row type, and pushes it to the appropriate writer.
//!
//! ## Dispatch by PvDataType
//!
//! ```text
//! PvDataType::Scalar      → ScalarWriter    → samples
//! PvDataType::String      → StringWriter    → samples_string
//! PvDataType::Array       → ArrayWriter     → samples_array
//! PvDataType::Matrix      → ArrayWriter     → samples_array (with dim)
//! PvDataType::Image       → ImageWriter     → samples_image
//! PvDataType::Table       → JsonWriter      → samples_table
//! PvDataType::Histogram   → JsonWriter      → samples_histogram
//! PvDataType::Continuum   → JsonWriter      → samples_continuum
//! PvDataType::NameValue   → JsonWriter      → samples_namevalue
//! PvDataType::MultiChannel→ JsonWriter      → samples_multi
//! PvDataType::Custom      → JsonWriter      → samples_custom
//! PvDataType::Union       → JsonWriter      → samples_custom
//! PvDataType::Aggregate   → ScalarWriter    → samples (value as f64)
//! ```
//!
//! ## Performance
//!
//! - **Parallel flush**: `flush_all()` uses `tokio::try_join!` to flush
//!   all 5 writers concurrently on the connection pool. Total latency =
//!   max(writers) instead of sum(writers).
//! - **Zero-copy dispatch**: `dispatch()` takes `&mut PvUpdate` to
//!   `std::mem::take` image data instead of cloning (saves MB per frame).
//! - **Error-tracked serde**: JSON serialization failures increment
//!   `total_errors` and skip the sample (no silent `unwrap_or_default`).
//!
//! ## Flushing
//!
//! `flush_all()` flushes every writer. Called when:
//! - Any writer's buffer is full
//! - The flush timer expires (100ms)
//! - A consumer batch is fully processed (before XACK)

use std::fmt;

use sqlx::PgPool;

use aura_core::error::{AuraError, AuraResult};
use aura_core::pva::{NormativeType, PvDataType};
use aura_core::sample::StoreReason;
use aura_core::PvUpdate;

pub mod array;
pub mod image;
pub mod json;
pub mod pv_cache;
pub mod scalar;
pub mod string;

use array::{ArrayRow, ArrayWriter};
use image::{ImageRow, ImageWriter};
use json::{JsonRow, JsonWriter};
use pv_cache::PvCache;
use scalar::{ScalarRow, ScalarWriter};
use string::{StringRow, StringWriter};

/// Configuration for the batch writer.
#[derive(Debug, Clone)]
pub struct BatchWriterConfig {
    pub scalar_batch_size: usize,
    pub string_batch_size: usize,
    pub array_batch_size: usize,
    pub json_batch_size: usize,
    pub image_batch_size: usize,
}

impl Default for BatchWriterConfig {
    fn default() -> Self {
        Self {
            scalar_batch_size: 500,
            string_batch_size: 500,
            array_batch_size: 200,
            json_batch_size: 200,
            image_batch_size: 50,
        }
    }
}

/// Orchestrates all type-specific writers.
///
/// One instance per `aura-store` process.
pub struct BatchWriter {
    pv_cache: PvCache,
    scalar: ScalarWriter,
    string: StringWriter,
    array: ArrayWriter,
    json: JsonWriter,
    image: ImageWriter,
    total_dispatched: u64,
    total_errors: u64,
}

impl BatchWriter {
    pub fn new(config: BatchWriterConfig) -> Self {
        Self {
            pv_cache: PvCache::new(),
            scalar: ScalarWriter::new(config.scalar_batch_size),
            string: StringWriter::new(config.string_batch_size),
            array: ArrayWriter::new(config.array_batch_size),
            json: JsonWriter::new(config.json_batch_size),
            image: ImageWriter::new(config.image_batch_size),
            total_dispatched: 0,
            total_errors: 0,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(BatchWriterConfig::default())
    }

    pub async fn warm_cache(&mut self, pool: &PgPool) -> AuraResult<usize> {
        self.pv_cache.warm(pool).await
    }

    /// Dispatch a PV update to the correct writer.
    ///
    /// Returns `true` if any writer buffer is full and should be flushed.
    pub async fn dispatch(
        &mut self,
        update: &mut PvUpdate,
        reason: StoreReason,
        pool: &PgPool,
    ) -> AuraResult<bool> {
        let pv_id = self.pv_cache.resolve(&update.pv_name, pool).await?;
        let time = update.timestamp();
        let severity = update.severity();
        let status = update.status();
        let data_type = update.data_type();

        self.total_dispatched += 1;

        match data_type {
            PvDataType::Scalar | PvDataType::Aggregate => {
                let value = update.as_f64().unwrap_or(0.0);
                self.scalar.push(ScalarRow::new(time, pv_id, value, severity, status, reason));
            }

            PvDataType::String => {
                let value = extract_string(&update.data);
                self.string.push(StringRow::new(time, pv_id, value, severity, status));
            }

            PvDataType::Array => {
                let values = extract_array_f64(&update.data);
                self.array.push(ArrayRow::array(time, pv_id, values, severity, status));
            }

            PvDataType::Matrix => {
                let (values, dim) = extract_matrix(&update.data);
                self.array.push(ArrayRow::matrix(time, pv_id, values, dim, severity, status));
            }

            PvDataType::Image => {
                let row = extract_image_move(time, pv_id, &mut update.data, severity, status);
                self.image.push(row);
            }

            PvDataType::Table
            | PvDataType::Histogram
            | PvDataType::Continuum
            | PvDataType::NameValue
            | PvDataType::MultiChannel
            | PvDataType::Union
            | PvDataType::Custom => {
                match serde_json::to_value(&update.data) {
                    Ok(data) => {
                        let row = match data_type {
                            PvDataType::Table => JsonRow::table(time, pv_id, data, severity, status),
                            PvDataType::Histogram => JsonRow::histogram(time, pv_id, data, severity, status),
                            PvDataType::Continuum => JsonRow::continuum(time, pv_id, data, severity, status),
                            PvDataType::NameValue => JsonRow::namevalue(time, pv_id, data, severity, status),
                            PvDataType::MultiChannel => JsonRow::multi(time, pv_id, data, severity, status),
                            _ => {
                                // Union | Custom
                                let nt = update.data.type_name();
                                JsonRow::custom(time, pv_id, nt, data, severity, status)
                            }
                        };
                        self.json.push(row);
                    }
                    Err(e) => {
                        self.total_errors += 1;
                        tracing::warn!(
                            pv = %update.pv_name,
                            data_type = ?data_type,
                            error = %e,
                            "JSON serialization failed, sample dropped"
                        );
                    }
                }
            }
        }

        Ok(self.scalar.is_full()
            || self.string.is_full()
            || self.array.is_full()
            || self.json.is_full()
            || self.image.is_full())
    }

    /// Flush all writers concurrently via `tokio::try_join!`.
    ///
    /// Total latency = max(writers) instead of sum(writers).
    /// Each writer gets its own connection from the pool.
    pub async fn flush_all(&mut self, pool: &PgPool) -> AuraResult<FlushReport> {
        let (scalar, string, array, json, image) = tokio::try_join!(
            self.scalar.flush(pool),
            self.string.flush(pool),
            self.array.flush(pool),
            self.json.flush(pool),
            self.image.flush(pool),
        )?;

        Ok(FlushReport { scalar, string, array, json, image })
    }

    /// Discard all buffered rows without writing (error recovery).
    pub fn discard_all(&mut self) {
        self.scalar.discard();
        self.string.discard();
        self.array.discard();
        self.json.discard();
        self.image.discard();
    }

    pub fn total_buffered(&self) -> usize {
        self.scalar.buffered()
            + self.string.buffered()
            + self.array.buffered()
            + self.json.buffered()
            + self.image.buffered()
    }

    pub fn has_pending(&self) -> bool {
        !self.scalar.is_empty()
            || !self.string.is_empty()
            || !self.array.is_empty()
            || !self.json.is_empty()
            || !self.image.is_empty()
    }

    #[inline] pub fn total_dispatched(&self) -> u64 { self.total_dispatched }
    #[inline] pub fn total_errors(&self) -> u64 { self.total_errors }
    #[inline] pub fn pv_cache(&self) -> &PvCache { &self.pv_cache }
    #[inline] pub fn scalar_writer(&self) -> &ScalarWriter { &self.scalar }
    #[inline] pub fn string_writer(&self) -> &StringWriter { &self.string }
    #[inline] pub fn array_writer(&self) -> &ArrayWriter { &self.array }
    #[inline] pub fn json_writer(&self) -> &JsonWriter { &self.json }
    #[inline] pub fn image_writer(&self) -> &ImageWriter { &self.image }

    /// Snapshot of all writer statistics.
    pub fn stats(&self) -> WriterStats {
        WriterStats {
            dispatched: self.total_dispatched,
            errors: self.total_errors,
            buffered: self.total_buffered(),
            scalar_written: self.scalar.total_written(),
            string_written: self.string.total_written(),
            array_written: self.array.total_written(),
            json_written: self.json.total_written(),
            image_written: self.image.total_written(),
            image_bytes: self.image.total_bytes(),
            cache_entries: self.pv_cache.len(),
            cache_hit_ratio: self.pv_cache.hit_ratio(),
            scalar_backpressure: self.scalar.total_backpressure(),
            string_backpressure: self.string.total_backpressure(),
            array_backpressure: self.array.total_backpressure(),
            json_backpressure: self.json.total_backpressure(),
            image_backpressure: self.image.total_backpressure(),
        }
    }
}

/// Report from a single `flush_all` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushReport {
    pub scalar: usize,
    pub string: usize,
    pub array: usize,
    pub json: usize,
    pub image: usize,
}

impl FlushReport {
    pub fn total(&self) -> usize {
        self.scalar + self.string + self.array + self.json + self.image
    }
    pub fn is_empty(&self) -> bool { self.total() == 0 }
}

impl fmt::Display for FlushReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "flushed {} rows (scalar={}, string={}, array={}, json={}, image={})",
               self.total(), self.scalar, self.string, self.array, self.json, self.image)
    }
}

/// Snapshot of all writer statistics with backpressure monitoring.
#[derive(Debug, Clone)]
pub struct WriterStats {
    pub dispatched: u64,
    pub errors: u64,
    pub buffered: usize,
    pub scalar_written: u64,
    pub string_written: u64,
    pub array_written: u64,
    pub json_written: u64,
    pub image_written: u64,
    pub image_bytes: u64,
    pub cache_entries: usize,
    pub cache_hit_ratio: f64,
    // Backpressure counters per writer.
    pub scalar_backpressure: u64,
    pub string_backpressure: u64,
    pub array_backpressure: u64,
    pub json_backpressure: u64,
    pub image_backpressure: u64,
}

impl WriterStats {
    pub fn total_written(&self) -> u64 {
        self.scalar_written + self.string_written + self.array_written
            + self.json_written + self.image_written
    }

    /// Total backpressure events across all writers.
    pub fn total_backpressure(&self) -> u64 {
        self.scalar_backpressure + self.string_backpressure
            + self.array_backpressure + self.json_backpressure
            + self.image_backpressure
    }

    pub fn has_backpressure(&self) -> bool {
        self.total_backpressure() > 0
    }
}

impl fmt::Display for WriterStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dispatched={} written={} buffered={} errors={} \
                    cache_hit={:.1}% backpressure={}",
               self.dispatched, self.total_written(), self.buffered,
               self.errors, self.cache_hit_ratio * 100.0,
               self.total_backpressure())
    }
}

/// Extract string value from NTScalar(String).
fn extract_string(nt: &NormativeType) -> String {
    match nt {
        NormativeType::NTScalar(s) => {
            match &s.value {
                aura_core::pva::ScalarValue::String(v) => v.clone(),
                other => format!("{:?}", other),
            }
        }
        _ => String::new(),
    }
}

/// Extract f64 array from NTScalarArray.
fn extract_array_f64(nt: &NormativeType) -> Vec<f64> {
    match nt {
        NormativeType::NTScalarArray(a) => {
            a.value.as_f64_vec().unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

/// Extract matrix values + dimensions from NTMatrix.
fn extract_matrix(nt: &NormativeType) -> (Vec<f64>, Vec<i32>) {
    match nt {
        NormativeType::NTMatrix(m) => (m.value.clone(), m.dim.clone()),
        _ => (Vec::new(), Vec::new()),
    }
}

/// Extract image row via `std::mem::take` — zero-copy for the data payload.
///
/// Takes `&mut NormativeType` and moves the BYTEA data out instead of
/// cloning.
fn extract_image_move(
    time: chrono::DateTime<chrono::Utc>,
    pv_id: i32,
    nt: &mut NormativeType,
    severity: i16,
    status: i16,
) -> ImageRow {
    match nt {
        NormativeType::NTNDArray(img) => {
            // Move pixel data out — replaces with empty vec, no clone.
            let data = match &mut img.value {
                aura_core::pva::ArrayValue::UByteArray(v) => std::mem::take(v),
                _ => Vec::new(),
            };
            let dims: Vec<i32> = img.dimension.iter().map(|d| d.size).collect();
            let codec = std::mem::take(&mut img.codec.name);
            ImageRow::new(
                time, pv_id, data, codec,
                img.compressed_size,
                img.uncompressed_size,
                dims, img.unique_id,
                severity, status,
            )
        }
        _ => ImageRow::new(
            time, pv_id, Vec::new(), String::new(),
            0, 0, Vec::new(), 0, severity, status,
        ),
    }
}

/// Non-mutating image extraction for tests or when move is not possible.
fn extract_image(
    time: chrono::DateTime<chrono::Utc>,
    pv_id: i32,
    nt: &NormativeType,
    severity: i16,
    status: i16,
) -> ImageRow {
    match nt {
        NormativeType::NTNDArray(img) => {
            let data = match &img.value {
                aura_core::pva::ArrayValue::UByteArray(v) => v.clone(),
                _ => Vec::new(),
            };
            let dims: Vec<i32> = img.dimension.iter().map(|d| d.size).collect();
            ImageRow::new(
                time, pv_id, data, img.codec.name.clone(),
                img.compressed_size,
                img.uncompressed_size,
                dims, img.unique_id,
                severity, status,
            )
        }
        _ => ImageRow::new(
            time, pv_id, Vec::new(), String::new(),
            0, 0, Vec::new(), 0, severity, status,
        ),
    }
}

impl fmt::Debug for BatchWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchWriter")
            .field("dispatched", &self.total_dispatched)
            .field("errors", &self.total_errors)
            .field("buffered", &self.total_buffered())
            .field("cache", &self.pv_cache.len())
            .finish()
    }
}

impl fmt::Display for BatchWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BatchWriter: {} dispatched, {} errors, {} buffered, {} PVs cached",
               self.total_dispatched, self.total_errors,
               self.total_buffered(), self.pv_cache.len())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use super::*;

    // ── BatchWriterConfig ────────────────────────────────────────────

    #[test]
    fn test_config_default() {
        let cfg = BatchWriterConfig::default();
        assert_eq!(cfg.scalar_batch_size, 500);
        assert_eq!(cfg.string_batch_size, 500);
        assert_eq!(cfg.array_batch_size, 200);
        assert_eq!(cfg.json_batch_size, 200);
        assert_eq!(cfg.image_batch_size, 50);
    }

    #[test]
    fn test_config_clone() {
        let a = BatchWriterConfig::default();
        let b = a.clone();
        assert_eq!(a.scalar_batch_size, b.scalar_batch_size);
        assert_eq!(a.image_batch_size, b.image_batch_size);
    }

    #[test]
    fn test_config_debug() {
        let d = format!("{:?}", BatchWriterConfig::default());
        assert!(d.contains("scalar_batch_size"));
    }

    // ── BatchWriter construction ─────────────────────────────────────

    #[test]
    fn test_new() {
        let w = BatchWriter::with_defaults();
        assert_eq!(w.total_dispatched(), 0);
        assert_eq!(w.total_errors(), 0);
        assert_eq!(w.total_buffered(), 0);
        assert!(!w.has_pending());
    }

    #[test]
    fn test_custom_config() {
        let cfg = BatchWriterConfig {
            scalar_batch_size: 1000,
            string_batch_size: 100,
            array_batch_size: 50,
            json_batch_size: 50,
            image_batch_size: 10,
        };
        let w = BatchWriter::new(cfg);
        assert_eq!(w.scalar_writer().batch_size(), 1000);
    }

    // ── discard_all ──────────────────────────────────────────────────

    #[test]
    fn test_discard_all() {
        let mut w = BatchWriter::with_defaults();
        w.scalar.push(ScalarRow::new(Utc::now(), 1, 10.0, 0, 0, StoreReason::Initial));
        w.string.push(StringRow::new(Utc::now(), 1, "x".to_string(), 0, 0));
        assert!(w.has_pending());
        assert_eq!(w.total_buffered(), 2);
        w.discard_all();
        assert!(!w.has_pending());
        assert_eq!(w.total_buffered(), 0);
    }

    // ── FlushReport ──────────────────────────────────────────────────

    #[test]
    fn test_flush_report_total() {
        let r = FlushReport { scalar: 100, string: 5, array: 10, json: 3, image: 2 };
        assert_eq!(r.total(), 120);
        assert!(!r.is_empty());
    }

    #[test]
    fn test_flush_report_empty() {
        let r = FlushReport { scalar: 0, string: 0, array: 0, json: 0, image: 0 };
        assert!(r.is_empty());
    }

    #[test]
    fn test_flush_report_display() {
        let r = FlushReport { scalar: 100, string: 0, array: 0, json: 0, image: 0 };
        let s = r.to_string();
        assert!(s.contains("100 rows"));
        assert!(s.contains("scalar=100"));
    }

    #[test]
    fn test_flush_report_eq() {
        let a = FlushReport { scalar: 1, string: 2, array: 3, json: 4, image: 5 };
        let b = a;
        assert_eq!(a, b);
    }

    // ── WriterStats ──────────────────────────────────────────────────

    #[test]
    fn test_writer_stats_total() {
        let stats = WriterStats {
            dispatched: 1000, errors: 0, buffered: 5,
            scalar_written: 800, string_written: 50, array_written: 30,
            json_written: 20, image_written: 10, image_bytes: 50000,
            cache_entries: 100, cache_hit_ratio: 0.999,
            scalar_backpressure: 0, string_backpressure: 0,
            array_backpressure: 0, json_backpressure: 0, image_backpressure: 0,
        };
        assert_eq!(stats.total_written(), 910);
        assert_eq!(stats.total_backpressure(), 0);
        assert!(!stats.has_backpressure());
    }

    #[test]
    fn test_writer_stats_with_backpressure() {
        let stats = WriterStats {
            dispatched: 1000, errors: 5, buffered: 0,
            scalar_written: 900, string_written: 0, array_written: 0,
            json_written: 0, image_written: 0, image_bytes: 0,
            cache_entries: 50, cache_hit_ratio: 0.95,
            scalar_backpressure: 3, string_backpressure: 0,
            array_backpressure: 0, json_backpressure: 1, image_backpressure: 0,
        };
        assert_eq!(stats.total_backpressure(), 4);
        assert!(stats.has_backpressure());
    }

    #[test]
    fn test_writer_stats_display() {
        let w = BatchWriter::with_defaults();
        let s = w.stats().to_string();
        assert!(s.contains("dispatched=0"));
        assert!(s.contains("written=0"));
        assert!(s.contains("backpressure=0"));
    }

    // ── Individual writer accessors ──────────────────────────────────

    #[test]
    fn test_writer_accessors() {
        let w = BatchWriter::with_defaults();
        assert!(w.scalar_writer().is_empty());
        assert!(w.string_writer().is_empty());
        assert!(w.array_writer().is_empty());
        assert!(w.json_writer().is_empty());
        assert!(w.image_writer().is_empty());
        assert!(w.pv_cache().is_empty());
    }

    // ── Extraction helpers ───────────────────────────────────────────

    #[test]
    fn test_extract_string_from_nt_scalar() {
        use aura_core::pva::*;
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::String("hello".to_string()),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None, control: None, value_alarm: None,
        });
        assert_eq!(extract_string(&nt), "hello");
    }

    #[test]
    fn test_extract_string_non_string_scalar() {
        use aura_core::pva::*;
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(42.0),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None, control: None, value_alarm: None,
        });
        let s = extract_string(&nt);
        assert!(!s.is_empty());
    }

    #[test]
    fn test_extract_string_non_scalar() {
        use aura_core::pva::*;
        let nt = NormativeType::NTTable(NTTable {
            labels: vec![], columns: vec![],
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
        });
        assert_eq!(extract_string(&nt), "");
    }

    #[test]
    fn test_extract_array_f64() {
        use aura_core::pva::*;
        let nt = NormativeType::NTScalarArray(NTScalarArray {
            value: ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None, control: None, value_alarm: None,
        });
        assert_eq!(extract_array_f64(&nt), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_extract_array_non_array() {
        use aura_core::pva::*;
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(1.0),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None, control: None, value_alarm: None,
        });
        assert!(extract_array_f64(&nt).is_empty());
    }

    #[test]
    fn test_extract_matrix() {
        use aura_core::pva::*;
        let nt = NormativeType::NTMatrix(NTMatrix {
            value: vec![1.0, 2.0, 3.0, 4.0],
            dim: vec![2, 2],
            descriptor: String::new(),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None,
        });
        let (values, dim) = extract_matrix(&nt);
        assert_eq!(values, vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(dim, vec![2, 2]);
    }

    #[test]
    fn test_extract_matrix_non_matrix() {
        use aura_core::pva::*;
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(1.0),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None, control: None, value_alarm: None,
        });
        let (v, d) = extract_matrix(&nt);
        assert!(v.is_empty());
        assert!(d.is_empty());
    }

    #[test]
    fn test_extract_image() {
        use aura_core::pva::*;
        let nt = NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![0xFF; 100]),
            codec: Codec { name: "jpeg".to_string(), parameters: serde_json::Value::Null },
            compressed_size: 100,
            uncompressed_size: 1000,
            dimension: vec![
                Dimension { size: 640, offset: 0, full_size: 640, binning: 1, reverse: false },
                Dimension { size: 480, offset: 0, full_size: 480, binning: 1, reverse: false },
            ],
            unique_id: 42,
            data_timestamp: None,
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            attribute: vec![],
        });
        let row = extract_image(Utc::now(), 7, &nt, 0, 0);
        assert_eq!(row.pv_id, 7);
        assert_eq!(row.data.len(), 100);
        assert_eq!(row.codec, "jpeg");
        assert_eq!(row.compressed_size, 100);
        assert_eq!(row.uncompressed_size, 1000);
        assert_eq!(row.dimensions, vec![640, 480]);
        assert_eq!(row.unique_id, 42);
    }

    #[test]
    fn test_extract_image_non_image() {
        use aura_core::pva::*;
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(0.0),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None, control: None, value_alarm: None,
        });
        let row = extract_image(Utc::now(), 1, &nt, 0, 0);
        assert!(row.data.is_empty());
    }

    #[test]
    fn test_extract_image_move() {
        use aura_core::pva::*;
        let mut nt = NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![0xAB; 500]),
            codec: Codec { name: "blosc".to_string(), parameters: serde_json::Value::Null },
            compressed_size: 500,
            uncompressed_size: 2000,
            dimension: vec![
                Dimension { size: 320, offset: 0, full_size: 320, binning: 1, reverse: false },
                Dimension { size: 240, offset: 0, full_size: 240, binning: 1, reverse: false },
            ],
            unique_id: 99,
            data_timestamp: None,
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            attribute: vec![],
        });
        let row = extract_image_move(Utc::now(), 5, &mut nt, 0, 0);
        assert_eq!(row.data.len(), 500);
        assert_eq!(row.codec, "blosc");
        assert_eq!(row.dimensions, vec![320, 240]);
        // Original data should be emptied (moved out).
        if let NormativeType::NTNDArray(img) = &nt {
            if let ArrayValue::UByteArray(v) = &img.value {
                assert!(v.is_empty(), "data should have been moved out");
            }
            assert!(img.codec.name.is_empty(), "codec should have been moved out");
        }
    }

    #[test]
    fn test_extract_image_move_non_image() {
        use aura_core::pva::*;
        let mut nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(0.0),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None, control: None, value_alarm: None,
        });
        let row = extract_image_move(Utc::now(), 1, &mut nt, 0, 0);
        assert!(row.data.is_empty());
    }

    // ── Display / Debug ──────────────────────────────────────────────

    #[test]
    fn test_batch_writer_display() {
        let w = BatchWriter::with_defaults();
        let s = w.to_string();
        assert!(s.contains("BatchWriter"));
        assert!(s.contains("0 dispatched"));
        assert!(s.contains("0 errors"));
    }

    #[test]
    fn test_batch_writer_debug() {
        let w = BatchWriter::with_defaults();
        let d = format!("{:?}", w);
        assert!(d.contains("BatchWriter"));
        assert!(d.contains("dispatched"));
        assert!(d.contains("errors"));
        assert!(d.contains("cache"));
    }
}