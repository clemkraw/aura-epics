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

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::postgres::PgPool;

use aura_core::PvUpdate;
use aura_core::error::AuraResult;
use aura_core::pva::{NormativeType, PvDataType};
use aura_core::sample::StoreReason;

pub mod array;
pub mod copy_pool;
pub mod image;
pub mod json;
pub mod pv_cache;
pub mod scalar;
pub mod shared_buf;
pub mod string;

use array::{ArrayCapture, ArrayData, ArrayWriter};
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
    pub(crate) json: JsonWriter,
    pub(crate) image: ImageWriter,
    copy_pool: copy_pool::CopyPool,
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
            copy_pool: copy_pool::CopyPool::new(),
            total_dispatched: 0,
            total_errors: 0,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(BatchWriterConfig::default())
    }

    #[inline]
    pub fn copy_pool(&self) -> &copy_pool::CopyPool {
        &self.copy_pool
    }
    #[inline]
    pub fn copy_pool_mut(&mut self) -> &mut copy_pool::CopyPool {
        &mut self.copy_pool
    }

    pub async fn warm_cache(&mut self, pool: &PgPool) -> AuraResult<usize> {
        self.pv_cache.warm(pool).await
    }

    /// Ingest scalar rows directly.
    /// Uses buffer.append for O(1) bulk insert instead of N × push.
    #[inline]
    pub fn ingest_scalars(&mut self, mut rows: Vec<ScalarRow>) -> usize {
        let count = rows.len();
        if count == 0 {
            return 0;
        }
        self.scalar.total_non_finite += rows.iter().filter(|r| r.has_non_finite()).count() as u64;
        self.scalar.buffer.append(&mut rows);
        self.total_dispatched += count as u64;
        count
    }

    /// Ingest non-scalar rows (String, Array, Json, Image).
    pub fn ingest_other_rows(&mut self, rows: Vec<shared_buf::WriterRow>) -> usize {
        let count = rows.len();
        for row in rows {
            match row {
                shared_buf::WriterRow::String(r) => {
                    let _ = self.string.push(r);
                }
                shared_buf::WriterRow::Array(r) => {
                    let _ = self.array.push(*r);
                }
                shared_buf::WriterRow::Json(r) => {
                    let _ = self.json.push(r);
                }
                shared_buf::WriterRow::Image(r) => {
                    let _ = self.image.push(r);
                }
            }
        }
        self.total_dispatched += count as u64;
        count
    }

    /// Dispatch a PV update to the correct writer.
    ///
    /// Uses `dispatch_sync` for the fast path (cache hit, 99.9%).
    /// Falls back to async resolve on cache miss only.
    pub async fn dispatch(
        &mut self,
        update: &mut PvUpdate,
        reason: StoreReason,
        pool: &PgPool,
    ) -> AuraResult<bool> {
        match self.dispatch_sync(update, reason) {
            Some(result) => result,
            None => {
                let pv_id = self.pv_cache.resolve(&update.pv_name, pool).await?;
                self.dispatch_with_id(update, pv_id, reason)
            }
        }
    }

    /// Pure synchronous dispatch — returns None on cache miss.
    #[inline]
    pub fn dispatch_sync(
        &mut self,
        update: &mut PvUpdate,
        reason: StoreReason,
    ) -> Option<AuraResult<bool>> {
        let pv_id = self.pv_cache.resolve_cached(&update.pv_name)?;
        Some(self.dispatch_with_id(update, pv_id, reason))
    }

    #[inline]
    fn dispatch_with_id(
        &mut self,
        update: &mut PvUpdate,
        pv_id: i32,
        reason: StoreReason,
    ) -> AuraResult<bool> {
        self.total_dispatched += 1;

        // FAST PATH: NTScalar (99% of samples)
        if let NormativeType::NTScalar(ref nt) = update.data {
            let value = nt.value.as_f64().unwrap_or(0.0);
            let time = nt.timestamp.to_datetime();
            let severity = nt.alarm.severity as i16;
            let status = nt.alarm.status as i16;
            self.scalar
                .push(ScalarRow::new(time, pv_id, value, severity, status, reason));
            return Ok(self.scalar.is_full());
        }

        // SLOW PATH: all other types
        let time = update.timestamp();
        let severity = update.severity();
        let status = update.status();
        let data_type = update.data_type();

        match data_type {
            PvDataType::Scalar | PvDataType::Aggregate => {
                let value = update.as_f64().unwrap_or(0.0);
                self.scalar
                    .push(ScalarRow::new(time, pv_id, value, severity, status, reason));
            }

            PvDataType::String => {
                let value = extract_string(&update.data);
                self.string
                    .push(StringRow::new(time, pv_id, value, severity, status));
            }

            PvDataType::Array => {
                let values = extract_array_f64(&update.data);
                self.array.push(ArrayCapture {
                    time,
                    pv_id,
                    severity,
                    status,
                    data: ArrayData::Numeric(values),
                });
            }

            PvDataType::Matrix => {
                let (values, _dim) = extract_matrix(&update.data);
                self.array.push(ArrayCapture {
                    time,
                    pv_id,
                    severity,
                    status,
                    data: ArrayData::Numeric(values),
                });
            }

            PvDataType::Image => {
                let row = ImageRow::from_update(time, pv_id, &mut update.data, severity, status);
                self.image.push(row);
            }

            PvDataType::Table | PvDataType::Custom | PvDataType::Union => {
                match serde_json::to_vec(&update.data) {
                    Ok(bytes) => {
                        let row = if data_type == PvDataType::Table {
                            JsonRow::table(time, pv_id, bytes, severity, status)
                        } else {
                            let nt = update.data.type_name();
                            JsonRow::custom(time, pv_id, nt, bytes, severity, status)
                        };
                        self.json.push(row);
                    }
                    Err(e) => {
                        self.total_errors += 1;
                        tracing::warn!(pv = %update.pv_name, error = %e, "JSON serialization failed");
                    }
                }
            }

            PvDataType::Histogram
            | PvDataType::Continuum
            | PvDataType::NameValue
            | PvDataType::MultiChannel => match serde_json::to_value(&update.data) {
                Ok(data) => {
                    let row = match data_type {
                        PvDataType::Histogram => {
                            JsonRow::histogram(time, pv_id, data, severity, status)
                        }
                        PvDataType::Continuum => {
                            JsonRow::continuum(time, pv_id, data, severity, status)
                        }
                        PvDataType::NameValue => {
                            JsonRow::namevalue(time, pv_id, data, severity, status)
                        }
                        PvDataType::MultiChannel => {
                            JsonRow::multi(time, pv_id, data, severity, status)
                        }
                        _ => unreachable!(),
                    };
                    self.json.push(row);
                }
                Err(e) => {
                    self.total_errors += 1;
                    tracing::warn!(pv = %update.pv_name, error = %e, "JSON serialization failed");
                }
            },
        }

        Ok(self.scalar.is_full()
            || self.string.is_full()
            || self.array.is_full()
            || self.json.is_full()
            || self.image.is_full())
    }

    /// Flush all writers concurrently. Total latency = max(writers).
    pub async fn flush_all(&mut self) -> AuraResult<FlushReport> {
        let (scalar, string, array, json, image) = tokio::try_join!(
            self.scalar.flush(),
            self.string.flush(),
            self.array.flush(),
            self.json.flush(),
            self.image.flush(),
        )?;
        Ok(FlushReport {
            scalar,
            string,
            array,
            json,
            image,
        })
    }

    /// Extract all pending buffers for background flushing (ping-pong).
    pub fn take_flush_bundle(&mut self) -> Option<FlushBundle> {
        if !self.has_pending() {
            return None;
        }
        let pool = self.copy_pool.clone();
        Some(FlushBundle {
            scalar_rows: std::mem::take(&mut self.scalar.buffer),
            string_rows: std::mem::take(&mut self.string.buffer),
            array_num_rows: std::mem::take(&mut self.array.num_buffer),
            array_str_rows: std::mem::take(&mut self.array.str_buffer),
            json_rows: std::mem::take(&mut self.json.buffer),
            image_rows: std::mem::take(&mut self.image.buffer),
            pool,
        })
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

    #[inline]
    pub fn pv_cache(&self) -> &PvCache {
        &self.pv_cache
    }
    #[inline]
    pub fn pv_cache_mut(&mut self) -> &mut PvCache {
        &mut self.pv_cache
    }
    #[inline]
    pub fn scalar_writer(&self) -> &ScalarWriter {
        &self.scalar
    }
    #[inline]
    pub fn scalar_writer_mut(&mut self) -> &mut ScalarWriter {
        &mut self.scalar
    }
    #[inline]
    pub fn string_writer(&self) -> &StringWriter {
        &self.string
    }
    #[inline]
    pub fn string_writer_mut(&mut self) -> &mut StringWriter {
        &mut self.string
    }
    #[inline]
    pub fn array_writer(&self) -> &ArrayWriter {
        &self.array
    }
    #[inline]
    pub fn array_writer_mut(&mut self) -> &mut ArrayWriter {
        &mut self.array
    }
    #[inline]
    pub fn json_writer(&self) -> &JsonWriter {
        &self.json
    }
    #[inline]
    pub fn json_writer_mut(&mut self) -> &mut JsonWriter {
        &mut self.json
    }
    #[inline]
    pub fn image_writer(&self) -> &ImageWriter {
        &self.image
    }
    #[inline]
    pub fn image_writer_mut(&mut self) -> &mut ImageWriter {
        &mut self.image
    }

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
    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }
}

impl fmt::Display for FlushReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "flushed {} rows (scalar={}, string={}, array={}, json={}, image={})",
            self.total(),
            self.scalar,
            self.string,
            self.array,
            self.json,
            self.image
        )
    }
}

/// Detached bundle of writer buffers for background flushing.
/// Created by `BatchWriter::take_flush_bundle()` — O(1) pointer swap.
pub struct FlushBundle {
    pub scalar_rows: Vec<ScalarRow>,
    pub string_rows: Vec<StringRow>,
    pub array_num_rows: Vec<array::ArrayNumRow>,
    pub array_str_rows: Vec<array::ArrayStrRow>,
    pub json_rows: Vec<JsonRow>,
    pub image_rows: Vec<ImageRow>,
    pub pool: copy_pool::CopyPool,
}

/// Result of [`FlushBundle::flush`]: what was written, what is retryable,
/// and what is definitively lost.
pub struct FlushOutcome {
    /// Rows successfully committed, per table family.
    pub report: FlushReport,
    /// Transiently-failed rows, ready to be flushed again. `None` = nothing to retry.
    pub retry: Option<Box<FlushBundle>>,
    /// Rows permanently lost in this flush (COPY rows for destructured tables).
    pub lost_rows: usize,
    /// Human-readable error messages (one per failed COPY).
    pub errors: Vec<String>,
}

impl FlushOutcome {
    pub fn is_clean(&self) -> bool {
        self.retry.is_none() && self.lost_rows == 0 && self.errors.is_empty()
    }
}

/// Flush one section (one target table) of a bundle.
///
/// Splits into parallel chunks above `parallel_threshold` (pass
/// `usize::MAX` to force the single-connection path). Returns
/// `(rows_written, rows_to_retry)` and pushes failures into
/// `errors` / `lost_rows`.
#[allow(clippy::too_many_arguments)]
async fn flush_section<T: Send>(
    pool: &copy_pool::CopyPool,
    sql: &'static str,
    rows: Vec<T>,
    build: fn(&[T]) -> Vec<u8>,
    parallel_threshold: usize,
    errors: &mut Vec<String>,
    lost_rows: &mut usize,
    progress: &Arc<AtomicU64>,
) -> (usize, Vec<T>) {
    let count = rows.len();
    if count == 0 {
        return (0, Vec::new());
    }
    let n = pool.len();

    if count >= parallel_threshold && n > 1 {
        let chunk_size = count.div_ceil(n); // >= 1
        let payloads: Vec<(Vec<u8>, usize)> = rows
            .chunks(chunk_size)
            .map(|chunk| (build(chunk), chunk.len()))
            .collect();
        // Per-chunk send with per-chunk progress (equivalent to
        // CopyPool::send_parallel_classified, plus the progress increment
        // in the same poll as each chunk's commit).
        let futs: Vec<_> = payloads
            .into_iter()
            .enumerate()
            .map(|(i, (payload, row_count))| {
                let pool = pool.clone();
                let progress = Arc::clone(progress);
                async move {
                    let r = pool
                        .send_copy_classified(i, sql, bytes::Bytes::from(payload), row_count)
                        .await;
                    if r.is_ok() {
                        progress.fetch_add(row_count as u64, Ordering::Relaxed);
                    }
                    r.map(|()| row_count)
                }
            })
            .collect();
        let results = futures_util::future::join_all(futs).await;

        let mut written = 0usize;
        let mut retry_chunk = vec![false; results.len()];
        for (i, r) in results.iter().enumerate() {
            match r {
                Ok(n_rows) => written += n_rows,
                Err(e) => {
                    errors.push(e.to_string());
                    if e.transient {
                        retry_chunk[i] = true;
                    } else {
                        let start = i * chunk_size;
                        let end = (start + chunk_size).min(count);
                        *lost_rows += end - start;
                    }
                }
            }
        }

        if retry_chunk.iter().any(|&b| b) {
            // Move only the rows of transiently-failed chunks out of `rows`.
            let keep: Vec<T> = rows
                .into_iter()
                .enumerate()
                .filter_map(|(pos, row)| retry_chunk[pos / chunk_size].then_some(row))
                .collect();
            (written, keep)
        } else {
            (written, Vec::new())
        }
    } else {
        let payload = build(&rows);
        match pool
            .send_copy_classified(0, sql, bytes::Bytes::from(payload), count)
            .await
        {
            Ok(()) => {
                progress.fetch_add(count as u64, Ordering::Relaxed);
                (count, Vec::new())
            }
            Err(e) => {
                errors.push(e.to_string());
                if e.transient {
                    (0, rows)
                } else {
                    *lost_rows += count;
                    (0, Vec::new())
                }
            }
        }
    }
}

impl FlushBundle {
    /// Flush all buffers to PostgreSQL via COPY.
    pub async fn flush(self) -> FlushOutcome {
        self.flush_with_progress(Arc::new(AtomicU64::new(0))).await
    }

    /// Like [`FlushBundle::flush`], but increments `progress` by the row
    /// count of each COPY chunk as it commits.
    pub async fn flush_with_progress(self, progress: Arc<AtomicU64>) -> FlushOutcome {
        const PARALLEL_THRESHOLD: usize = 10_000;

        let FlushBundle {
            scalar_rows,
            string_rows,
            array_num_rows,
            array_str_rows,
            json_rows,
            image_rows,
            pool,
        } = self;

        let mut errors: Vec<String> = Vec::new();
        let mut lost_rows = 0usize;

        let (scalar, keep_scalar) = flush_section(
            &pool,
            ScalarWriter::COPY_SQL,
            scalar_rows,
            ScalarWriter::build_copy_payload,
            PARALLEL_THRESHOLD,
            &mut errors,
            &mut lost_rows,
            &progress,
        )
        .await;

        let (string, keep_string) = flush_section(
            &pool,
            string::COPY_SQL,
            string_rows,
            StringWriter::build_copy_payload,
            PARALLEL_THRESHOLD,
            &mut errors,
            &mut lost_rows,
            &progress,
        )
        .await;

        let (array_num, keep_array_num) = flush_section(
            &pool,
            array::COPY_SQL_NUM,
            array_num_rows,
            ArrayWriter::build_num_payload,
            PARALLEL_THRESHOLD,
            &mut errors,
            &mut lost_rows,
            &progress,
        )
        .await;

        let (array_str, keep_array_str) = flush_section(
            &pool,
            array::COPY_SQL_STR,
            array_str_rows,
            ArrayWriter::build_str_payload,
            usize::MAX, // always single-connection (rare rows)
            &mut errors,
            &mut lost_rows,
            &progress,
        )
        .await;

        // ── JSON family: one shared row Vec, six target tables. ──
        // On failure, retain only the rows of the tables whose COPY failed
        // transiently (rows of committed tables must NOT be resent).
        let mut json = 0usize;
        let mut keep_json: Vec<JsonRow> = Vec::new();
        if !json_rows.is_empty() {
            use json::JsonTable;
            type JsonBuilder = fn(&[JsonRow]) -> Option<(Vec<u8>, usize)>;
            let plan: [(JsonTable, &'static str, JsonBuilder); 6] = [
                (
                    JsonTable::Table,
                    json::COPY_TABLE,
                    JsonWriter::build_table_payload,
                ),
                (
                    JsonTable::Custom,
                    json::COPY_CUSTOM,
                    JsonWriter::build_custom_payload,
                ),
                (
                    JsonTable::NameValue,
                    json::COPY_NV,
                    JsonWriter::build_nv_payload,
                ),
                (
                    JsonTable::Histogram,
                    json::COPY_HIST,
                    JsonWriter::build_hist_payload,
                ),
                (
                    JsonTable::Continuum,
                    json::COPY_CONT,
                    JsonWriter::build_cont_payload,
                ),
                (
                    JsonTable::Multi,
                    json::COPY_MCH,
                    JsonWriter::build_mch_payload,
                ),
            ];

            let mut retry_tables: Vec<JsonTable> = Vec::new();
            for (table, sql, build) in plan {
                if let Some((payload, n)) = build(&json_rows) {
                    match pool
                        .send_copy_classified(0, sql, bytes::Bytes::from(payload), n)
                        .await
                    {
                        Ok(()) => {
                            json += n;
                            progress.fetch_add(n as u64, Ordering::Relaxed);
                        }
                        Err(e) => {
                            errors.push(e.to_string());
                            if e.transient {
                                retry_tables.push(table);
                            } else {
                                lost_rows += n;
                            }
                        }
                    }
                }
            }
            if !retry_tables.is_empty() {
                keep_json = json_rows
                    .into_iter()
                    .filter(|r| retry_tables.contains(&r.table))
                    .collect();
            }
        }

        let (image, keep_image) = flush_section(
            &pool,
            image::COPY_SQL,
            image_rows,
            ImageWriter::build_copy_payload,
            usize::MAX, // always single-connection (huge rows)
            &mut errors,
            &mut lost_rows,
            &progress,
        )
        .await;

        let report = FlushReport {
            scalar,
            string,
            array: array_num + array_str,
            json,
            image,
        };

        let has_retry = !keep_scalar.is_empty()
            || !keep_string.is_empty()
            || !keep_array_num.is_empty()
            || !keep_array_str.is_empty()
            || !keep_json.is_empty()
            || !keep_image.is_empty();

        let retry = has_retry.then(|| {
            Box::new(FlushBundle {
                scalar_rows: keep_scalar,
                string_rows: keep_string,
                array_num_rows: keep_array_num,
                array_str_rows: keep_array_str,
                json_rows: keep_json,
                image_rows: keep_image,
                pool: pool.clone(),
            })
        });

        FlushOutcome {
            report,
            retry,
            lost_rows,
            errors,
        }
    }

    pub fn total_rows(&self) -> usize {
        self.scalar_rows.len()
            + self.string_rows.len()
            + self.array_num_rows.len()
            + self.array_str_rows.len()
            + self.json_rows.len()
            + self.image_rows.len()
    }
}

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
    pub scalar_backpressure: u64,
    pub string_backpressure: u64,
    pub array_backpressure: u64,
    pub json_backpressure: u64,
    pub image_backpressure: u64,
}

impl WriterStats {
    pub fn total_written(&self) -> u64 {
        self.scalar_written
            + self.string_written
            + self.array_written
            + self.json_written
            + self.image_written
    }

    pub fn total_backpressure(&self) -> u64 {
        self.scalar_backpressure
            + self.string_backpressure
            + self.array_backpressure
            + self.json_backpressure
            + self.image_backpressure
    }

    pub fn has_backpressure(&self) -> bool {
        self.total_backpressure() > 0
    }
}

impl fmt::Display for WriterStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "dispatched={} written={} buffered={} errors={} \
                    cache_hit={:.1}% backpressure={}",
            self.dispatched,
            self.total_written(),
            self.buffered,
            self.errors,
            self.cache_hit_ratio * 100.0,
            self.total_backpressure()
        )
    }
}

fn extract_string(nt: &NormativeType) -> String {
    match nt {
        NormativeType::NTScalar(s) => match &s.value {
            aura_core::pva::ScalarValue::String(v) => v.clone(),
            other => format!("{:?}", other),
        },
        _ => String::new(),
    }
}

fn extract_array_f64(nt: &NormativeType) -> Vec<f64> {
    match nt {
        NormativeType::NTScalarArray(a) => a.value.as_f64_vec().unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn extract_matrix(nt: &NormativeType) -> (Vec<f64>, Vec<i32>) {
    match nt {
        NormativeType::NTMatrix(m) => (m.value.clone(), m.dim.clone()),
        _ => (Vec::new(), Vec::new()),
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
        write!(
            f,
            "BatchWriter: {} dispatched, {} errors, {} buffered, {} PVs cached",
            self.total_dispatched,
            self.total_errors,
            self.total_buffered(),
            self.pv_cache.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_config_default() {
        let cfg = BatchWriterConfig::default();
        assert_eq!(cfg.scalar_batch_size, 500);
        assert_eq!(cfg.image_batch_size, 50);
    }

    #[test]
    fn test_new() {
        let w = BatchWriter::with_defaults();
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
        assert_eq!(BatchWriter::new(cfg).scalar_writer().batch_size(), 1000);
    }

    #[test]
    fn test_discard_all() {
        let mut w = BatchWriter::with_defaults();
        w.scalar.push(ScalarRow::new(
            Utc::now(),
            1,
            10.0,
            0,
            0,
            StoreReason::Initial,
        ));
        w.string
            .push(StringRow::new(Utc::now(), 1, "x".to_string(), 0, 0));
        assert!(w.has_pending());
        w.discard_all();
        assert!(!w.has_pending());
    }

    #[test]
    fn test_flush_report() {
        let r = FlushReport {
            scalar: 100,
            string: 5,
            array: 10,
            json: 3,
            image: 2,
        };
        assert_eq!(r.total(), 120);
        assert!(!r.is_empty());
        assert!(
            FlushReport {
                scalar: 0,
                string: 0,
                array: 0,
                json: 0,
                image: 0
            }
            .is_empty()
        );
    }

    #[test]
    fn test_writer_stats() {
        let stats = WriterStats {
            dispatched: 1000,
            errors: 0,
            buffered: 5,
            scalar_written: 800,
            string_written: 50,
            array_written: 30,
            json_written: 20,
            image_written: 10,
            image_bytes: 50000,
            cache_entries: 100,
            cache_hit_ratio: 0.999,
            scalar_backpressure: 0,
            string_backpressure: 0,
            array_backpressure: 0,
            json_backpressure: 0,
            image_backpressure: 0,
        };
        assert_eq!(stats.total_written(), 910);
        assert!(!stats.has_backpressure());
    }

    #[test]
    fn test_writer_accessors() {
        let w = BatchWriter::with_defaults();
        assert!(w.scalar_writer().is_empty());
        assert!(w.pv_cache().is_empty());
        assert!(w.copy_pool().is_empty());
    }

    // ── Extraction helpers ───────────────────────────────────────

    #[test]
    fn test_extract_string() {
        use aura_core::pva::*;
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::String("hello".to_string()),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None,
            control: None,
            value_alarm: None,
        });
        assert_eq!(extract_string(&nt), "hello");
    }

    #[test]
    fn test_extract_string_non_scalar() {
        use aura_core::pva::*;
        assert_eq!(
            extract_string(&NormativeType::NTTable(NTTable {
                labels: vec![],
                columns: vec![],
                alarm: Alarm::default(),
                timestamp: TimeStamp::new(1000, 0),
            })),
            ""
        );
    }

    #[test]
    fn test_extract_array() {
        use aura_core::pva::*;
        let nt = NormativeType::NTScalarArray(NTScalarArray {
            value: ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None,
            control: None,
            value_alarm: None,
        });
        assert_eq!(extract_array_f64(&nt), vec![1.0, 2.0, 3.0]);
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
        let (v, d) = extract_matrix(&nt);
        assert_eq!(v, vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(d, vec![2, 2]);
    }

    #[test]
    fn test_extract_image_move() {
        use aura_core::pva::*;
        let mut nt = NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![0xAB; 500]),
            codec: Codec {
                name: "blosc".to_string(),
                parameters: serde_json::Value::Null,
            },
            compressed_size: 500,
            uncompressed_size: 2000,
            dimension: vec![
                Dimension {
                    size: 320,
                    offset: 0,
                    full_size: 320,
                    binning: 1,
                    reverse: false,
                },
                Dimension {
                    size: 240,
                    offset: 0,
                    full_size: 240,
                    binning: 1,
                    reverse: false,
                },
            ],
            unique_id: 99,
            data_timestamp: None,
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            attribute: vec![],
        });
        let row = ImageRow::from_update(Utc::now(), 5, &mut nt, 0, 0);
        assert_eq!(row.data.len(), 500);
        assert_eq!(row.dimensions, vec![320, 240]);
        // Original data moved out
        if let NormativeType::NTNDArray(img) = &nt
            && let ArrayValue::UByteArray(v) = &img.value
        {
            assert!(v.is_empty());
        }
    }

    #[test]
    fn test_display() {
        let w = BatchWriter::with_defaults();
        assert!(w.to_string().contains("BatchWriter"));
        assert!(format!("{:?}", w).contains("dispatched"));
    }

    #[test]
    fn test_cache_initial_state() {
        let w = BatchWriter::with_defaults();
        assert!(w.pv_cache().is_empty());
        assert_eq!(w.pv_cache().hit_ratio(), 1.0); // no lookups = 100% hit
    }
}
