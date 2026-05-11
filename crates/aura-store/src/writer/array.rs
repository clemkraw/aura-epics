//! Batch writer for the `samples_array` table.
//!
//! Handles NTScalarArray (waveforms) and NTMatrix.
//! Stores `f64[]` with optional dimension metadata `int[]`.
//!
//! ## Write Strategy
//!
//! Two-tier insertion depending on batch size:
//!
//! - **Small batches (< 50 rows):** Transaction-based individual INSERTs.
//!
//! - **Large batches (≥ 50 rows):** PostgreSQL `COPY FROM STDIN` text
//!   protocol. Bypasses the SQL parser entirely — data is streamed
//!   directly to the storage engine.
//!
//! ## Backpressure
//!
//! The buffer has a hard memory limit (`max_buffer_bytes`, default 64 MB).
//! When exceeded, `push()` returns `PushResult::BackpressureExceeded` —
//! the caller must flush or drop data. This prevents OOM kills when
//! TimescaleDB is slow and waveform data accumulates.
//!
//! ## Performance
//!
//! - Buffer pre-allocated to batch_size: zero allocation on push
//! - `current_bytes` tracked incrementally: no re-scan on push
//! - `has_non_finite()` detects NaN/Infinity for downstream warnings
//! - `total_elements` counter: storage volume for capacity planning

use chrono::{DateTime, Utc};
use std::fmt;

use sqlx::PgPool;
use sqlx::postgres::PgPoolCopyExt;
use aura_core::error::{AuraError, AuraResult};

/// The INSERT query. `"values"` is quoted — PostgreSQL reserved keyword.
const INSERT_SQL: &str = r#"
    INSERT INTO samples_array (time, pv_id, "values", dim, severity, status)
    VALUES ($1, $2, $3, $4, $5, $6)
"#;

/// Batch size threshold for switching from INSERT to COPY protocol.
const COPY_THRESHOLD: usize = 50;

/// Default maximum buffer memory (64 MB).
const DEFAULT_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;


/// A single row for the `samples_array` table.
#[derive(Debug, Clone)]
pub struct ArrayRow {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub values: Vec<f64>,
    /// Matrix dimensions `[rows, cols]`. `None` for flat arrays.
    pub dim: Option<Vec<i32>>,
    pub severity: i16,
    pub status: i16,
}

impl ArrayRow {
    /// Create a flat array row (NTScalarArray).
    #[inline]
    pub fn array(
        time: DateTime<Utc>,
        pv_id: i32,
        values: Vec<f64>,
        severity: i16,
        status: i16,
    ) -> Self {
        Self { time, pv_id, values, dim: None, severity, status }
    }

    /// Create a matrix row (NTMatrix) with dimension metadata.
    #[inline]
    pub fn matrix(
        time: DateTime<Utc>,
        pv_id: i32,
        values: Vec<f64>,
        dim: Vec<i32>,
        severity: i16,
        status: i16,
    ) -> Self {
        Self { time, pv_id, values, dim: Some(dim), severity, status }
    }

    /// Number of elements.
    #[inline]
    pub fn len(&self) -> usize { self.values.len() }

    /// Whether the array is empty.
    #[inline]
    pub fn is_empty(&self) -> bool { self.values.is_empty() }

    /// Whether this row has matrix dimensions.
    #[inline]
    pub fn is_matrix(&self) -> bool { self.dim.is_some() }

    /// Whether any value is NaN or Infinity.
    #[inline]
    pub fn has_non_finite(&self) -> bool {
        self.values.iter().any(|v| !v.is_finite())
    }

    /// Approximate heap memory in bytes.
    #[inline]
    pub fn mem_size(&self) -> usize {
        let values_heap = self.values.len() * 8;
        let dim_heap = self.dim.as_ref().map_or(0, |d| 24 + d.len() * 4);
        40 + values_heap + dim_heap
    }
}

impl fmt::Display for ArrayRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref dim) = self.dim {
            let dims: Vec<String> = dim.iter().map(|d| d.to_string()).collect();
            write!(f, "pv_id={} matrix[{}] ({} elements)",
                   self.pv_id, dims.join("×"), self.values.len())
        } else {
            write!(f, "pv_id={} array[{}]", self.pv_id, self.values.len())
        }
    }
}

/// Result of pushing a row into the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    /// Row accepted, buffer not yet full.
    Accepted,
    /// Row accepted, buffer full — caller should flush.
    Full,
    /// Row **rejected** — memory limit exceeded, caller MUST flush first.
    BackpressureExceeded,
}

impl PushResult {
    /// Whether the buffer needs flushing.
    #[inline]
    pub fn needs_flush(&self) -> bool { !matches!(self, Self::Accepted) }

    /// Whether the row was accepted into the buffer.
    #[inline]
    pub fn is_accepted(&self) -> bool { !matches!(self, Self::BackpressureExceeded) }
}

impl fmt::Display for PushResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Accepted => "accepted",
            Self::Full => "full",
            Self::BackpressureExceeded => "backpressure",
        })
    }
}

/// Batch writer for array/matrix samples with backpressure and
/// automatic INSERT/COPY strategy selection.
pub struct ArrayWriter {
    batch_size: usize,
    max_buffer_bytes: usize,
    buffer: Vec<ArrayRow>,
    /// Incrementally tracked — avoids re-scanning on every push.
    current_bytes: usize,

    total_written: u64,
    total_flushes: u64,
    total_elements: u64,
    total_backpressure: u64,
    copy_flushes: u64,
    insert_flushes: u64,
}

impl ArrayWriter {
    /// Create with batch size and default memory limit (64 MB).
    pub fn new(batch_size: usize) -> Self {
        Self::with_limits(batch_size, DEFAULT_MAX_BUFFER_BYTES)
    }

    /// Create with explicit batch size and memory limit.
    pub fn with_limits(batch_size: usize, max_buffer_bytes: usize) -> Self {
        let batch_size = batch_size.max(1);
        let max_buffer_bytes = max_buffer_bytes.max(1024);
        Self {
            batch_size,
            max_buffer_bytes,
            buffer: Vec::with_capacity(batch_size),
            current_bytes: 0,
            total_written: 0,
            total_flushes: 0,
            total_elements: 0,
            total_backpressure: 0,
            copy_flushes: 0,
            insert_flushes: 0,
        }
    }

    /// Create with default settings (batch=200, limit=64 MB).
    pub fn with_defaults() -> Self { Self::new(200) }

    /// Push a row into the buffer with backpressure protection.
    #[inline]
    pub fn push(&mut self, row: ArrayRow) -> PushResult {
        let row_bytes = row.mem_size();

        // Backpressure: reject if adding this row exceeds the memory limit.
        // Exception: the first row is always accepted (empty buffer can't flush).
        if !self.buffer.is_empty()
            && self.current_bytes + row_bytes > self.max_buffer_bytes
        {
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

    /// Flush all buffered rows to the database.
    ///
    /// Selects INSERT (< 50 rows) or COPY (≥ 50 rows) automatically.
    pub async fn flush(&mut self, pool: &PgPool) -> AuraResult<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let count = self.buffer.len();

        if count >= COPY_THRESHOLD {
            self.flush_copy(pool).await?;
            self.copy_flushes += 1;
        } else {
            self.flush_insert(pool).await?;
            self.insert_flushes += 1;
        }

        let elements: u64 = self.buffer.iter()
            .map(|r| r.values.len() as u64)
            .sum();
        self.total_elements += elements;
        self.total_written += count as u64;
        self.total_flushes += 1;
        self.buffer.clear();
        self.current_bytes = 0;

        Ok(count)
    }

    /// Transaction INSERT — best for small batches.
    async fn flush_insert(&self, pool: &PgPool) -> AuraResult<()> {
        let mut tx = pool.begin().await
            .map_err(|e| AuraError::database(
                format!("samples_array tx begin failed: {e}")
            ))?;

        for row in &self.buffer {
            sqlx::query(INSERT_SQL)
                .bind(row.time)
                .bind(row.pv_id)
                .bind(&row.values)
                .bind(&row.dim)
                .bind(row.severity)
                .bind(row.status)
                .execute(&mut *tx)
                .await
                .map_err(|e| AuraError::database(
                    format!("samples_array insert failed (pv_id={}, len={}): {e}",
                            row.pv_id, row.values.len())
                ))?;
        }

        tx.commit().await
            .map_err(|e| AuraError::database(
                format!("samples_array tx commit failed ({} rows): {e}",
                        self.buffer.len())
            ))
    }

    /// COPY FROM STDIN — streams text data to PostgreSQL, bypassing SQL parsing.
    async fn flush_copy(&self, pool: &PgPool) -> AuraResult<()> {
        let mut payload = String::with_capacity(self.current_bytes);

        for row in &self.buffer {
            // time (ISO 8601)
            payload.push_str(&row.time.to_rfc3339());
            payload.push('\t');

            // pv_id
            payload.push_str(&row.pv_id.to_string());
            payload.push('\t');

            // values as PostgreSQL array literal: {1.0,2.0,NaN}
            payload.push('{');
            for (i, v) in row.values.iter().enumerate() {
                if i > 0 { payload.push(','); }
                if v.is_nan() {
                    payload.push_str("NaN");
                } else if v.is_infinite() {
                    payload.push_str(if *v > 0.0 { "Infinity" } else { "-Infinity" });
                } else {
                    payload.push_str(&v.to_string());
                }
            }
            payload.push('}');
            payload.push('\t');

            // dim: {2,3} or \N (NULL)
            match &row.dim {
                Some(dim) => {
                    payload.push('{');
                    for (i, d) in dim.iter().enumerate() {
                        if i > 0 { payload.push(','); }
                        payload.push_str(&d.to_string());
                    }
                    payload.push('}');
                }
                None => payload.push_str("\\N"),
            }
            payload.push('\t');

            // severity, status
            payload.push_str(&row.severity.to_string());
            payload.push('\t');
            payload.push_str(&row.status.to_string());
            payload.push('\n');
        }

        let copy_sql = "COPY samples_array (time, pv_id, \"values\", dim, severity, status) FROM STDIN";

        let mut copy_in = pool.copy_in_raw(copy_sql).await
            .map_err(|e| AuraError::database(
                format!("samples_array COPY begin failed: {e}")
            ))?;

        copy_in.send(payload.as_bytes()).await
            .map_err(|e| AuraError::database(
                format!("samples_array COPY send failed ({} rows, {} bytes): {e}",
                        self.buffer.len(), payload.len())
            ))?;

        copy_in.finish().await
            .map_err(|e| AuraError::database(
                format!("samples_array COPY finish failed: {e}")
            ))?;

        Ok(())
    }

    #[inline] pub fn buffered(&self) -> usize { self.buffer.len() }
    #[inline] pub fn is_empty(&self) -> bool { self.buffer.is_empty() }
    #[inline] pub fn is_full(&self) -> bool { self.buffer.len() >= self.batch_size }
    #[inline] pub fn batch_size(&self) -> usize { self.batch_size }
    #[inline] pub fn max_buffer_bytes(&self) -> usize { self.max_buffer_bytes }
    #[inline] pub fn buffered_bytes(&self) -> usize { self.current_bytes }
    #[inline] pub fn total_written(&self) -> u64 { self.total_written }
    #[inline] pub fn total_flushes(&self) -> u64 { self.total_flushes }
    #[inline] pub fn total_elements(&self) -> u64 { self.total_elements }
    #[inline] pub fn total_backpressure(&self) -> u64 { self.total_backpressure }
    #[inline] pub fn copy_flushes(&self) -> u64 { self.copy_flushes }
    #[inline] pub fn insert_flushes(&self) -> u64 { self.insert_flushes }

    /// Buffer memory utilization (0.0 to 1.0).
    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 { return 0.0; }
        self.current_bytes as f64 / self.max_buffer_bytes as f64
    }

    /// Average array length across all written rows.
    pub fn avg_array_len(&self) -> f64 {
        if self.total_written == 0 { return 0.0; }
        self.total_elements as f64 / self.total_written as f64
    }

    /// Average rows per flush.
    pub fn avg_batch_size(&self) -> f64 {
        if self.total_flushes == 0 { return 0.0; }
        self.total_written as f64 / self.total_flushes as f64
    }

    /// Clear the buffer without flushing.
    pub fn discard(&mut self) {
        self.buffer.clear();
        self.current_bytes = 0;
    }
}

impl fmt::Debug for ArrayWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayWriter")
            .field("buffered", &self.buffer.len())
            .field("batch_size", &self.batch_size)
            .field("bytes", &format!("{}/{}", self.current_bytes, self.max_buffer_bytes))
            .field("pressure", &format!("{:.1}%", self.memory_pressure() * 100.0))
            .field("total_written", &self.total_written)
            .field("total_elements", &self.total_elements)
            .field("copy_flushes", &self.copy_flushes)
            .field("insert_flushes", &self.insert_flushes)
            .field("backpressure_events", &self.total_backpressure)
            .finish()
    }
}

impl fmt::Display for ArrayWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArrayWriter: {}/{} buffered ({:.1} KB/{:.1} MB, {:.0}% pressure), \
             {} written ({} elems, avg {:.0}), {} flushes ({} COPY/{} INSERT), \
             {} backpressure",
            self.buffer.len(), self.batch_size,
            self.current_bytes as f64 / 1024.0,
            self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
            self.memory_pressure() * 100.0,
            self.total_written, self.total_elements, self.avg_array_len(),
            self.total_flushes, self.copy_flushes, self.insert_flushes,
            self.total_backpressure,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> { Utc::now() }
    fn arr(pv: i32, n: usize) -> ArrayRow { ArrayRow::array(now(), pv, vec![0.0; n], 0, 0) }
    fn mtx(pv: i32, r: i32, c: i32) -> ArrayRow {
        ArrayRow::matrix(now(), pv, vec![0.0; (r*c) as usize], vec![r, c], 0, 0)
    }

    // ═════════════════════════════════════════════════════════════════
    // ArrayRow
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_array_row_basic() {
        let row = ArrayRow::array(now(), 1, vec![1.0, 2.0, 3.0], 0, 0);
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
        assert!(!row.is_matrix());
        assert!(!row.has_non_finite());
    }

    #[test]
    fn test_matrix_row_basic() {
        let row = ArrayRow::matrix(now(), 2, vec![1.0; 4], vec![2, 2], 1, 1);
        assert_eq!(row.len(), 4);
        assert!(row.is_matrix());
        assert_eq!(row.dim, Some(vec![2, 2]));
    }

    #[test]
    fn test_empty_array() {
        let row = arr(1, 0);
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);
    }

    #[test]
    fn test_nan_detection() {
        assert!(ArrayRow::array(now(), 1, vec![1.0, f64::NAN], 0, 0).has_non_finite());
    }

    #[test]
    fn test_infinity_detection() {
        assert!(ArrayRow::array(now(), 1, vec![f64::INFINITY], 0, 0).has_non_finite());
        assert!(ArrayRow::array(now(), 1, vec![f64::NEG_INFINITY], 0, 0).has_non_finite());
    }

    #[test]
    fn test_clean_values() {
        assert!(!ArrayRow::array(now(), 1, vec![1.0, -1.0, 0.0, 1e300], 0, 0).has_non_finite());
    }

    #[test]
    fn test_mem_size_array() {
        assert_eq!(arr(1, 1024).mem_size(), 40 + 1024 * 8);
    }

    #[test]
    fn test_mem_size_matrix() {
        assert_eq!(mtx(1, 10, 10).mem_size(), 40 + 800 + 24 + 8);
    }

    #[test]
    fn test_mem_size_empty() {
        assert_eq!(arr(1, 0).mem_size(), 40);
    }

    #[test]
    fn test_row_clone() {
        let a = mtx(1, 2, 3);
        let b = a.clone();
        assert_eq!(a.values, b.values);
        assert_eq!(a.dim, b.dim);
    }

    #[test]
    fn test_row_display_array() {
        assert!(arr(42, 3).to_string().contains("array[3]"));
    }

    #[test]
    fn test_row_display_matrix() {
        let s = mtx(7, 2, 3).to_string();
        assert!(s.contains("matrix[2×3]"));
        assert!(s.contains("6 elements"));
    }

    #[test]
    fn test_row_debug() {
        assert!(format!("{:?}", arr(1, 1)).contains("ArrayRow"));
    }

    // ═════════════════════════════════════════════════════════════════
    // PushResult
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_push_result_accepted() {
        assert!(!PushResult::Accepted.needs_flush());
        assert!(PushResult::Accepted.is_accepted());
    }

    #[test]
    fn test_push_result_full() {
        assert!(PushResult::Full.needs_flush());
        assert!(PushResult::Full.is_accepted());
    }

    #[test]
    fn test_push_result_backpressure() {
        assert!(PushResult::BackpressureExceeded.needs_flush());
        assert!(!PushResult::BackpressureExceeded.is_accepted());
    }

    #[test]
    fn test_push_result_display() {
        assert_eq!(PushResult::Accepted.to_string(), "accepted");
        assert_eq!(PushResult::Full.to_string(), "full");
        assert_eq!(PushResult::BackpressureExceeded.to_string(), "backpressure");
    }

    #[test]
    fn test_push_result_eq() {
        assert_eq!(PushResult::Accepted, PushResult::Accepted);
        assert_ne!(PushResult::Accepted, PushResult::Full);
    }

    // ═════════════════════════════════════════════════════════════════
    // ArrayWriter — Construction
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_new_defaults() {
        let w = ArrayWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.batch_size(), 200);
        assert_eq!(w.max_buffer_bytes(), DEFAULT_MAX_BUFFER_BYTES);
        assert_eq!(w.buffered_bytes(), 0);
        assert_eq!(w.memory_pressure(), 0.0);
        assert_eq!(w.total_written(), 0);
        assert_eq!(w.total_flushes(), 0);
        assert_eq!(w.total_elements(), 0);
        assert_eq!(w.total_backpressure(), 0);
        assert_eq!(w.copy_flushes(), 0);
        assert_eq!(w.insert_flushes(), 0);
    }

    #[test]
    fn test_custom_limits() {
        let w = ArrayWriter::with_limits(50, 1024 * 1024);
        assert_eq!(w.batch_size(), 50);
        assert_eq!(w.max_buffer_bytes(), 1024 * 1024);
    }

    #[test]
    fn test_min_batch_size() {
        assert_eq!(ArrayWriter::new(0).batch_size(), 1);
    }

    #[test]
    fn test_min_buffer_bytes() {
        assert_eq!(ArrayWriter::with_limits(10, 0).max_buffer_bytes(), 1024);
    }

    // ═════════════════════════════════════════════════════════════════
    // ArrayWriter — Push
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_push_accepted() {
        let mut w = ArrayWriter::with_defaults();
        assert_eq!(w.push(arr(1, 10)), PushResult::Accepted);
        assert_eq!(w.buffered(), 1);
        assert!(w.buffered_bytes() > 0);
    }

    #[test]
    fn test_push_until_full() {
        let mut w = ArrayWriter::new(3);
        assert_eq!(w.push(arr(1, 1)), PushResult::Accepted);
        assert_eq!(w.push(arr(2, 1)), PushResult::Accepted);
        assert_eq!(w.push(arr(3, 1)), PushResult::Full);
        assert!(w.is_full());
    }

    #[test]
    fn test_push_mixed() {
        let mut w = ArrayWriter::new(10);
        w.push(arr(1, 100));
        w.push(mtx(2, 3, 4));
        assert_eq!(w.buffered(), 2);
    }

    // ── Backpressure ─────────────────────────────────────────────────

    #[test]
    fn test_backpressure_triggered() {
        // 1 KB limit. 100 f64s = 840 bytes.
        let mut w = ArrayWriter::with_limits(100, 1024);
        assert_eq!(w.push(arr(1, 100)), PushResult::Accepted); // 840 < 1024
        assert_eq!(w.push(arr(2, 100)), PushResult::BackpressureExceeded); // 1680 > 1024
        assert_eq!(w.buffered(), 1); // rejected row NOT added
        assert_eq!(w.total_backpressure(), 1);
    }

    #[test]
    fn test_backpressure_first_row_always_accepted() {
        let mut w = ArrayWriter::with_limits(100, 100);
        let r = w.push(arr(1, 1000)); // 8040 bytes >> 100 limit
        assert!(r.is_accepted()); // empty buffer always accepts
    }

    #[test]
    fn test_backpressure_clears_after_discard() {
        let mut w = ArrayWriter::with_limits(100, 1024);
        w.push(arr(1, 100));
        w.discard();
        assert_eq!(w.push(arr(2, 100)), PushResult::Accepted);
    }

    #[test]
    fn test_memory_pressure() {
        let mut w = ArrayWriter::with_limits(100, 10_000);
        w.push(arr(1, 100)); // 840 bytes → 8.4%
        let p = w.memory_pressure();
        assert!(p > 0.08 && p < 0.09, "got {:.1}%", p * 100.0);
    }

    // ── Incremental byte tracking ────────────────────────────────────

    #[test]
    fn test_bytes_tracked_incrementally() {
        let mut w = ArrayWriter::with_defaults();
        let s1 = arr(1, 100).mem_size();
        let s2 = arr(2, 200).mem_size();
        w.push(arr(1, 100));
        assert_eq!(w.buffered_bytes(), s1);
        w.push(arr(2, 200));
        assert_eq!(w.buffered_bytes(), s1 + s2);
    }

    #[test]
    fn test_bytes_reset_on_discard() {
        let mut w = ArrayWriter::with_defaults();
        w.push(arr(1, 100));
        w.discard();
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── Discard ──────────────────────────────────────────────────────

    #[test]
    fn test_discard() {
        let mut w = ArrayWriter::with_defaults();
        w.push(arr(1, 50));
        w.push(arr(2, 50));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── Statistics ───────────────────────────────────────────────────

    #[test]
    fn test_avg_array_len() {
        let mut w = ArrayWriter::with_defaults();
        w.total_written = 10;
        w.total_elements = 10_240;
        assert_eq!(w.avg_array_len(), 1024.0);
    }

    #[test]
    fn test_avg_array_len_empty() {
        assert_eq!(ArrayWriter::with_defaults().avg_array_len(), 0.0);
    }

    #[test]
    fn test_avg_batch_size() {
        let mut w = ArrayWriter::with_defaults();
        w.total_written = 800;
        w.total_flushes = 4;
        assert_eq!(w.avg_batch_size(), 200.0);
    }

    #[test]
    fn test_avg_batch_size_empty() {
        assert_eq!(ArrayWriter::with_defaults().avg_batch_size(), 0.0);
    }

    // ── SQL constants ────────────────────────────────────────────────

    #[test]
    fn test_insert_sql_quotes_values() {
        assert!(INSERT_SQL.contains(r#""values""#));
    }

    #[test]
    fn test_insert_sql_columns() {
        let u = INSERT_SQL.to_uppercase();
        for col in ["TIME", "PV_ID", "DIM", "SEVERITY", "STATUS"] {
            assert!(u.contains(col), "missing: {col}");
        }
    }

    #[test]
    fn test_copy_threshold_sane() {
        assert!(COPY_THRESHOLD > 0 && COPY_THRESHOLD <= 200);
    }

    // ── Large waveform ───────────────────────────────────────────────

    #[test]
    fn test_large_waveform_memory() {
        let mut w = ArrayWriter::new(10);
        for i in 0..10 {
            w.push(arr(i, 4096));
        }
        assert!(w.is_full());
        assert_eq!(w.buffered_bytes(), 10 * (40 + 4096 * 8));
    }

    // ── Display / Debug ──────────────────────────────────────────────

    #[test]
    fn test_display_empty() {
        let s = ArrayWriter::with_defaults().to_string();
        assert!(s.contains("0/200"));
        assert!(s.contains("0 backpressure"));
    }

    #[test]
    fn test_display_with_stats() {
        let mut w = ArrayWriter::with_defaults();
        w.total_written = 100;
        w.total_elements = 102_400;
        w.copy_flushes = 3;
        w.insert_flushes = 2;
        w.total_backpressure = 1;
        let s = w.to_string();
        assert!(s.contains("100 written"));
        assert!(s.contains("3 COPY"));
        assert!(s.contains("1 backpressure"));
    }

    #[test]
    fn test_debug() {
        let d = format!("{:?}", ArrayWriter::with_defaults());
        assert!(d.contains("pressure"));
        assert!(d.contains("copy_flushes"));
        assert!(d.contains("backpressure_events"));
    }
}