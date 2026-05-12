//! Batch writer for the `samples` table (scalar numeric data).
//!
//! This handles ~90% of all writes: NTScalar (numeric), NTEnum, NTAggregate.
//!
//! ## Write Strategy
//!
//! Two-tier insertion depending on batch size:
//!
//! - **Small batches (< 50 rows):** UNNEST batch INSERT — a single SQL
//!   statement with 6 parallel arrays. One network round-trip for N rows.
//!
//! - **Large batches (≥ 50 rows):** `COPY FROM STDIN` text protocol.
//!   Bypasses SQL parsing entirely — data streams directly to the storage engine.
//!
//! ## Backpressure
//!
//! Hard memory limit (`max_buffer_bytes`, default 64 MB) prevents OOM
//! when TimescaleDB is slow and scalar samples accumulate. `push()`
//! returns `PushResult` with backpressure signaling. At 26 bytes per
//! `ScalarRow`, 64 MB holds ~2.5 million rows — a comfortable buffer
//! for sustained 500k samples/s with occasional DB hiccups.
//!
//! ## Row Layout
//!
//! ```text
//! time(8) + pv_id(4) + value(8) + severity(2) + status(2) + reason(2) = 26 bytes
//! ```
//!
//! After TimescaleDB compression: ~4-6 bytes per row (gorilla + dictionary).

use chrono::{DateTime, Utc};
use std::fmt;

use sqlx::PgPool;
use sqlx::postgres::PgPoolCopyExt;
use aura_core::error::{AuraError, AuraResult};
use aura_core::sample::StoreReason;

/// UNNEST batch INSERT query (parameterized, no string interpolation).
const UNNEST_SQL: &str = r#"
    INSERT INTO samples (time, pv_id, value, severity, status, reason)
    SELECT * FROM UNNEST($1::timestamptz[], $2::int[], $3::float8[], $4::smallint[], $5::smallint[], $6::smallint[])
"#;

/// Batch size threshold for switching from UNNEST to COPY.
const COPY_THRESHOLD: usize = 50;

/// Default maximum buffer memory (64 MB).
const DEFAULT_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Approximate memory per ScalarRow (stack + heap overhead).
const ROW_MEM_SIZE: usize = 48; // 26 bytes data + alignment + Vec slot overhead

/// A single row for the `samples` table.
///
/// Fixed-size, no heap allocations. 26 bytes of useful data per row.
#[derive(Debug, Clone, Copy)]
pub struct ScalarRow {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub value: f64,
    pub severity: i16,
    pub status: i16,
    pub reason: i16,
}

impl ScalarRow {
    /// Create a row from resolved components.
    #[inline]
    pub fn new(
        time: DateTime<Utc>,
        pv_id: i32,
        value: f64,
        severity: i16,
        status: i16,
        reason: StoreReason,
    ) -> Self {
        Self { time, pv_id, value, severity, status, reason: reason as i16 }
    }

    /// Whether the value is NaN or Infinity.
    ///
    /// PostgreSQL accepts these, but they can poison downstream
    /// aggregates (AVG of NaN = NaN). The writer logs a warning
    /// but still archives — the IOC sent it, we store it.
    #[inline]
    pub fn has_non_finite(&self) -> bool {
        !self.value.is_finite()
    }

    /// Approximate memory size (constant — no heap allocations).
    #[inline]
    pub const fn mem_size() -> usize {
        ROW_MEM_SIZE
    }
}

impl fmt::Display for ScalarRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pv_id={} value={} sev={} reason={}",
               self.pv_id, self.value, self.severity, self.reason)
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
    #[inline] pub fn needs_flush(&self) -> bool { !matches!(self, Self::Accepted) }
    #[inline] pub fn is_accepted(&self) -> bool { !matches!(self, Self::BackpressureExceeded) }
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

/// Batch writer for scalar numeric samples with backpressure
/// and automatic UNNEST/COPY strategy selection.
///
/// This is the highest-throughput writer — handles ~90% of all traffic.
pub struct ScalarWriter {
    batch_size: usize,
    max_buffer_bytes: usize,
    buffer: Vec<ScalarRow>,

    total_written: u64,
    total_flushes: u64,
    total_backpressure: u64,
    total_non_finite: u64,
    copy_flushes: u64,
    unnest_flushes: u64,
}

impl ScalarWriter {
    /// Create with batch size and default memory limit (64 MB).
    pub fn new(batch_size: usize) -> Self {
        Self::with_limits(batch_size, DEFAULT_MAX_BUFFER_BYTES)
    }

    /// Create with explicit batch size and memory limit.
    pub fn with_limits(batch_size: usize, max_buffer_bytes: usize) -> Self {
        let batch_size = batch_size.max(1);
        let max_buffer_bytes = max_buffer_bytes.max(ROW_MEM_SIZE);
        Self {
            batch_size,
            max_buffer_bytes,
            buffer: Vec::with_capacity(batch_size),
            total_written: 0,
            total_flushes: 0,
            total_backpressure: 0,
            total_non_finite: 0,
            copy_flushes: 0,
            unnest_flushes: 0,
        }
    }

    /// Create with default settings (batch=500, limit=64 MB).
    pub fn with_defaults() -> Self { Self::new(500) }

    /// Push a single row with backpressure protection.
    #[inline]
    pub fn push(&mut self, row: ScalarRow) -> PushResult {
        if !self.buffer.is_empty()
            && self.buffered_bytes() + ROW_MEM_SIZE > self.max_buffer_bytes
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

    /// Push multiple rows at once.
    ///
    /// Returns the number of rows accepted. Stops at backpressure limit.
    pub fn push_batch(&mut self, rows: impl IntoIterator<Item = ScalarRow>) -> usize {
        let mut accepted = 0;
        for row in rows {
            match self.push(row) {
                PushResult::Accepted | PushResult::Full => { accepted += 1; }
                PushResult::BackpressureExceeded => break,
            }
        }
        accepted
    }

    /// Flush all buffered rows to the database.
    ///
    /// Selects UNNEST (< 50 rows) or COPY (≥ 50 rows) automatically.
    pub async fn flush(&mut self, pool: &PgPool) -> AuraResult<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let count = self.buffer.len();

        if count >= COPY_THRESHOLD {
            self.flush_copy(pool).await?;
            self.copy_flushes += 1;
        } else {
            self.flush_unnest(pool).await?;
            self.unnest_flushes += 1;
        }

        self.total_written += count as u64;
        self.total_flushes += 1;
        self.buffer.clear();

        Ok(count)
    }

    /// UNNEST batch INSERT — single SQL statement with 6 parallel arrays.
    async fn flush_unnest(&self, pool: &PgPool) -> AuraResult<()> {
        let count = self.buffer.len();
        let mut times = Vec::with_capacity(count);
        let mut pv_ids = Vec::with_capacity(count);
        let mut values = Vec::with_capacity(count);
        let mut severities = Vec::with_capacity(count);
        let mut statuses = Vec::with_capacity(count);
        let mut reasons = Vec::with_capacity(count);

        for row in &self.buffer {
            times.push(row.time);
            pv_ids.push(row.pv_id);
            values.push(row.value);
            severities.push(row.severity);
            statuses.push(row.status);
            reasons.push(row.reason);
        }

        sqlx::query(UNNEST_SQL)
            .bind(&times)
            .bind(&pv_ids)
            .bind(&values)
            .bind(&severities)
            .bind(&statuses)
            .bind(&reasons)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(
                format!("samples UNNEST failed ({count} rows): {e}")
            ))?;

        Ok(())
    }

    /// COPY FROM STDIN — streams text data directly to PostgreSQL.
    /// Bypasses SQL parsing. 3-10x faster for large batches.
    async fn flush_copy(&self, pool: &PgPool) -> AuraResult<()> {
        // Pre-allocate: ~80 bytes per row (timestamp + fields + separators).
        let mut payload = String::with_capacity(self.buffer.len() * 80);

        for row in &self.buffer {
            // time (ISO 8601)
            payload.push_str(&row.time.to_rfc3339());
            payload.push('\t');

            // pv_id
            payload.push_str(&row.pv_id.to_string());
            payload.push('\t');

            // value — handle NaN/Infinity for PostgreSQL
            if row.value.is_nan() {
                payload.push_str("NaN");
            } else if row.value.is_infinite() {
                payload.push_str(if row.value > 0.0 { "Infinity" } else { "-Infinity" });
            } else {
                payload.push_str(&row.value.to_string());
            }
            payload.push('\t');

            // severity, status, reason
            payload.push_str(&row.severity.to_string());
            payload.push('\t');
            payload.push_str(&row.status.to_string());
            payload.push('\t');
            payload.push_str(&row.reason.to_string());
            payload.push('\n');
        }

        let copy_sql = "COPY samples (time, pv_id, value, severity, status, reason) FROM STDIN";

        let mut copy_in = pool.copy_in_raw(copy_sql).await
            .map_err(|e| AuraError::database(
                format!("samples COPY begin failed: {e}")
            ))?;

        copy_in.send(payload.as_bytes()).await
            .map_err(|e| AuraError::database(
                format!("samples COPY send failed ({} rows, {} bytes): {e}",
                        self.buffer.len(), payload.len())
            ))?;

        copy_in.finish().await
            .map_err(|e| AuraError::database(
                format!("samples COPY finish failed: {e}")
            ))?;

        Ok(())
    }

    #[inline] pub fn buffered(&self) -> usize { self.buffer.len() }
    #[inline] pub fn is_empty(&self) -> bool { self.buffer.is_empty() }
    #[inline] pub fn is_full(&self) -> bool { self.buffer.len() >= self.batch_size }
    #[inline] pub fn batch_size(&self) -> usize { self.batch_size }
    #[inline] pub fn max_buffer_bytes(&self) -> usize { self.max_buffer_bytes }
    #[inline] pub fn total_written(&self) -> u64 { self.total_written }
    #[inline] pub fn total_flushes(&self) -> u64 { self.total_flushes }
    #[inline] pub fn total_backpressure(&self) -> u64 { self.total_backpressure }
    #[inline] pub fn total_non_finite(&self) -> u64 { self.total_non_finite }
    #[inline] pub fn copy_flushes(&self) -> u64 { self.copy_flushes }
    #[inline] pub fn unnest_flushes(&self) -> u64 { self.unnest_flushes }

    /// Approximate bytes currently buffered.
    #[inline]
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len() * ROW_MEM_SIZE
    }

    /// Buffer memory utilization (0.0 to 1.0).
    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 { return 0.0; }
        self.buffered_bytes() as f64 / self.max_buffer_bytes as f64
    }

    /// Maximum rows the buffer can hold before backpressure.
    pub fn max_rows(&self) -> usize {
        self.max_buffer_bytes / ROW_MEM_SIZE
    }

    /// Average rows per flush.
    pub fn avg_batch_size(&self) -> f64 {
        if self.total_flushes == 0 { return 0.0; }
        self.total_written as f64 / self.total_flushes as f64
    }

    /// Clear the buffer without flushing.
    pub fn discard(&mut self) {
        self.buffer.clear();
    }
}

impl fmt::Debug for ScalarWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarWriter")
            .field("buffered", &self.buffer.len())
            .field("batch_size", &self.batch_size)
            .field("bytes", &format!("{}/{}", self.buffered_bytes(), self.max_buffer_bytes))
            .field("pressure", &format!("{:.1}%", self.memory_pressure() * 100.0))
            .field("total_written", &self.total_written)
            .field("copy_flushes", &self.copy_flushes)
            .field("unnest_flushes", &self.unnest_flushes)
            .field("non_finite", &self.total_non_finite)
            .field("backpressure_events", &self.total_backpressure)
            .finish()
    }
}

impl fmt::Display for ScalarWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ScalarWriter: {}/{} buffered ({:.1} KB/{:.1} MB, {:.0}% pressure), \
             {} written (avg {:.0}), {} flushes ({} COPY/{} UNNEST), \
             {} non-finite, {} backpressure",
            self.buffer.len(), self.batch_size,
            self.buffered_bytes() as f64 / 1024.0,
            self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
            self.memory_pressure() * 100.0,
            self.total_written, self.avg_batch_size(),
            self.total_flushes, self.copy_flushes, self.unnest_flushes,
            self.total_non_finite, self.total_backpressure,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> { Utc::now() }
    fn row(pv: i32, val: f64) -> ScalarRow {
        ScalarRow::new(now(), pv, val, 0, 0, StoreReason::EpsilonExceeded)
    }
    fn row_reason(pv: i32, val: f64, sev: i16, reason: StoreReason) -> ScalarRow {
        ScalarRow::new(now(), pv, val, sev, 0, reason)
    }

    // ═════════════════════════════════════════════════════════════════
    // ScalarRow
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_row_new() {
        let r = row(42, 4.217);
        assert_eq!(r.pv_id, 42);
        assert_eq!(r.value, 4.217);
        assert_eq!(r.severity, 0);
        assert_eq!(r.status, 0);
        assert_eq!(r.reason, StoreReason::EpsilonExceeded as i16);
        assert!(!r.has_non_finite());
    }

    #[test]
    fn test_row_reason_mapping() {
        for reason in StoreReason::ALL {
            let r = row_reason(1, 0.0, 0, reason);
            assert_eq!(r.reason, reason as i16);
        }
    }

    #[test]
    fn test_row_severity() {
        let r = row_reason(1, 0.0, 2, StoreReason::AlarmChange);
        assert_eq!(r.severity, 2);
    }

    #[test]
    fn test_row_nan() {
        let r = row(1, f64::NAN);
        assert!(r.has_non_finite());
    }

    #[test]
    fn test_row_infinity() {
        assert!(row(1, f64::INFINITY).has_non_finite());
        assert!(row(1, f64::NEG_INFINITY).has_non_finite());
    }

    #[test]
    fn test_row_finite() {
        assert!(!row(1, 0.0).has_non_finite());
        assert!(!row(1, -1e300).has_non_finite());
        assert!(!row(1, 1e-300).has_non_finite());
    }

    #[test]
    fn test_row_mem_size() {
        assert_eq!(ScalarRow::mem_size(), ROW_MEM_SIZE);
        assert!(ROW_MEM_SIZE >= 26); // at least the data size
    }

    #[test]
    fn test_row_copy() {
        let a = row(1, 10.0);
        let b = a; // Copy (not Clone — ScalarRow is Copy)
        assert_eq!(a.pv_id, b.pv_id);
        assert_eq!(a.value, b.value);
    }

    #[test]
    fn test_row_display() {
        let r = row(42, 4.217);
        let s = r.to_string();
        assert!(s.contains("pv_id=42"));
        assert!(s.contains("4.217"));
    }

    #[test]
    fn test_row_debug() {
        assert!(format!("{:?}", row(1, 0.0)).contains("ScalarRow"));
    }

    // ═════════════════════════════════════════════════════════════════
    // PushResult
    // ═════════════════════════════════════════════════════════════════

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
    }

    #[test]
    fn test_push_result_backpressure() {
        assert!(PushResult::BackpressureExceeded.needs_flush());
        assert!(!PushResult::BackpressureExceeded.is_accepted());
    }

    #[test]
    fn test_push_result_eq() {
        assert_eq!(PushResult::Full, PushResult::Full);
        assert_ne!(PushResult::Full, PushResult::Accepted);
    }

    // ═════════════════════════════════════════════════════════════════
    // ScalarWriter — Construction
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_new_defaults() {
        let w = ScalarWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.buffered(), 0);
        assert_eq!(w.batch_size(), 500);
        assert_eq!(w.max_buffer_bytes(), DEFAULT_MAX_BUFFER_BYTES);
        assert_eq!(w.buffered_bytes(), 0);
        assert_eq!(w.memory_pressure(), 0.0);
        assert_eq!(w.total_written(), 0);
        assert_eq!(w.total_flushes(), 0);
        assert_eq!(w.total_backpressure(), 0);
        assert_eq!(w.total_non_finite(), 0);
        assert_eq!(w.copy_flushes(), 0);
        assert_eq!(w.unnest_flushes(), 0);
        assert_eq!(w.avg_batch_size(), 0.0);
    }

    #[test]
    fn test_custom_limits() {
        let w = ScalarWriter::with_limits(100, 1024 * 1024);
        assert_eq!(w.batch_size(), 100);
        assert_eq!(w.max_buffer_bytes(), 1024 * 1024);
    }

    #[test]
    fn test_min_batch_size() {
        assert_eq!(ScalarWriter::new(0).batch_size(), 1);
    }

    #[test]
    fn test_min_buffer_bytes() {
        assert_eq!(ScalarWriter::with_limits(10, 0).max_buffer_bytes(), ROW_MEM_SIZE);
    }

    #[test]
    fn test_max_rows() {
        let w = ScalarWriter::with_limits(500, ROW_MEM_SIZE * 1000);
        assert_eq!(w.max_rows(), 1000);
    }

    // ═════════════════════════════════════════════════════════════════
    // ScalarWriter — Push
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_push_accepted() {
        let mut w = ScalarWriter::with_defaults();
        assert_eq!(w.push(row(1, 10.0)), PushResult::Accepted);
        assert_eq!(w.buffered(), 1);
        assert_eq!(w.buffered_bytes(), ROW_MEM_SIZE);
    }

    #[test]
    fn test_push_until_full() {
        let mut w = ScalarWriter::new(3);
        assert_eq!(w.push(row(1, 1.0)), PushResult::Accepted);
        assert_eq!(w.push(row(2, 2.0)), PushResult::Accepted);
        assert_eq!(w.push(row(3, 3.0)), PushResult::Full);
        assert!(w.is_full());
    }

    #[test]
    fn test_push_tracks_non_finite() {
        let mut w = ScalarWriter::with_defaults();
        w.push(row(1, 1.0));                // finite
        w.push(row(2, f64::NAN));           // non-finite
        w.push(row(3, f64::INFINITY));      // non-finite
        w.push(row(4, f64::NEG_INFINITY));  // non-finite
        w.push(row(5, 5.0));                // finite
        assert_eq!(w.total_non_finite(), 3);
        assert_eq!(w.buffered(), 5); // all accepted
    }

    // ── push_batch ───────────────────────────────────────────────────

    #[test]
    fn test_push_batch() {
        let mut w = ScalarWriter::new(10);
        let rows = (0..5).map(|i| row(i, i as f64));
        let accepted = w.push_batch(rows);
        assert_eq!(accepted, 5);
        assert_eq!(w.buffered(), 5);
    }

    #[test]
    fn test_push_batch_stops_at_backpressure() {
        // Max ~2 rows.
        let mut w = ScalarWriter::with_limits(100, ROW_MEM_SIZE * 2 + 1);
        let rows = (0..10).map(|i| row(i, i as f64));
        let accepted = w.push_batch(rows);
        assert_eq!(accepted, 2); // third row triggers backpressure
        assert_eq!(w.buffered(), 2);
    }

    #[test]
    fn test_push_batch_empty() {
        let mut w = ScalarWriter::with_defaults();
        let accepted = w.push_batch(std::iter::empty());
        assert_eq!(accepted, 0);
    }

    // ── Backpressure ─────────────────────────────────────────────────

    #[test]
    fn test_backpressure_triggered() {
        // Max = 2 rows worth of memory.
        let mut w = ScalarWriter::with_limits(100, ROW_MEM_SIZE * 2);
        assert_eq!(w.push(row(1, 1.0)), PushResult::Accepted);
        assert_eq!(w.push(row(2, 2.0)), PushResult::Accepted);
        assert_eq!(w.push(row(3, 3.0)), PushResult::BackpressureExceeded);
        assert_eq!(w.buffered(), 2); // rejected
        assert_eq!(w.total_backpressure(), 1);
    }

    #[test]
    fn test_backpressure_first_row_always_accepted() {
        let mut w = ScalarWriter::with_limits(100, 1); // absurdly small
        let r = w.push(row(1, 1.0));
        assert!(r.is_accepted()); // empty buffer always accepts
    }

    #[test]
    fn test_backpressure_clears_after_discard() {
        let mut w = ScalarWriter::with_limits(100, ROW_MEM_SIZE * 2);
        w.push(row(1, 1.0));
        w.push(row(2, 2.0));
        w.discard();
        assert_eq!(w.push(row(3, 3.0)), PushResult::Accepted);
    }

    #[test]
    fn test_memory_pressure() {
        let mut w = ScalarWriter::with_limits(100, ROW_MEM_SIZE * 10);
        w.push(row(1, 1.0)); // 1/10 = 10%
        let p = w.memory_pressure();
        assert!((p - 0.1).abs() < 0.01, "got {:.1}%", p * 100.0);
    }

    // ── Buffered bytes ───────────────────────────────────────────────

    #[test]
    fn test_buffered_bytes() {
        let mut w = ScalarWriter::with_defaults();
        w.push(row(1, 1.0));
        w.push(row(2, 2.0));
        assert_eq!(w.buffered_bytes(), 2 * ROW_MEM_SIZE);
    }

    #[test]
    fn test_buffered_bytes_after_discard() {
        let mut w = ScalarWriter::with_defaults();
        w.push(row(1, 1.0));
        w.discard();
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── Discard ──────────────────────────────────────────────────────

    #[test]
    fn test_discard() {
        let mut w = ScalarWriter::with_defaults();
        w.push(row(1, 1.0));
        w.push(row(2, 2.0));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
        assert_eq!(w.total_written(), 0); // counters unchanged
    }

    // ── Statistics ───────────────────────────────────────────────────

    #[test]
    fn test_avg_batch_size() {
        let mut w = ScalarWriter::with_defaults();
        w.total_written = 2000;
        w.total_flushes = 4;
        assert_eq!(w.avg_batch_size(), 500.0);
    }

    #[test]
    fn test_avg_batch_size_empty() {
        assert_eq!(ScalarWriter::with_defaults().avg_batch_size(), 0.0);
    }

    // ── SQL constants ────────────────────────────────────────────────

    #[test]
    fn test_unnest_sql_columns() {
        let u = UNNEST_SQL.to_uppercase();
        for col in ["TIME", "PV_ID", "VALUE", "SEVERITY", "STATUS", "REASON"] {
            assert!(u.contains(col), "missing: {col}");
        }
    }

    #[test]
    fn test_unnest_sql_uses_unnest() {
        assert!(UNNEST_SQL.to_uppercase().contains("UNNEST"));
    }

    #[test]
    fn test_copy_threshold_sane() {
        assert!(COPY_THRESHOLD > 0 && COPY_THRESHOLD <= 500);
    }

    // ── Large batch simulation ───────────────────────────────────────

    #[test]
    fn test_large_batch() {
        let mut w = ScalarWriter::new(500);
        let rows = (0..500).map(|i| row(i % 100, i as f64));
        let accepted = w.push_batch(rows);
        assert_eq!(accepted, 500);
        assert!(w.is_full());
        assert_eq!(w.buffered_bytes(), 500 * ROW_MEM_SIZE);
    }

    // ── Display / Debug ──────────────────────────────────────────────

    #[test]
    fn test_display_empty() {
        let s = ScalarWriter::with_defaults().to_string();
        assert!(s.contains("0/500"));
        assert!(s.contains("0 backpressure"));
        assert!(s.contains("0 non-finite"));
    }

    #[test]
    fn test_display_with_stats() {
        let mut w = ScalarWriter::with_defaults();
        w.total_written = 10_000;
        w.total_flushes = 20;
        w.copy_flushes = 15;
        w.unnest_flushes = 5;
        w.total_non_finite = 3;
        w.total_backpressure = 1;
        let s = w.to_string();
        assert!(s.contains("10000 written"));
        assert!(s.contains("15 COPY"));
        assert!(s.contains("5 UNNEST"));
        assert!(s.contains("3 non-finite"));
        assert!(s.contains("1 backpressure"));
    }

    #[test]
    fn test_debug() {
        let d = format!("{:?}", ScalarWriter::with_defaults());
        assert!(d.contains("ScalarWriter"));
        assert!(d.contains("pressure"));
        assert!(d.contains("non_finite"));
        assert!(d.contains("backpressure_events"));
    }
}