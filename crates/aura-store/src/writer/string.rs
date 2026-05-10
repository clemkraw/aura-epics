//! Batch writer for the `samples_string` table.
//!
//! Handles NTScalar with String values. Separated from numeric scalars
//! because TEXT columns have different compression characteristics
//! (LZ4 vs gorilla) and shouldn't pollute the numeric table's ratio.
//!
//! ## Write Strategy
//!
//! Two-tier insertion depending on batch size:
//!
//! - **Small batches (< 50 rows):** UNNEST batch INSERT — a single SQL
//!   statement with 5 parallel arrays. One round-trip for N rows.
//!
//! - **Large batches (≥ 50 rows):** `COPY FROM STDIN` text protocol.
//!   Bypasses SQL parsing. 3-10x faster on large batches.
//!   Tab and newline characters in string values are escaped.
//!
//! ## Backpressure
//!
//! Hard memory limit (`max_buffer_bytes`, default 32 MB) prevents OOM
//! when string PVs produce large payloads (e.g., JSON-like strings,
//! long status messages). `push()` returns `PushResult` with
//! backpressure signaling.
//!
//! ## Performance
//!
//! - Buffer pre-allocated to batch_size
//! - `current_bytes` tracked incrementally (no re-scan)
//! - `total_value_bytes` tracks total string payload for storage metrics

use chrono::{DateTime, Utc};
use std::fmt;

use sqlx::PgPool;
use sqlx::postgres::PgPoolCopyExt;
use aura_core::error::{AuraError, AuraResult};

/// UNNEST batch INSERT query.
const INSERT_SQL: &str = r#"
    INSERT INTO samples_string (time, pv_id, value, severity, status)
    SELECT * FROM UNNEST($1::timestamptz[], $2::int[], $3::text[], $4::smallint[], $5::smallint[])
"#;

/// Batch size threshold for switching from UNNEST to COPY protocol.
const COPY_THRESHOLD: usize = 50;

/// Default maximum buffer memory (32 MB — strings can be large).
const DEFAULT_MAX_BUFFER_BYTES: usize = 32 * 1024 * 1024;

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
    pub fn new(
        time: DateTime<Utc>, pv_id: i32, value: String,
        severity: i16, status: i16,
    ) -> Self {
        Self { time, pv_id, value, severity, status }
    }

    /// Length of the string value in bytes.
    #[inline]
    pub fn value_len(&self) -> usize { self.value.len() }

    /// Whether the string value is empty.
    #[inline]
    pub fn is_value_empty(&self) -> bool { self.value.is_empty() }

    /// Approximate heap memory used by this row in bytes.
    #[inline]
    pub fn mem_size(&self) -> usize { 40 + self.value.len() }
}

impl fmt::Display for StringRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.value.len() <= 32 {
            write!(f, "pv_id={} \"{}\"", self.pv_id, self.value)
        } else {
            write!(f, "pv_id={} \"{}...\" ({} bytes)",
                   self.pv_id, &self.value[..32], self.value.len())
        }
    }
}

/// Result of pushing a row into the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    Accepted,
    Full,
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

/// Batch writer for string scalar samples with backpressure
/// and automatic UNNEST/COPY strategy selection.
pub struct StringWriter {
    batch_size: usize,
    max_buffer_bytes: usize,
    buffer: Vec<StringRow>,
    current_bytes: usize,

    total_written: u64,
    total_flushes: u64,
    total_value_bytes: u64,
    total_backpressure: u64,
    copy_flushes: u64,
    insert_flushes: u64,
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
            total_written: 0,
            total_flushes: 0,
            total_value_bytes: 0,
            total_backpressure: 0,
            copy_flushes: 0,
            insert_flushes: 0,
        }
    }

    pub fn with_defaults() -> Self { Self::new(500) }

    #[inline]
    pub fn push(&mut self, row: StringRow) -> PushResult {
        let row_bytes = row.mem_size();

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

    pub async fn flush(&mut self, pool: &PgPool) -> AuraResult<usize> {
        if self.buffer.is_empty() { return Ok(0); }
        let count = self.buffer.len();

        if count >= COPY_THRESHOLD {
            self.flush_copy(pool).await?;
            self.copy_flushes += 1;
        } else {
            self.flush_unnest(pool).await?;
            self.insert_flushes += 1;
        }

        let value_bytes: u64 = self.buffer.iter()
            .map(|r| r.value.len() as u64).sum();
        self.total_value_bytes += value_bytes;
        self.total_written += count as u64;
        self.total_flushes += 1;
        self.buffer.clear();
        self.current_bytes = 0;

        Ok(count)
    }

    async fn flush_unnest(&self, pool: &PgPool) -> AuraResult<()> {
        let count = self.buffer.len();
        let mut times = Vec::with_capacity(count);
        let mut pv_ids = Vec::with_capacity(count);
        let mut values: Vec<&str> = Vec::with_capacity(count);
        let mut severities = Vec::with_capacity(count);
        let mut statuses = Vec::with_capacity(count);

        for row in &self.buffer {
            times.push(row.time);
            pv_ids.push(row.pv_id);
            values.push(&row.value);
            severities.push(row.severity);
            statuses.push(row.status);
        }

        sqlx::query(INSERT_SQL)
            .bind(&times).bind(&pv_ids).bind(&values)
            .bind(&severities).bind(&statuses)
            .execute(pool).await
            .map_err(|e| AuraError::database(
                format!("samples_string UNNEST failed ({count} rows): {e}")
            ))?;

        Ok(())
    }

    async fn flush_copy(&self, pool: &PgPool) -> AuraResult<()> {
        let mut payload = String::with_capacity(self.current_bytes);

        for row in &self.buffer {
            payload.push_str(&row.time.to_rfc3339());
            payload.push('\t');
            payload.push_str(&row.pv_id.to_string());
            payload.push('\t');
            escape_copy_text(&row.value, &mut payload);
            payload.push('\t');
            payload.push_str(&row.severity.to_string());
            payload.push('\t');
            payload.push_str(&row.status.to_string());
            payload.push('\n');
        }

        let copy_sql = "COPY samples_string (time, pv_id, value, severity, status) FROM STDIN";
        let mut copy_in = pool.copy_in_raw(copy_sql).await
            .map_err(|e| AuraError::database(format!("samples_string COPY begin failed: {e}")))?;
        copy_in.send(payload.as_bytes()).await
            .map_err(|e| AuraError::database(
                format!("samples_string COPY send failed ({} rows): {e}", self.buffer.len())
            ))?;
        copy_in.finish().await
            .map_err(|e| AuraError::database(format!("samples_string COPY finish failed: {e}")))?;

        Ok(())
    }

    // ── Accessors ────────────────────────────────────────────────────

    #[inline] pub fn buffered(&self) -> usize { self.buffer.len() }
    #[inline] pub fn is_empty(&self) -> bool { self.buffer.is_empty() }
    #[inline] pub fn is_full(&self) -> bool { self.buffer.len() >= self.batch_size }
    #[inline] pub fn batch_size(&self) -> usize { self.batch_size }
    #[inline] pub fn max_buffer_bytes(&self) -> usize { self.max_buffer_bytes }
    #[inline] pub fn buffered_bytes(&self) -> usize { self.current_bytes }
    #[inline] pub fn total_written(&self) -> u64 { self.total_written }
    #[inline] pub fn total_flushes(&self) -> u64 { self.total_flushes }
    #[inline] pub fn total_value_bytes(&self) -> u64 { self.total_value_bytes }
    #[inline] pub fn total_backpressure(&self) -> u64 { self.total_backpressure }
    #[inline] pub fn copy_flushes(&self) -> u64 { self.copy_flushes }
    #[inline] pub fn insert_flushes(&self) -> u64 { self.insert_flushes }

    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 { return 0.0; }
        self.current_bytes as f64 / self.max_buffer_bytes as f64
    }

    pub fn avg_value_len(&self) -> f64 {
        if self.total_written == 0 { return 0.0; }
        self.total_value_bytes as f64 / self.total_written as f64
    }

    pub fn avg_batch_size(&self) -> f64 {
        if self.total_flushes == 0 { return 0.0; }
        self.total_written as f64 / self.total_flushes as f64
    }

    pub fn discard(&mut self) {
        self.buffer.clear();
        self.current_bytes = 0;
    }
}

/// Escape a string for PostgreSQL COPY text format.
///
/// Must escape: `\` → `\\`, `\t` → `\t`, `\n` → `\n`, `\r` → `\r`
fn escape_copy_text(value: &str, out: &mut String) {
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
}

impl fmt::Debug for StringWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StringWriter")
            .field("buffered", &self.buffer.len())
            .field("batch_size", &self.batch_size)
            .field("bytes", &format!("{}/{}", self.current_bytes, self.max_buffer_bytes))
            .field("pressure", &format!("{:.1}%", self.memory_pressure() * 100.0))
            .field("total_written", &self.total_written)
            .field("total_value_bytes", &self.total_value_bytes)
            .field("copy_flushes", &self.copy_flushes)
            .field("insert_flushes", &self.insert_flushes)
            .field("backpressure_events", &self.total_backpressure)
            .finish()
    }
}

impl fmt::Display for StringWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StringWriter: {}/{} buffered ({:.1} KB/{:.1} MB, {:.0}% pressure), \
             {} written ({:.1} KB payload, avg {:.0} bytes), \
             {} flushes ({} COPY/{} UNNEST), {} backpressure",
            self.buffer.len(), self.batch_size,
            self.current_bytes as f64 / 1024.0,
            self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
            self.memory_pressure() * 100.0,
            self.total_written,
            self.total_value_bytes as f64 / 1024.0,
            self.avg_value_len(),
            self.total_flushes, self.copy_flushes, self.insert_flushes,
            self.total_backpressure,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> { Utc::now() }
    fn row(pv: i32, val: &str) -> StringRow { StringRow::new(now(), pv, val.to_string(), 0, 0) }
    fn row_sev(pv: i32, val: &str, sev: i16) -> StringRow {
        StringRow::new(now(), pv, val.to_string(), sev, 1)
    }

    // ═════════════════════════════════════════════════════════════════
    // StringRow
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_row_new() {
        let r = row(1, "hello");
        assert_eq!(r.pv_id, 1);
        assert_eq!(r.value, "hello");
        assert_eq!(r.value_len(), 5);
        assert!(!r.is_value_empty());
    }

    #[test]
    fn test_row_empty_value() {
        let r = row(1, "");
        assert!(r.is_value_empty());
        assert_eq!(r.value_len(), 0);
    }

    #[test]
    fn test_row_severity() {
        let r = row_sev(1, "alarm", 2);
        assert_eq!(r.severity, 2);
        assert_eq!(r.status, 1);
    }

    #[test]
    fn test_row_mem_size() {
        assert_eq!(row(1, "hello").mem_size(), 45);
        assert_eq!(row(1, "").mem_size(), 40);
    }

    #[test]
    fn test_row_mem_size_large() {
        let big = "x".repeat(10_000);
        assert_eq!(row(1, &big).mem_size(), 40 + 10_000);
    }

    #[test]
    fn test_row_clone() {
        let a = row(1, "hello");
        let b = a.clone();
        assert_eq!(a.value, b.value);
    }

    #[test]
    fn test_row_display_short() {
        let s = row(42, "hello").to_string();
        assert!(s.contains("pv_id=42"));
        assert!(s.contains("\"hello\""));
    }

    #[test]
    fn test_row_display_long() {
        let s = row(1, &"a".repeat(100)).to_string();
        assert!(s.contains("..."));
        assert!(s.contains("100 bytes"));
    }

    #[test]
    fn test_row_debug() {
        assert!(format!("{:?}", row(1, "x")).contains("StringRow"));
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
    // escape_copy_text
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_escape_plain() {
        let mut out = String::new();
        escape_copy_text("hello world", &mut out);
        assert_eq!(out, "hello world");
    }

    #[test]
    fn test_escape_tab() {
        let mut out = String::new();
        escape_copy_text("a\tb", &mut out);
        assert_eq!(out, "a\\tb");
    }

    #[test]
    fn test_escape_newline() {
        let mut out = String::new();
        escape_copy_text("line1\nline2", &mut out);
        assert_eq!(out, "line1\\nline2");
    }

    #[test]
    fn test_escape_carriage_return() {
        let mut out = String::new();
        escape_copy_text("a\rb", &mut out);
        assert_eq!(out, "a\\rb");
    }

    #[test]
    fn test_escape_backslash() {
        let mut out = String::new();
        escape_copy_text("path\\to\\file", &mut out);
        assert_eq!(out, "path\\\\to\\\\file");
    }

    #[test]
    fn test_escape_combined() {
        let mut out = String::new();
        escape_copy_text("a\t\n\r\\b", &mut out);
        assert_eq!(out, "a\\t\\n\\r\\\\b");
    }

    #[test]
    fn test_escape_empty() {
        let mut out = String::new();
        escape_copy_text("", &mut out);
        assert_eq!(out, "");
    }

    #[test]
    fn test_escape_unicode() {
        let mut out = String::new();
        escape_copy_text("température: 4.2°K", &mut out);
        assert_eq!(out, "température: 4.2°K");
    }

    // ═════════════════════════════════════════════════════════════════
    // StringWriter — Construction
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_new_defaults() {
        let w = StringWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.batch_size(), 500);
        assert_eq!(w.max_buffer_bytes(), DEFAULT_MAX_BUFFER_BYTES);
        assert_eq!(w.buffered_bytes(), 0);
        assert_eq!(w.memory_pressure(), 0.0);
        assert_eq!(w.total_written(), 0);
        assert_eq!(w.total_flushes(), 0);
        assert_eq!(w.total_value_bytes(), 0);
        assert_eq!(w.total_backpressure(), 0);
        assert_eq!(w.copy_flushes(), 0);
        assert_eq!(w.insert_flushes(), 0);
    }

    #[test]
    fn test_custom_limits() {
        let w = StringWriter::with_limits(100, 1024 * 1024);
        assert_eq!(w.batch_size(), 100);
        assert_eq!(w.max_buffer_bytes(), 1024 * 1024);
    }

    #[test]
    fn test_min_batch_size() {
        assert_eq!(StringWriter::new(0).batch_size(), 1);
    }

    #[test]
    fn test_min_buffer_bytes() {
        assert_eq!(StringWriter::with_limits(10, 0).max_buffer_bytes(), 1024);
    }

    // ═════════════════════════════════════════════════════════════════
    // StringWriter — Push
    // ═════════════════════════════════════════════════════════════════

    #[test]
    fn test_push_accepted() {
        let mut w = StringWriter::with_defaults();
        assert_eq!(w.push(row(1, "hello")), PushResult::Accepted);
        assert_eq!(w.buffered(), 1);
        assert_eq!(w.buffered_bytes(), 45);
    }

    #[test]
    fn test_push_until_full() {
        let mut w = StringWriter::new(3);
        assert_eq!(w.push(row(1, "a")), PushResult::Accepted);
        assert_eq!(w.push(row(2, "b")), PushResult::Accepted);
        assert_eq!(w.push(row(3, "c")), PushResult::Full);
        assert!(w.is_full());
    }

    // ── Backpressure ─────────────────────────────────────────────────

    #[test]
    fn test_backpressure_triggered() {
        let mut w = StringWriter::with_limits(100, 100);
        assert_eq!(w.push(row(1, "hello")), PushResult::Accepted); // 45 bytes
        assert_eq!(w.push(row(2, "world")), PushResult::Accepted); // 90 bytes
        assert_eq!(w.push(row(3, "!!!!!")), PushResult::BackpressureExceeded); // 135 > 100
        assert_eq!(w.buffered(), 2);
        assert_eq!(w.total_backpressure(), 1);
    }

    #[test]
    fn test_backpressure_first_row_always_accepted() {
        let mut w = StringWriter::with_limits(100, 10);
        let r = w.push(row(1, &"x".repeat(1000)));
        assert!(r.is_accepted());
    }

    #[test]
    fn test_backpressure_clears_after_discard() {
        let mut w = StringWriter::with_limits(100, 100);
        w.push(row(1, &"x".repeat(80)));
        w.discard();
        assert_eq!(w.push(row(2, "fresh")), PushResult::Accepted);
    }

    #[test]
    fn test_memory_pressure() {
        let mut w = StringWriter::with_limits(100, 1000);
        w.push(row(1, &"x".repeat(60))); // 100 bytes → 10%
        let p = w.memory_pressure();
        assert!(p > 0.09 && p < 0.11, "got {:.1}%", p * 100.0);
    }

    // ── Byte tracking ────────────────────────────────────────────────

    #[test]
    fn test_bytes_incremental() {
        let mut w = StringWriter::with_defaults();
        w.push(row(1, "abc")); // 43
        w.push(row(2, "defgh")); // 45
        assert_eq!(w.buffered_bytes(), 43 + 45);
    }

    #[test]
    fn test_bytes_reset_on_discard() {
        let mut w = StringWriter::with_defaults();
        w.push(row(1, "test"));
        w.discard();
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── Discard ──────────────────────────────────────────────────────

    #[test]
    fn test_discard() {
        let mut w = StringWriter::with_defaults();
        w.push(row(1, "a"));
        w.push(row(2, "b"));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── Statistics ───────────────────────────────────────────────────

    #[test]
    fn test_avg_value_len() {
        let mut w = StringWriter::with_defaults();
        w.total_written = 100;
        w.total_value_bytes = 5_000;
        assert_eq!(w.avg_value_len(), 50.0);
    }

    #[test]
    fn test_avg_value_len_empty() {
        assert_eq!(StringWriter::with_defaults().avg_value_len(), 0.0);
    }

    #[test]
    fn test_avg_batch_size() {
        let mut w = StringWriter::with_defaults();
        w.total_written = 1000;
        w.total_flushes = 4;
        assert_eq!(w.avg_batch_size(), 250.0);
    }

    #[test]
    fn test_avg_batch_size_empty() {
        assert_eq!(StringWriter::with_defaults().avg_batch_size(), 0.0);
    }

    // ── SQL constants ────────────────────────────────────────────────

    #[test]
    fn test_insert_sql_columns() {
        let u = INSERT_SQL.to_uppercase();
        for col in ["TIME", "PV_ID", "VALUE", "SEVERITY", "STATUS"] {
            assert!(u.contains(col), "missing: {col}");
        }
    }

    #[test]
    fn test_insert_sql_uses_unnest() {
        assert!(INSERT_SQL.to_uppercase().contains("UNNEST"));
    }

    #[test]
    fn test_copy_threshold_sane() {
        assert!(COPY_THRESHOLD > 0 && COPY_THRESHOLD <= 500);
    }

    // ── Display / Debug ──────────────────────────────────────────────

    #[test]
    fn test_display_empty() {
        let s = StringWriter::with_defaults().to_string();
        assert!(s.contains("0/500"));
        assert!(s.contains("0 backpressure"));
    }

    #[test]
    fn test_display_with_stats() {
        let mut w = StringWriter::with_defaults();
        w.total_written = 200;
        w.total_value_bytes = 10_000;
        w.copy_flushes = 2;
        w.insert_flushes = 2;
        w.total_backpressure = 1;
        let s = w.to_string();
        assert!(s.contains("200 written"));
        assert!(s.contains("2 COPY"));
        assert!(s.contains("2 UNNEST"));
        assert!(s.contains("1 backpressure"));
    }

    #[test]
    fn test_debug() {
        let d = format!("{:?}", StringWriter::with_defaults());
        assert!(d.contains("StringWriter"));
        assert!(d.contains("pressure"));
        assert!(d.contains("backpressure_events"));
    }
}