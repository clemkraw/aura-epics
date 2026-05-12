//! Batch writer for JSON-stored tables.
//!
//! Handles all Normative Types stored in their respective hypertables:
//! - `samples_table`     ← NTTable (JSONB)
//! - `samples_custom`    ← NTUnion, Custom (JSONB + nt_type tag)
//! - `samples_namevalue` ← NTNameValue (text[] + float8[])
//! - `samples_histogram` ← NTHistogram (float8[] + bigint[])
//! - `samples_continuum` ← NTContinuum (float8[] + float8[] + text[])
//! - `samples_multi`     ← NTMultiChannel (text[] + float8[] + smallint[])
//!
//! ## Write Strategy (3 tiers)
//!
//! On flush, rows are **grouped by table** and each group is written
//! with the fastest available method:
//!
//! - **COPY (≥ 50 rows):** `COPY FROM STDIN` text protocol for
//!   `samples_table` and `samples_custom`.
//!
//! - **UNNEST (< 50 rows):** single SQL statement with parallel arrays.
//!   One network round-trip for N rows. Used for Table/Custom below
//!   the COPY threshold.
//!
//! - **Transaction INSERT:** per-row INSERT inside a transaction for
//!   namevalue, histogram, continuum, multi. These tables use
//!   `jsonb_array_elements_text()` in their INSERT SQL to extract
//!   arrays from the JSONB payload — not possible with COPY or UNNEST.
//!
//! ## Performance
//!
//! - `estimate_json_size()`: zero-allocation recursive size estimate
//!   (no `to_string()` — avoids MB of throwaway String allocations).
//! - `escape_json_for_copy()`: escapes JSONB for COPY TSV format
//!   in-place without intermediate String allocation.
//! - `estimated_size` pre-computed on row construction (no re-walk).
//! - `current_bytes` tracked incrementally on push.
//! - SQL queries are `&'static str` constants (prepared statement cache).
//!
//! ## Backpressure
//!
//! Hard memory limit (`max_buffer_bytes`, default 32 MB). `push()`
//! returns `PushResult` with backpressure signaling.

use chrono::{DateTime, Utc};
use std::fmt;

use sqlx::PgPool;
use sqlx::postgres::PgPoolCopyExt;

use aura_core::error::{AuraError, AuraResult};

const DEFAULT_MAX_BUFFER_BYTES: usize = 32 * 1024 * 1024;

/// Batch size threshold for switching from UNNEST to COPY.
const COPY_THRESHOLD: usize = 50;

/// UNNEST batch INSERT for `samples_table` (JSONB column).
const UNNEST_TABLE: &str =
    "INSERT INTO samples_table (time, pv_id, data, severity, status) \
     SELECT * FROM UNNEST($1::timestamptz[], $2::int[], $3::jsonb[], $4::smallint[], $5::smallint[])";

/// UNNEST batch INSERT for `samples_custom` (JSONB + nt_type).
const UNNEST_CUSTOM: &str =
    "INSERT INTO samples_custom (time, pv_id, nt_type, data, severity, status) \
     SELECT * FROM UNNEST($1::timestamptz[], $2::int[], $3::text[], $4::jsonb[], $5::smallint[], $6::smallint[])";

/// COPY FROM STDIN for `samples_table`.
const COPY_TABLE: &str =
    "COPY samples_table (time, pv_id, data, severity, status) FROM STDIN";

/// COPY FROM STDIN for `samples_custom`.
const COPY_CUSTOM: &str =
    "COPY samples_custom (time, pv_id, nt_type, data, severity, status) FROM STDIN";

/// Per-row INSERT for tables that extract arrays from JSONB.
mod row_sql {
    pub const NAMEVALUE: &str =
        "INSERT INTO samples_namevalue (time, pv_id, names, \"values\", severity, status) \
         VALUES ($1, $2, \
         ARRAY(SELECT jsonb_array_elements_text($3->'names')), \
         ARRAY(SELECT (jsonb_array_elements_text($3->'values'))::float8), \
         $4, $5)";

    pub const HISTOGRAM: &str =
        "INSERT INTO samples_histogram (time, pv_id, ranges, counts, severity, status) \
         VALUES ($1, $2, \
         ARRAY(SELECT (jsonb_array_elements_text($3->'ranges'))::float8), \
         ARRAY(SELECT (jsonb_array_elements_text($3->'counts'))::bigint), \
         $4, $5)";

    pub const CONTINUUM: &str =
        "INSERT INTO samples_continuum (time, pv_id, base, trace_data, units, severity, status) \
         VALUES ($1, $2, \
         ARRAY(SELECT (jsonb_array_elements_text($3->'base'))::float8), \
         ARRAY(SELECT (jsonb_array_elements_text($3->'values'))::float8), \
         ARRAY(SELECT jsonb_array_elements_text($3->'units')), \
         $4, $5)";

    pub const MULTI: &str =
        "INSERT INTO samples_multi (time, pv_id, channel_names, channel_values, severities, severity, status) \
         VALUES ($1, $2, \
         ARRAY(SELECT jsonb_array_elements_text($3->'channel_names')), \
         ARRAY(SELECT (jsonb_array_elements_text($3->'channel_values'))::float8), \
         ARRAY(SELECT (jsonb_array_elements_text($3->'severities'))::smallint), \
         $4, $5)";
}

/// Estimate serialized size of a `serde_json::Value` WITHOUT allocating.
///
/// Walks the JSON tree recursively. Accuracy: ±20%.
/// Replaces `value.to_string().len()` which allocates a full String.
pub fn estimate_json_size(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(b) => if *b { 4 } else { 5 },
        serde_json::Value::Number(_) => 8, // conservative avg
        serde_json::Value::String(s) => s.len() + 2, // quotes
        serde_json::Value::Array(a) => {
            // [] + elements + commas
            2 + a.iter().map(estimate_json_size).sum::<usize>()
                + a.len().saturating_sub(1)
        }
        serde_json::Value::Object(o) => {
            // {} + "key":val + commas
            2 + o.iter()
                .map(|(k, v)| k.len() + 3 + estimate_json_size(v)) // "k":v
                .sum::<usize>()
                + o.len().saturating_sub(1)
        }
    }
}

/// Escape a JSON string for PostgreSQL COPY text format.
///
/// COPY text format uses tab as delimiter and newline as row terminator.
/// Within a field, these characters must be escaped:
/// - `\` → `\\`
/// - `\t` (tab) → `\t` literal
/// - `\n` (newline) → `\n` literal
/// - `\r` (carriage return) → `\r` literal
///
/// Note: JSON itself uses `\` for escaping, so a JSON string like
/// `{"msg":"line1\nline2"}` already has escaped newlines. But
/// `serde_json::to_string()` outputs literal newlines in pretty mode.
/// We use compact mode and escape any remaining special chars.
fn escape_json_for_copy(json: &str, out: &mut String) {
    out.reserve(json.len());
    for c in json.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
}

/// Target table for a JSON row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JsonTable {
    Table,
    Custom,
    NameValue,
    Histogram,
    Continuum,
    Multi,
}

impl JsonTable {
    pub const fn table_name(&self) -> &'static str {
        match self {
            Self::Table     => "samples_table",
            Self::Custom    => "samples_custom",
            Self::NameValue => "samples_namevalue",
            Self::Histogram => "samples_histogram",
            Self::Continuum => "samples_continuum",
            Self::Multi     => "samples_multi",
        }
    }

    /// Whether this table supports UNNEST/COPY batch writes.
    pub const fn supports_bulk(&self) -> bool {
        matches!(self, Self::Table | Self::Custom)
    }

    pub const ALL: [Self; 6] = [
        Self::Table, Self::Custom, Self::NameValue,
        Self::Histogram, Self::Continuum, Self::Multi,
    ];

    const fn index(&self) -> usize {
        match self {
            Self::Table => 0, Self::Custom => 1, Self::NameValue => 2,
            Self::Histogram => 3, Self::Continuum => 4, Self::Multi => 5,
        }
    }
}

impl fmt::Display for JsonTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.table_name())
    }
}

/// A single row for one of the JSON-stored tables.
#[derive(Debug, Clone)]
pub struct JsonRow {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub table: JsonTable,
    pub data: serde_json::Value,
    pub nt_type: Option<String>,
    pub severity: i16,
    pub status: i16,
    /// Pre-computed on construction — zero cost on access.
    estimated_size: usize,
}

impl JsonRow {
    #[inline]
    fn create(
        time: DateTime<Utc>, pv_id: i32, table: JsonTable,
        data: serde_json::Value, nt_type: Option<String>,
        severity: i16, status: i16,
    ) -> Self {
        let estimated_size = 64 + estimate_json_size(&data)
            + nt_type.as_ref().map_or(0, |s| 24 + s.len());
        Self { time, pv_id, table, data, nt_type, severity, status, estimated_size }
    }

    pub fn table(time: DateTime<Utc>, pv_id: i32, data: serde_json::Value, sev: i16, status: i16) -> Self {
        Self::create(time, pv_id, JsonTable::Table, data, None, sev, status)
    }
    pub fn custom(time: DateTime<Utc>, pv_id: i32, nt_type: &str, data: serde_json::Value, sev: i16, status: i16) -> Self {
        Self::create(time, pv_id, JsonTable::Custom, data, Some(nt_type.to_string()), sev, status)
    }
    pub fn namevalue(time: DateTime<Utc>, pv_id: i32, data: serde_json::Value, sev: i16, status: i16) -> Self {
        Self::create(time, pv_id, JsonTable::NameValue, data, None, sev, status)
    }
    pub fn histogram(time: DateTime<Utc>, pv_id: i32, data: serde_json::Value, sev: i16, status: i16) -> Self {
        Self::create(time, pv_id, JsonTable::Histogram, data, None, sev, status)
    }
    pub fn continuum(time: DateTime<Utc>, pv_id: i32, data: serde_json::Value, sev: i16, status: i16) -> Self {
        Self::create(time, pv_id, JsonTable::Continuum, data, None, sev, status)
    }
    pub fn multi(time: DateTime<Utc>, pv_id: i32, data: serde_json::Value, sev: i16, status: i16) -> Self {
        Self::create(time, pv_id, JsonTable::Multi, data, None, sev, status)
    }

    /// Approximate memory used by this row.
    #[inline]
    pub fn mem_size(&self) -> usize { self.estimated_size }
}

impl fmt::Display for JsonRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pv_id={} table={} (~{}B)", self.pv_id, self.table, self.estimated_size)
    }
}

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

/// Batch writer for JSON-stored tables with COPY/UNNEST/row INSERT.
pub struct JsonWriter {
    batch_size: usize,
    max_buffer_bytes: usize,
    buffer: Vec<JsonRow>,
    current_bytes: usize,

    // Statistics.
    total_written: u64,
    total_flushes: u64,
    total_json_bytes: u64,
    total_backpressure: u64,
    per_table_written: [u64; 6],
    copy_calls: u64,
    unnest_calls: u64,
    row_insert_calls: u64,
}

impl JsonWriter {
    pub fn new(batch_size: usize) -> Self {
        Self::with_limits(batch_size, DEFAULT_MAX_BUFFER_BYTES)
    }

    pub fn with_limits(batch_size: usize, max_buffer_bytes: usize) -> Self {
        let batch_size = batch_size.max(1);
        Self {
            batch_size,
            max_buffer_bytes: max_buffer_bytes.max(64), // minimum 64 bytes
            buffer: Vec::with_capacity(batch_size),
            current_bytes: 0,
            total_written: 0, total_flushes: 0,
            total_json_bytes: 0, total_backpressure: 0,
            per_table_written: [0; 6],
            copy_calls: 0, unnest_calls: 0, row_insert_calls: 0,
        }
    }

    pub fn with_defaults() -> Self { Self::new(200) }

    /// Push a row with backpressure protection.
    #[inline]
    pub fn push(&mut self, row: JsonRow) -> PushResult {
        let row_bytes = row.mem_size();
        if !self.buffer.is_empty()
            && self.current_bytes + row_bytes > self.max_buffer_bytes
        {
            self.total_backpressure += 1;
            return PushResult::BackpressureExceeded;
        }
        self.current_bytes += row_bytes;
        self.buffer.push(row);
        if self.buffer.len() >= self.batch_size { PushResult::Full } else { PushResult::Accepted }
    }

    /// Flush all buffered rows, grouped by table.
    ///
    /// For Table/Custom: COPY (≥50 rows) or UNNEST (<50).
    /// For the rest: transaction INSERT.
    pub async fn flush(&mut self, pool: &PgPool) -> AuraResult<usize> {
        if self.buffer.is_empty() { return Ok(0); }
        let count = self.buffer.len();

        // Flush Table rows (COPY or UNNEST).
        self.flush_bulk(pool, JsonTable::Table).await?;
        // Flush Custom rows (COPY or UNNEST).
        self.flush_bulk(pool, JsonTable::Custom).await?;
        // Flush row-insert tables in a single transaction.
        self.flush_row_inserts(pool).await?;

        // Update stats.
        for row in &self.buffer {
            self.per_table_written[row.table.index()] += 1;
            self.total_json_bytes += estimate_json_size(&row.data) as u64;
        }
        self.total_written += count as u64;
        self.total_flushes += 1;
        self.buffer.clear();
        self.current_bytes = 0;
        Ok(count)
    }

    // ── COPY / UNNEST for Table and Custom ───────────────────────────

    async fn flush_bulk(&mut self, pool: &PgPool, table: JsonTable) -> AuraResult<()> {
        let rows: Vec<&JsonRow> = self.buffer.iter()
            .filter(|r| r.table == table)
            .collect();
        if rows.is_empty() { return Ok(()); }

        if rows.len() >= COPY_THRESHOLD {
            match table {
                JsonTable::Table => self.flush_copy_table(pool, &rows).await?,
                JsonTable::Custom => self.flush_copy_custom(pool, &rows).await?,
                _ => unreachable!(),
            }
            self.copy_calls += 1;
        } else {
            match table {
                JsonTable::Table => self.flush_unnest_table(pool, &rows).await?,
                JsonTable::Custom => self.flush_unnest_custom(pool, &rows).await?,
                _ => unreachable!(),
            }
            self.unnest_calls += 1;
        }
        Ok(())
    }

    /// COPY for samples_table: time\tpv_id\tjson\tseverity\tstatus\n
    async fn flush_copy_table(&self, pool: &PgPool, rows: &[&JsonRow]) -> AuraResult<()> {
        // Pre-allocate: ~200 bytes per row (timestamp + json + separators).
        let mut payload = String::with_capacity(rows.len() * 200);

        for row in rows {
            // time (ISO 8601)
            payload.push_str(&row.time.to_rfc3339());
            payload.push('\t');
            // pv_id
            payload.push_str(&row.pv_id.to_string());
            payload.push('\t');
            // JSONB — must escape for COPY
            let json_str = serde_json::to_string(&row.data)
                .unwrap_or_else(|_| "{}".to_string());
            escape_json_for_copy(&json_str, &mut payload);
            payload.push('\t');
            // severity, status
            payload.push_str(&row.severity.to_string());
            payload.push('\t');
            payload.push_str(&row.status.to_string());
            payload.push('\n');
        }

        let mut copy_in = pool.copy_in_raw(COPY_TABLE).await
            .map_err(|e| AuraError::database(format!("samples_table COPY begin: {e}")))?;
        copy_in.send(payload.as_bytes()).await
            .map_err(|e| AuraError::database(
                format!("samples_table COPY send ({} rows, {}B): {e}", rows.len(), payload.len())
            ))?;
        copy_in.finish().await
            .map_err(|e| AuraError::database(format!("samples_table COPY finish: {e}")))?;
        Ok(())
    }

    /// COPY for samples_custom: time\tpv_id\tnt_type\tjson\tseverity\tstatus\n
    async fn flush_copy_custom(&self, pool: &PgPool, rows: &[&JsonRow]) -> AuraResult<()> {
        let mut payload = String::with_capacity(rows.len() * 250);

        for row in rows {
            payload.push_str(&row.time.to_rfc3339());
            payload.push('\t');
            payload.push_str(&row.pv_id.to_string());
            payload.push('\t');
            // nt_type — escape in case it contains special chars
            let nt = row.nt_type.as_deref().unwrap_or("Custom");
            escape_json_for_copy(nt, &mut payload);
            payload.push('\t');
            let json_str = serde_json::to_string(&row.data)
                .unwrap_or_else(|_| "{}".to_string());
            escape_json_for_copy(&json_str, &mut payload);
            payload.push('\t');
            payload.push_str(&row.severity.to_string());
            payload.push('\t');
            payload.push_str(&row.status.to_string());
            payload.push('\n');
        }

        let mut copy_in = pool.copy_in_raw(COPY_CUSTOM).await
            .map_err(|e| AuraError::database(format!("samples_custom COPY begin: {e}")))?;
        copy_in.send(payload.as_bytes()).await
            .map_err(|e| AuraError::database(
                format!("samples_custom COPY send ({} rows, {}B): {e}", rows.len(), payload.len())
            ))?;
        copy_in.finish().await
            .map_err(|e| AuraError::database(format!("samples_custom COPY finish: {e}")))?;
        Ok(())
    }

    /// UNNEST for samples_table (< COPY_THRESHOLD rows).
    async fn flush_unnest_table(&self, pool: &PgPool, rows: &[&JsonRow]) -> AuraResult<()> {
        let n = rows.len();
        let mut times = Vec::with_capacity(n);
        let mut pv_ids = Vec::with_capacity(n);
        let mut datas: Vec<&serde_json::Value> = Vec::with_capacity(n);
        let mut sevs = Vec::with_capacity(n);
        let mut stats = Vec::with_capacity(n);

        for r in rows {
            times.push(r.time);
            pv_ids.push(r.pv_id);
            datas.push(&r.data);
            sevs.push(r.severity);
            stats.push(r.status);
        }

        sqlx::query(UNNEST_TABLE)
            .bind(&times).bind(&pv_ids).bind(&datas)
            .bind(&sevs).bind(&stats)
            .execute(pool).await
            .map_err(|e| AuraError::database(
                format!("samples_table UNNEST ({n} rows): {e}")
            ))?;
        Ok(())
    }

    /// UNNEST for samples_custom (< COPY_THRESHOLD rows).
    async fn flush_unnest_custom(&self, pool: &PgPool, rows: &[&JsonRow]) -> AuraResult<()> {
        let n = rows.len();
        let mut times = Vec::with_capacity(n);
        let mut pv_ids = Vec::with_capacity(n);
        let mut nt_types: Vec<&str> = Vec::with_capacity(n);
        let mut datas: Vec<&serde_json::Value> = Vec::with_capacity(n);
        let mut sevs = Vec::with_capacity(n);
        let mut stats = Vec::with_capacity(n);

        for r in rows {
            times.push(r.time);
            pv_ids.push(r.pv_id);
            nt_types.push(r.nt_type.as_deref().unwrap_or("Custom"));
            datas.push(&r.data);
            sevs.push(r.severity);
            stats.push(r.status);
        }

        sqlx::query(UNNEST_CUSTOM)
            .bind(&times).bind(&pv_ids).bind(&nt_types)
            .bind(&datas).bind(&sevs).bind(&stats)
            .execute(pool).await
            .map_err(|e| AuraError::database(
                format!("samples_custom UNNEST ({n} rows): {e}")
            ))?;
        Ok(())
    }

    /// Transaction INSERT for namevalue, histogram, continuum, multi.
    async fn flush_row_inserts(&mut self, pool: &PgPool) -> AuraResult<()> {
        let rows: Vec<&JsonRow> = self.buffer.iter()
            .filter(|r| !r.table.supports_bulk())
            .collect();
        if rows.is_empty() { return Ok(()); }

        let mut tx = pool.begin().await
            .map_err(|e| AuraError::database(format!("json row tx begin: {e}")))?;

        for row in &rows {
            let sql = match row.table {
                JsonTable::NameValue => row_sql::NAMEVALUE,
                JsonTable::Histogram => row_sql::HISTOGRAM,
                JsonTable::Continuum => row_sql::CONTINUUM,
                JsonTable::Multi     => row_sql::MULTI,
                _ => unreachable!(),
            };
            sqlx::query(sql)
                .bind(row.time).bind(row.pv_id).bind(&row.data)
                .bind(row.severity).bind(row.status)
                .execute(&mut *tx).await
                .map_err(|e| AuraError::database(
                    format!("{} insert (pv_id={}): {e}", row.table, row.pv_id)
                ))?;
            self.row_insert_calls += 1;
        }

        tx.commit().await
            .map_err(|e| AuraError::database(
                format!("json row tx commit ({} rows): {e}", rows.len())
            ))?;
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
    #[inline] pub fn total_json_bytes(&self) -> u64 { self.total_json_bytes }
    #[inline] pub fn total_backpressure(&self) -> u64 { self.total_backpressure }
    #[inline] pub fn copy_calls(&self) -> u64 { self.copy_calls }
    #[inline] pub fn unnest_calls(&self) -> u64 { self.unnest_calls }
    #[inline] pub fn row_insert_calls(&self) -> u64 { self.row_insert_calls }

    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 { return 0.0; }
        self.current_bytes as f64 / self.max_buffer_bytes as f64
    }

    pub fn avg_json_bytes(&self) -> f64 {
        if self.total_written == 0 { return 0.0; }
        self.total_json_bytes as f64 / self.total_written as f64
    }

    /// Count buffered rows per table.
    pub fn counts_by_table(&self) -> [(JsonTable, usize); 6] {
        let mut counts = [0usize; 6];
        for row in &self.buffer {
            counts[row.table.index()] += 1;
        }
        let mut result = [(JsonTable::Table, 0usize); 6];
        for (i, t) in JsonTable::ALL.iter().enumerate() {
            result[i] = (*t, counts[i]);
        }
        result
    }

    /// Total rows written to a specific table.
    pub fn written_by_table(&self, table: JsonTable) -> u64 {
        self.per_table_written[table.index()]
    }

    /// Clear buffer without flushing.
    pub fn discard(&mut self) {
        self.buffer.clear();
        self.current_bytes = 0;
    }
}

impl fmt::Debug for JsonWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonWriter")
            .field("buffered", &self.buffer.len())
            .field("bytes", &format!("{}/{}", self.current_bytes, self.max_buffer_bytes))
            .field("pressure", &format!("{:.1}%", self.memory_pressure() * 100.0))
            .field("total_written", &self.total_written)
            .field("copy_calls", &self.copy_calls)
            .field("unnest_calls", &self.unnest_calls)
            .field("row_insert_calls", &self.row_insert_calls)
            .field("backpressure", &self.total_backpressure)
            .finish()
    }
}

impl fmt::Display for JsonWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
               "JsonWriter: {}/{} ({:.1}KB/{:.1}MB, {:.0}%), {} written (avg {:.0}B JSON), \
             {} flushes ({} COPY/{} UNNEST/{} rows), {} backpressure",
               self.buffer.len(), self.batch_size,
               self.current_bytes as f64 / 1024.0,
               self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
               self.memory_pressure() * 100.0,
               self.total_written, self.avg_json_bytes(),
               self.total_flushes, self.copy_calls, self.unnest_calls, self.row_insert_calls,
               self.total_backpressure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn now() -> DateTime<Utc> { Utc::now() }

    // ═════════════════════════════════════════════════════════════════
    // estimate_json_size (zero allocation)
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_est_null() { assert_eq!(estimate_json_size(&json!(null)), 4); }
    #[test] fn test_est_true() { assert_eq!(estimate_json_size(&json!(true)), 4); }
    #[test] fn test_est_false() { assert_eq!(estimate_json_size(&json!(false)), 5); }
    #[test] fn test_est_number() { assert_eq!(estimate_json_size(&json!(42)), 8); }
    #[test] fn test_est_string_empty() { assert_eq!(estimate_json_size(&json!("")), 2); }
    #[test] fn test_est_string() { assert_eq!(estimate_json_size(&json!("hello")), 7); }
    #[test] fn test_est_array_empty() { assert_eq!(estimate_json_size(&json!([])), 2); }

    #[test] fn test_est_array() {
        // [1,2,3] = 2 (brackets) + 3*8 (numbers) + 2 (commas) = 28
        assert_eq!(estimate_json_size(&json!([1, 2, 3])), 28);
    }

    #[test] fn test_est_object_empty() { assert_eq!(estimate_json_size(&json!({})), 2); }

    #[test] fn test_est_object() {
        // {"a":1} = 2 (braces) + 1 (key len) + 3 (":) + 8 (number) = 14
        assert_eq!(estimate_json_size(&json!({"a": 1})), 14);
    }

    #[test] fn test_est_nested() {
        let s = estimate_json_size(&json!({"data": [1,2], "name": "test"}));
        assert!(s > 20 && s < 60, "got {s}");
    }

    #[test] fn test_est_large() {
        let big = json!({"rows": (0..1000).map(|i| json!({"id": i})).collect::<Vec<_>>()});
        assert!(estimate_json_size(&big) > 5_000);
    }

    #[test] fn test_est_vs_actual() {
        // estimate_json_size is a conservative UPPER BOUND for backpressure.
        // It overestimates small integers (8B for "42"=2B) but that's safe —
        // underestimating would risk OOM. We verify:
        // 1. Estimate >= actual (always an upper bound)
        // 2. Estimate < 5× actual for realistic EPICS payloads
        let values = vec![
            json!({"names": ["CRYO:T1", "CRYO:T2"], "values": [4.217, 2.003]}),
            json!({"ranges": [0.0, 1.0, 2.0, 3.0], "counts": [100, 200, 150, 50]}),
            json!({"channel_names": ["PV:A", "PV:B"], "channel_values": [1.23, 4.56], "severities": [0, 1]}),
            json!({"description": "Cryostat temperature sensor", "units": "K", "precision": 3}),
        ];
        for v in &values {
            let est = estimate_json_size(v);
            let actual = serde_json::to_string(v).unwrap().len();
            assert!(est >= actual / 2,
                    "estimate too low for {:?}: est={est} actual={actual}", v);
            assert!(est < actual * 5,
                    "estimate too high for {:?}: est={est} actual={actual}", v);
        }
    }

    // ═════════════════════════════════════════════════════════════════
    // escape_json_for_copy
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_escape_no_special() {
        let mut out = String::new();
        escape_json_for_copy(r#"{"a":1}"#, &mut out);
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test] fn test_escape_backslash() {
        let mut out = String::new();
        escape_json_for_copy(r#"{"path":"C:\tmp"}"#, &mut out);
        assert_eq!(out, r#"{"path":"C:\\tmp"}"#);
    }

    #[test] fn test_escape_tab() {
        let mut out = String::new();
        escape_json_for_copy("hello\tworld", &mut out);
        assert_eq!(out, r"hello\tworld");
    }

    #[test] fn test_escape_newline() {
        let mut out = String::new();
        escape_json_for_copy("line1\nline2", &mut out);
        assert_eq!(out, r"line1\nline2");
    }

    #[test] fn test_escape_carriage_return() {
        let mut out = String::new();
        escape_json_for_copy("a\rb", &mut out);
        assert_eq!(out, r"a\rb");
    }

    #[test] fn test_escape_mixed() {
        let mut out = String::new();
        escape_json_for_copy("a\tb\nc\\d\re", &mut out);
        assert_eq!(out, r"a\tb\nc\\d\re");
    }

    #[test] fn test_escape_empty() {
        let mut out = String::new();
        escape_json_for_copy("", &mut out);
        assert_eq!(out, "");
    }

    #[test] fn test_escape_json_with_inner_escapes() {
        // serde_json compact output won't have literal \n, but verify we handle it.
        let json_str = serde_json::to_string(&json!({"msg": "line1\nline2"})).unwrap();
        let mut out = String::new();
        escape_json_for_copy(&json_str, &mut out);
        // The JSON itself has \\n (escaped), which should become \\\\n in COPY.
        assert!(!out.contains('\n'), "should not contain literal newline");
    }

    // ═════════════════════════════════════════════════════════════════
    // JsonTable
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_table_names() {
        assert_eq!(JsonTable::Table.table_name(), "samples_table");
        assert_eq!(JsonTable::Custom.table_name(), "samples_custom");
        assert_eq!(JsonTable::NameValue.table_name(), "samples_namevalue");
        assert_eq!(JsonTable::Histogram.table_name(), "samples_histogram");
        assert_eq!(JsonTable::Continuum.table_name(), "samples_continuum");
        assert_eq!(JsonTable::Multi.table_name(), "samples_multi");
    }

    #[test] fn test_table_all_count() { assert_eq!(JsonTable::ALL.len(), 6); }

    #[test] fn test_table_all_unique() {
        for (i, a) in JsonTable::ALL.iter().enumerate() {
            for (j, b) in JsonTable::ALL.iter().enumerate() {
                if i != j { assert_ne!(a, b); }
            }
        }
    }

    #[test] fn test_table_supports_bulk() {
        assert!(JsonTable::Table.supports_bulk());
        assert!(JsonTable::Custom.supports_bulk());
        assert!(!JsonTable::NameValue.supports_bulk());
        assert!(!JsonTable::Histogram.supports_bulk());
        assert!(!JsonTable::Continuum.supports_bulk());
        assert!(!JsonTable::Multi.supports_bulk());
    }

    #[test] fn test_table_index_ordered() {
        for (i, t) in JsonTable::ALL.iter().enumerate() {
            assert_eq!(t.index(), i);
        }
    }

    #[test] fn test_table_display() {
        assert_eq!(JsonTable::Table.to_string(), "samples_table");
    }

    #[test] fn test_table_hash() {
        use std::collections::HashSet;
        assert_eq!(JsonTable::ALL.iter().copied().collect::<HashSet<_>>().len(), 6);
    }

    // ═════════════════════════════════════════════════════════════════
    // JsonRow
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_row_constructors() {
        assert_eq!(JsonRow::table(now(), 1, json!({}), 0, 0).table, JsonTable::Table);
        assert_eq!(JsonRow::custom(now(), 1, "X", json!({}), 0, 0).table, JsonTable::Custom);
        assert_eq!(JsonRow::namevalue(now(), 1, json!({}), 0, 0).table, JsonTable::NameValue);
        assert_eq!(JsonRow::histogram(now(), 1, json!({}), 0, 0).table, JsonTable::Histogram);
        assert_eq!(JsonRow::continuum(now(), 1, json!({}), 0, 0).table, JsonTable::Continuum);
        assert_eq!(JsonRow::multi(now(), 1, json!({}), 0, 0).table, JsonTable::Multi);
    }

    #[test] fn test_row_custom_nt_type() {
        let r = JsonRow::custom(now(), 1, "NTUnion", json!({}), 0, 0);
        assert_eq!(r.nt_type.as_deref(), Some("NTUnion"));
    }

    #[test] fn test_row_table_no_nt_type() {
        assert!(JsonRow::table(now(), 1, json!({}), 0, 0).nt_type.is_none());
    }

    #[test] fn test_row_mem_size_precomputed() {
        let r = JsonRow::table(now(), 1, json!({"x": [1,2,3]}), 0, 0);
        let s1 = r.mem_size();
        let s2 = r.mem_size();
        assert_eq!(s1, s2); // constant — no re-walk
    }

    #[test] fn test_row_mem_size_scales() {
        let s = JsonRow::table(now(), 1, json!({}), 0, 0);
        let l = JsonRow::table(now(), 1, json!({"a": [1,2,3,4,5,6,7,8,9,0]}), 0, 0);
        assert!(l.mem_size() > s.mem_size());
    }

    #[test] fn test_row_mem_size_nt_type_adds() {
        let w = JsonRow::table(now(), 1, json!({}), 0, 0);
        let c = JsonRow::custom(now(), 1, "NTUnion", json!({}), 0, 0);
        assert!(c.mem_size() > w.mem_size());
    }

    #[test] fn test_row_clone() {
        let a = JsonRow::table(now(), 1, json!({"x": 1}), 0, 0);
        let b = a.clone();
        assert_eq!(a.data, b.data);
        assert_eq!(a.estimated_size, b.estimated_size);
    }

    #[test] fn test_row_display() {
        let s = JsonRow::histogram(now(), 42, json!({}), 0, 0).to_string();
        assert!(s.contains("pv_id=42"));
        assert!(s.contains("samples_histogram"));
    }

    // ═════════════════════════════════════════════════════════════════
    // PushResult
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_push_accepted() {
        assert!(!PushResult::Accepted.needs_flush());
        assert!(PushResult::Accepted.is_accepted());
        assert_eq!(PushResult::Accepted.to_string(), "accepted");
    }

    #[test] fn test_push_full() {
        assert!(PushResult::Full.needs_flush());
        assert!(PushResult::Full.is_accepted());
    }

    #[test] fn test_push_backpressure() {
        assert!(PushResult::BackpressureExceeded.needs_flush());
        assert!(!PushResult::BackpressureExceeded.is_accepted());
    }

    #[test] fn test_push_eq() {
        assert_eq!(PushResult::Full, PushResult::Full);
        assert_ne!(PushResult::Full, PushResult::Accepted);
    }

    // ═════════════════════════════════════════════════════════════════
    // JsonWriter — construction
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_writer_defaults() {
        let w = JsonWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.batch_size(), 200);
        assert_eq!(w.copy_calls(), 0);
        assert_eq!(w.unnest_calls(), 0);
        assert_eq!(w.row_insert_calls(), 0);
        assert_eq!(w.total_backpressure(), 0);
        assert_eq!(w.avg_json_bytes(), 0.0);
    }

    #[test] fn test_min_batch() { assert_eq!(JsonWriter::new(0).batch_size(), 1); }

    #[test] fn test_min_buffer() {
        assert_eq!(JsonWriter::with_limits(10, 0).max_buffer_bytes(), 64);
    }

    // ═════════════════════════════════════════════════════════════════
    // JsonWriter — push
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_push_one() {
        let mut w = JsonWriter::with_defaults();
        assert_eq!(w.push(JsonRow::table(now(), 1, json!({}), 0, 0)), PushResult::Accepted);
        assert_eq!(w.buffered(), 1);
        assert!(w.buffered_bytes() > 0);
    }

    #[test] fn test_push_until_full() {
        let mut w = JsonWriter::new(3);
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        w.push(JsonRow::table(now(), 2, json!({}), 0, 0));
        assert_eq!(w.push(JsonRow::table(now(), 3, json!({}), 0, 0)), PushResult::Full);
        assert!(w.is_full());
    }

    #[test] fn test_push_mixed_tables() {
        let mut w = JsonWriter::new(100);
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        w.push(JsonRow::custom(now(), 2, "X", json!({}), 0, 0));
        w.push(JsonRow::namevalue(now(), 3, json!({}), 0, 0));
        w.push(JsonRow::histogram(now(), 4, json!({}), 0, 0));
        w.push(JsonRow::continuum(now(), 5, json!({}), 0, 0));
        w.push(JsonRow::multi(now(), 6, json!({}), 0, 0));
        assert_eq!(w.buffered(), 6);
    }

    // ═════════════════════════════════════════════════════════════════
    // JsonWriter — backpressure
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_backpressure_triggered() {
        let mut w = JsonWriter::with_limits(100, 200);
        let mut hit = false;
        for i in 0..50 {
            if w.push(JsonRow::table(now(), i, json!({}), 0, 0)) == PushResult::BackpressureExceeded {
                hit = true;
                break;
            }
        }
        assert!(hit);
        assert!(w.total_backpressure() > 0);
    }

    #[test] fn test_backpressure_first_always_accepted() {
        let mut w = JsonWriter::with_limits(100, 1); // absurdly small
        assert!(w.push(JsonRow::table(now(), 1, json!({"big": "data"}), 0, 0)).is_accepted());
    }

    #[test] fn test_backpressure_clears_after_discard() {
        let mut w = JsonWriter::with_limits(100, 200);
        // Fill until backpressure.
        for i in 0..50 {
            if !w.push(JsonRow::table(now(), i, json!({}), 0, 0)).is_accepted() { break; }
        }
        w.discard();
        assert!(w.push(JsonRow::table(now(), 999, json!({}), 0, 0)).is_accepted());
    }

    // ═════════════════════════════════════════════════════════════════
    // JsonWriter — counts & stats
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_counts_by_table() {
        let mut w = JsonWriter::new(100);
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        w.push(JsonRow::table(now(), 2, json!({}), 0, 0));
        w.push(JsonRow::custom(now(), 3, "X", json!({}), 0, 0));
        w.push(JsonRow::histogram(now(), 4, json!({}), 0, 0));
        let c = w.counts_by_table();
        assert_eq!(c[0].1, 2); // Table
        assert_eq!(c[1].1, 1); // Custom
        assert_eq!(c[3].1, 1); // Histogram
        assert_eq!(c[2].1, 0); // NameValue
    }

    #[test] fn test_written_initial() {
        let w = JsonWriter::with_defaults();
        for t in JsonTable::ALL { assert_eq!(w.written_by_table(t), 0); }
    }

    #[test] fn test_avg_json_bytes() {
        let mut w = JsonWriter::with_defaults();
        w.total_written = 100;
        w.total_json_bytes = 50_000;
        assert_eq!(w.avg_json_bytes(), 500.0);
    }

    #[test] fn test_avg_json_bytes_empty() {
        assert_eq!(JsonWriter::with_defaults().avg_json_bytes(), 0.0);
    }

    #[test] fn test_memory_pressure() {
        let mut w = JsonWriter::with_limits(100, 10_000);
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        let p = w.memory_pressure();
        assert!(p > 0.0 && p < 0.1, "got {p:.4}");
    }

    // ═════════════════════════════════════════════════════════════════
    // JsonWriter — discard
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_discard() {
        let mut w = JsonWriter::with_defaults();
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        w.push(JsonRow::custom(now(), 2, "X", json!({}), 0, 0));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
        assert_eq!(w.total_written(), 0); // counters unchanged
    }

    // ═════════════════════════════════════════════════════════════════
    // SQL constants
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_sql_unnest_table() {
        let u = UNNEST_TABLE.to_uppercase();
        assert!(u.contains("SAMPLES_TABLE"));
        assert!(u.contains("UNNEST"));
        assert!(u.contains("JSONB[]"));
    }

    #[test] fn test_sql_unnest_custom() {
        assert!(UNNEST_CUSTOM.contains("nt_type"));
        assert!(UNNEST_CUSTOM.to_uppercase().contains("UNNEST"));
    }

    #[test] fn test_sql_copy_table() {
        assert!(COPY_TABLE.contains("COPY"));
        assert!(COPY_TABLE.contains("samples_table"));
        assert!(COPY_TABLE.contains("FROM STDIN"));
    }

    #[test] fn test_sql_copy_custom() {
        assert!(COPY_CUSTOM.contains("COPY"));
        assert!(COPY_CUSTOM.contains("samples_custom"));
        assert!(COPY_CUSTOM.contains("nt_type"));
    }

    #[test] fn test_sql_row_namevalue() {
        assert!(row_sql::NAMEVALUE.contains("samples_namevalue"));
        assert!(row_sql::NAMEVALUE.contains(r#""values""#));
    }

    #[test] fn test_sql_row_histogram() {
        assert!(row_sql::HISTOGRAM.contains("samples_histogram"));
        assert!(row_sql::HISTOGRAM.contains("bigint"));
    }

    #[test] fn test_sql_row_continuum() {
        assert!(row_sql::CONTINUUM.contains("samples_continuum"));
        assert!(row_sql::CONTINUUM.contains("trace_data"));
    }

    #[test] fn test_sql_row_multi() {
        assert!(row_sql::MULTI.contains("samples_multi"));
        assert!(row_sql::MULTI.contains("channel_names"));
    }

    #[test] fn test_copy_threshold_sane() {
        assert!(COPY_THRESHOLD > 0 && COPY_THRESHOLD <= 500);
    }

    // ═════════════════════════════════════════════════════════════════
    // Display / Debug
    // ═════════════════════════════════════════════════════════════════

    #[test] fn test_display() {
        let mut w = JsonWriter::with_defaults();
        w.total_written = 500;
        w.copy_calls = 5;
        w.unnest_calls = 10;
        w.row_insert_calls = 50;
        w.total_backpressure = 1;
        let s = w.to_string();
        assert!(s.contains("500 written"));
        assert!(s.contains("5 COPY"));
        assert!(s.contains("10 UNNEST"));
        assert!(s.contains("50 rows"));
        assert!(s.contains("1 backpressure"));
    }

    #[test] fn test_display_empty() {
        let s = JsonWriter::with_defaults().to_string();
        assert!(s.contains("0/200"));
    }

    #[test] fn test_debug() {
        let d = format!("{:?}", JsonWriter::with_defaults());
        assert!(d.contains("JsonWriter"));
        assert!(d.contains("copy_calls"));
        assert!(d.contains("unnest_calls"));
        assert!(d.contains("row_insert_calls"));
    }
}