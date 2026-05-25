//! Batch writer for JSON-typed Normative Types.
//!
//! Routes each NT type to its optimal storage table:
//!
//! - `samples_table`  ← NTTable (JSONB, binary COPY)
//! - `samples_custom` ← NTUnion/Custom (JSONB + nt_type, binary COPY)
//! - `samples_nv`     ← NTNameValue (destructured: name TEXT + value FLOAT8)
//! - `samples_hist`   ← NTHistogram (destructured: range FLOAT8 + count BIGINT)
//! - `samples_cont`   ← NTContinuum (destructured: base FLOAT8 + trace FLOAT8)
//! - `samples_mch`    ← NTMultiChannel (destructured: ch_name TEXT + ch_value FLOAT8)
//!
//! # Write Strategy
//!
//! All writes go through the shared [`CopyPool`]. Table/Custom use text COPY
//! for JSONB. The 4 destructured tables use binary COPY for maximum
//! throughput and TimescaleDB gorilla compression.
//!
//! # Compression (destructured tables)
//!
//! ```text
//! pv_id:      dictionary  → ~0 bits
//! time:       delta       → ~0 bits (same for all elements in one capture)
//! capture_id: delta       → ~0 bits
//! idx:        delta       → ~1 bit  (0,1,2,3...)
//! value:      gorilla     → ~4-8 bits
//! Total: ~0.6-1.1 bytes/element vs 8+ bytes in PG arrays
//! ```

use chrono::{DateTime, Utc};
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

use super::copy_pool::{CopyPool, PG_EPOCH_OFFSET_US, PGCOPY_HEADER, PGCOPY_TRAILER, PushResult};
use aura_core::error::{AuraError, AuraResult};

const DEFAULT_MAX_BUFFER_BYTES: usize = 32 * 1024 * 1024;

/// Binary COPY for `samples_table` (JSONB).
const COPY_TABLE: &str =
    "COPY samples_table (time, pv_id, data, severity, status) FROM STDIN WITH (FORMAT binary)";

/// Binary COPY for `samples_custom` (JSONB + nt_type).
const COPY_CUSTOM: &str = "COPY samples_custom (time, pv_id, nt_type, data, severity, status) FROM STDIN WITH (FORMAT binary)";

/// Binary COPY for destructured tables.
const COPY_NV: &str = "COPY samples_nv (time, pv_id, capture_id, idx, name, value, severity, status) FROM STDIN WITH (FORMAT binary)";
const COPY_HIST: &str = "COPY samples_hist (time, pv_id, capture_id, idx, range_val, count, severity, status) FROM STDIN WITH (FORMAT binary)";
const COPY_CONT: &str = "COPY samples_cont (time, pv_id, capture_id, idx, base_val, trace_val, severity, status) FROM STDIN WITH (FORMAT binary)";
const COPY_MCH: &str = "COPY samples_mch (time, pv_id, capture_id, idx, ch_name, ch_value, ch_severity, severity, status) FROM STDIN WITH (FORMAT binary)";

/// Global capture_id counter (shared with array.rs via same pattern).
static NEXT_CAPTURE_ID: AtomicI64 = AtomicI64::new(1);
fn next_capture_id() -> i64 {
    NEXT_CAPTURE_ID.fetch_add(1, Ordering::Relaxed)
}

/// Estimate serialized size of a `serde_json::Value` WITHOUT allocating.
pub fn estimate_json_size(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(b) => {
            if *b {
                4
            } else {
                5
            }
        }
        serde_json::Value::Number(_) => 8,
        serde_json::Value::String(s) => s.len() + 2,
        serde_json::Value::Array(a) => {
            2 + a.iter().map(estimate_json_size).sum::<usize>() + a.len().saturating_sub(1)
        }
        serde_json::Value::Object(o) => {
            2 + o
                .iter()
                .map(|(k, v)| k.len() + 3 + estimate_json_size(v))
                .sum::<usize>()
                + o.len().saturating_sub(1)
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
            Self::Table => "samples_table",
            Self::Custom => "samples_custom",
            Self::NameValue => "samples_nv",
            Self::Histogram => "samples_hist",
            Self::Continuum => "samples_cont",
            Self::Multi => "samples_mch",
        }
    }

    /// Whether this table uses text COPY (JSONB). False = destructured binary COPY.
    pub const fn is_jsonb(&self) -> bool {
        matches!(self, Self::Table | Self::Custom)
    }

    pub const ALL: [Self; 6] = [
        Self::Table,
        Self::Custom,
        Self::NameValue,
        Self::Histogram,
        Self::Continuum,
        Self::Multi,
    ];

    const fn index(&self) -> usize {
        match self {
            Self::Table => 0,
            Self::Custom => 1,
            Self::NameValue => 2,
            Self::Histogram => 3,
            Self::Continuum => 4,
            Self::Multi => 5,
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
    estimated_size: usize,
}

impl JsonRow {
    #[inline]
    fn create(
        time: DateTime<Utc>,
        pv_id: i32,
        table: JsonTable,
        data: serde_json::Value,
        nt_type: Option<String>,
        severity: i16,
        status: i16,
    ) -> Self {
        let estimated_size =
            64 + estimate_json_size(&data) + nt_type.as_ref().map_or(0, |s| 24 + s.len());
        Self {
            time,
            pv_id,
            table,
            data,
            nt_type,
            severity,
            status,
            estimated_size,
        }
    }

    pub fn table(
        time: DateTime<Utc>,
        pv_id: i32,
        data: serde_json::Value,
        sev: i16,
        status: i16,
    ) -> Self {
        Self::create(time, pv_id, JsonTable::Table, data, None, sev, status)
    }
    pub fn custom(
        time: DateTime<Utc>,
        pv_id: i32,
        nt_type: &str,
        data: serde_json::Value,
        sev: i16,
        status: i16,
    ) -> Self {
        Self::create(
            time,
            pv_id,
            JsonTable::Custom,
            data,
            Some(nt_type.to_string()),
            sev,
            status,
        )
    }
    pub fn namevalue(
        time: DateTime<Utc>,
        pv_id: i32,
        data: serde_json::Value,
        sev: i16,
        status: i16,
    ) -> Self {
        Self::create(time, pv_id, JsonTable::NameValue, data, None, sev, status)
    }
    pub fn histogram(
        time: DateTime<Utc>,
        pv_id: i32,
        data: serde_json::Value,
        sev: i16,
        status: i16,
    ) -> Self {
        Self::create(time, pv_id, JsonTable::Histogram, data, None, sev, status)
    }
    pub fn continuum(
        time: DateTime<Utc>,
        pv_id: i32,
        data: serde_json::Value,
        sev: i16,
        status: i16,
    ) -> Self {
        Self::create(time, pv_id, JsonTable::Continuum, data, None, sev, status)
    }
    pub fn multi(
        time: DateTime<Utc>,
        pv_id: i32,
        data: serde_json::Value,
        sev: i16,
        status: i16,
    ) -> Self {
        Self::create(time, pv_id, JsonTable::Multi, data, None, sev, status)
    }

    #[inline]
    pub fn mem_size(&self) -> usize {
        self.estimated_size
    }
}

impl fmt::Display for JsonRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pv_id={} table={} (~{}B)",
            self.pv_id, self.table, self.estimated_size
        )
    }
}

/// Batch writer for all JSON-typed Normative Types.
pub struct JsonWriter {
    batch_size: usize,
    max_buffer_bytes: usize,
    buffer: Vec<JsonRow>,
    current_bytes: usize,
    copy_pool: Option<CopyPool>,

    total_written: u64,
    total_flushes: u64,
    total_json_bytes: u64,
    total_backpressure: u64,
    per_table_written: [u64; 6],
    copy_calls: u64,
    total_build_us: u64,
    total_send_us: u64,
}

impl JsonWriter {
    pub fn new(batch_size: usize) -> Self {
        Self::with_limits(batch_size, DEFAULT_MAX_BUFFER_BYTES)
    }

    pub fn with_limits(batch_size: usize, max_buffer_bytes: usize) -> Self {
        let batch_size = batch_size.max(1);
        Self {
            batch_size,
            max_buffer_bytes: max_buffer_bytes.max(64),
            buffer: Vec::with_capacity(batch_size),
            current_bytes: 0,
            copy_pool: None,
            total_written: 0,
            total_flushes: 0,
            total_json_bytes: 0,
            total_backpressure: 0,
            per_table_written: [0; 6],
            copy_calls: 0,
            total_build_us: 0,
            total_send_us: 0,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(200)
    }

    pub fn set_copy_pool(&mut self, pool: CopyPool) {
        self.copy_pool = Some(pool);
    }

    #[inline]
    pub fn push(&mut self, row: JsonRow) -> PushResult {
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

    /// Flush all buffered rows. Groups by table, builds payloads, sends.
    ///
    /// - Table/Custom: text COPY (JSONB)
    /// - NameValue/Histogram/Continuum/Multi: binary COPY (destructured)
    pub async fn flush(&mut self) -> AuraResult<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }
        let count = self.buffer.len();

        let pool = self
            .copy_pool
            .clone()
            .ok_or_else(|| AuraError::database("JsonWriter: no copy pool configured"))?;

        let t0 = std::time::Instant::now();
        let table_payload = Self::build_table_payload(&self.buffer);
        let custom_payload = Self::build_custom_payload(&self.buffer);
        let nv_payload = Self::build_nv_payload(&self.buffer);
        let hist_payload = Self::build_hist_payload(&self.buffer);
        let cont_payload = Self::build_cont_payload(&self.buffer);
        let mch_payload = Self::build_mch_payload(&self.buffer);
        self.total_build_us += t0.elapsed().as_micros() as u64;

        let t1 = std::time::Instant::now();
        if let Some((payload, n)) = table_payload {
            pool.send_copy(0, COPY_TABLE, payload, n).await?;
            self.copy_calls += 1;
        }
        if let Some((payload, n)) = custom_payload {
            pool.send_copy(0, COPY_CUSTOM, payload, n).await?;
            self.copy_calls += 1;
        }
        if let Some((payload, n)) = nv_payload {
            pool.send_copy(1 % pool.len(), COPY_NV, payload, n).await?;
            self.copy_calls += 1;
        }
        if let Some((payload, n)) = hist_payload {
            pool.send_copy(2 % pool.len(), COPY_HIST, payload, n)
                .await?;
            self.copy_calls += 1;
        }
        if let Some((payload, n)) = cont_payload {
            pool.send_copy(3 % pool.len(), COPY_CONT, payload, n)
                .await?;
            self.copy_calls += 1;
        }
        if let Some((payload, n)) = mch_payload {
            pool.send_copy(4 % pool.len(), COPY_MCH, payload, n).await?;
            self.copy_calls += 1;
        }
        self.total_send_us += t1.elapsed().as_micros() as u64;

        // Stats.
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

    /// samples_table: 5 columns (time, pv_id, data JSONB, severity, status)
    fn build_table_payload(buffer: &[JsonRow]) -> Option<(Vec<u8>, usize)> {
        let rows: Vec<&JsonRow> = buffer
            .iter()
            .filter(|r| r.table == JsonTable::Table)
            .collect();
        if rows.is_empty() {
            return None;
        }
        let count = rows.len();
        let mut buf = Vec::with_capacity(PGCOPY_HEADER.len() + count * 256 + PGCOPY_TRAILER.len());
        buf.extend_from_slice(&PGCOPY_HEADER);

        for row in rows {
            let pg_us = row.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
            let json_bytes = serde_json::to_vec(&row.data).unwrap_or_else(|_| b"{}".to_vec());

            buf.extend_from_slice(&5i16.to_be_bytes()); // num_columns
            // time
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&pg_us.to_be_bytes());
            // pv_id
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&row.pv_id.to_be_bytes());
            // data (JSONB) = version_byte + json_text
            let jsonb_len = 1 + json_bytes.len();
            buf.extend_from_slice(&(jsonb_len as i32).to_be_bytes());
            buf.push(0x01); // JSONB version byte
            buf.extend_from_slice(&json_bytes);
            // severity
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&row.severity.to_be_bytes());
            // status
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&row.status.to_be_bytes());
        }

        buf.extend_from_slice(&PGCOPY_TRAILER);
        Some((buf, count))
    }

    /// samples_custom: 6 columns (time, pv_id, nt_type TEXT, data JSONB, severity, status)
    fn build_custom_payload(buffer: &[JsonRow]) -> Option<(Vec<u8>, usize)> {
        let rows: Vec<&JsonRow> = buffer
            .iter()
            .filter(|r| r.table == JsonTable::Custom)
            .collect();
        if rows.is_empty() {
            return None;
        }
        let count = rows.len();
        let mut buf = Vec::with_capacity(PGCOPY_HEADER.len() + count * 300 + PGCOPY_TRAILER.len());
        buf.extend_from_slice(&PGCOPY_HEADER);

        for row in rows {
            let pg_us = row.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
            let nt_bytes = row.nt_type.as_deref().unwrap_or("Custom").as_bytes();
            let json_bytes = serde_json::to_vec(&row.data).unwrap_or_else(|_| b"{}".to_vec());

            buf.extend_from_slice(&6i16.to_be_bytes()); // num_columns
            // time
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&pg_us.to_be_bytes());
            // pv_id
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&row.pv_id.to_be_bytes());
            // nt_type (TEXT)
            buf.extend_from_slice(&(nt_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(nt_bytes);
            // data (JSONB)
            let jsonb_len = 1 + json_bytes.len();
            buf.extend_from_slice(&(jsonb_len as i32).to_be_bytes());
            buf.push(0x01);
            buf.extend_from_slice(&json_bytes);
            // severity
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&row.severity.to_be_bytes());
            // status
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&row.status.to_be_bytes());
        }

        buf.extend_from_slice(&PGCOPY_TRAILER);
        Some((buf, count))
    }

    /// NV: (time, pv_id, capture_id, idx, name TEXT, value f64, severity, status)
    /// 8 columns. Fixed part per element = 2+12+8+12+6+(4+N)+12+6+6 = 68+N
    fn build_nv_payload(buffer: &[JsonRow]) -> Option<(Vec<u8>, usize)> {
        let rows: Vec<&JsonRow> = buffer
            .iter()
            .filter(|r| r.table == JsonTable::NameValue)
            .collect();
        if rows.is_empty() {
            return None;
        }

        let mut buf = Vec::with_capacity(PGCOPY_HEADER.len() + 4096 + PGCOPY_TRAILER.len());
        buf.extend_from_slice(&PGCOPY_HEADER);
        let mut elem_count = 0usize;

        for row in rows {
            let cid = next_capture_id();
            let pg_us = row.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
            let names = row.data.get("name").and_then(|v| v.as_array());
            let values = row.data.get("value").and_then(|v| v.as_array());
            if names.is_none() || values.is_none() {
                continue;
            }
            let names = names.unwrap();
            let values = values.unwrap();
            let n = names.len().min(values.len());

            for i in 0..n {
                let name_str = names[i].as_str().unwrap_or("");
                let val = values[i].as_f64().unwrap_or(0.0);
                let name_bytes = name_str.as_bytes();

                buf.extend_from_slice(&8i16.to_be_bytes()); // num_columns
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&pg_us.to_be_bytes());
                buf.extend_from_slice(&4i32.to_be_bytes());
                buf.extend_from_slice(&row.pv_id.to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&cid.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&(i as i16).to_be_bytes());
                buf.extend_from_slice(&(name_bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(name_bytes);
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&val.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&row.severity.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&row.status.to_be_bytes());
                elem_count += 1;
            }
        }

        buf.extend_from_slice(&PGCOPY_TRAILER);
        if elem_count == 0 {
            return None;
        }
        Some((buf, elem_count))
    }

    /// HIST: (time, pv_id, capture_id, idx, range_val f64, count i64, severity, status)
    /// 8 columns, all fixed-width. 2+12+8+12+6+12+12+6+6 = 76 bytes/elem
    fn build_hist_payload(buffer: &[JsonRow]) -> Option<(Vec<u8>, usize)> {
        let rows: Vec<&JsonRow> = buffer
            .iter()
            .filter(|r| r.table == JsonTable::Histogram)
            .collect();
        if rows.is_empty() {
            return None;
        }

        let mut buf = Vec::with_capacity(PGCOPY_HEADER.len() + 4096 + PGCOPY_TRAILER.len());
        buf.extend_from_slice(&PGCOPY_HEADER);
        let mut elem_count = 0usize;

        for row in rows {
            let cid = next_capture_id();
            let pg_us = row.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
            let ranges = row.data.get("ranges").and_then(|v| v.as_array());
            let counts = row
                .data
                .get("value")
                .and_then(|v| v.as_array())
                .or_else(|| row.data.get("counts").and_then(|v| v.as_array()));
            if ranges.is_none() || counts.is_none() {
                continue;
            }
            let ranges = ranges.unwrap();
            let counts = counts.unwrap();
            let n = ranges.len().min(counts.len());

            for i in 0..n {
                let range_val = ranges[i].as_f64().unwrap_or(0.0);
                let count_val = counts[i].as_i64().unwrap_or(0);

                buf.extend_from_slice(&8i16.to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&pg_us.to_be_bytes());
                buf.extend_from_slice(&4i32.to_be_bytes());
                buf.extend_from_slice(&row.pv_id.to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&cid.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&(i as i16).to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&range_val.to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&count_val.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&row.severity.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&row.status.to_be_bytes());
                elem_count += 1;
            }
        }

        buf.extend_from_slice(&PGCOPY_TRAILER);
        if elem_count == 0 {
            return None;
        }
        Some((buf, elem_count))
    }

    /// CONT: (time, pv_id, capture_id, idx, base_val f64, trace_val f64, severity, status)
    /// 8 columns, all fixed-width. 76 bytes/elem (same as hist).
    fn build_cont_payload(buffer: &[JsonRow]) -> Option<(Vec<u8>, usize)> {
        let rows: Vec<&JsonRow> = buffer
            .iter()
            .filter(|r| r.table == JsonTable::Continuum)
            .collect();
        if rows.is_empty() {
            return None;
        }

        let mut buf = Vec::with_capacity(PGCOPY_HEADER.len() + 4096 + PGCOPY_TRAILER.len());
        buf.extend_from_slice(&PGCOPY_HEADER);
        let mut elem_count = 0usize;

        for row in rows {
            let cid = next_capture_id();
            let pg_us = row.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
            let base = row.data.get("base").and_then(|v| v.as_array());
            let values = row.data.get("value").and_then(|v| v.as_array());
            if base.is_none() || values.is_none() {
                continue;
            }
            let base = base.unwrap();
            let values = values.unwrap();
            let n = base.len().min(values.len());

            for i in 0..n {
                let base_val = base[i].as_f64().unwrap_or(0.0);
                let trace_val = values[i].as_f64().unwrap_or(0.0);

                buf.extend_from_slice(&8i16.to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&pg_us.to_be_bytes());
                buf.extend_from_slice(&4i32.to_be_bytes());
                buf.extend_from_slice(&row.pv_id.to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&cid.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&(i as i16).to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&base_val.to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&trace_val.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&row.severity.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&row.status.to_be_bytes());
                elem_count += 1;
            }
        }

        buf.extend_from_slice(&PGCOPY_TRAILER);
        if elem_count == 0 {
            return None;
        }
        Some((buf, elem_count))
    }

    /// MCH: (time, pv_id, capture_id, idx, ch_name TEXT, ch_value f64, ch_severity i16, severity, status)
    /// 9 columns. Variable-length due to ch_name.
    fn build_mch_payload(buffer: &[JsonRow]) -> Option<(Vec<u8>, usize)> {
        let rows: Vec<&JsonRow> = buffer
            .iter()
            .filter(|r| r.table == JsonTable::Multi)
            .collect();
        if rows.is_empty() {
            return None;
        }

        let mut buf = Vec::with_capacity(PGCOPY_HEADER.len() + 4096 + PGCOPY_TRAILER.len());
        buf.extend_from_slice(&PGCOPY_HEADER);
        let mut elem_count = 0usize;

        for row in rows {
            let cid = next_capture_id();
            let pg_us = row.time.timestamp_micros() - PG_EPOCH_OFFSET_US;
            let names = row.data.get("channel_names").and_then(|v| v.as_array());
            let values = row.data.get("channel_values").and_then(|v| v.as_array());
            let sevs = row.data.get("severities").and_then(|v| v.as_array());
            if names.is_none() || values.is_none() {
                continue;
            }
            let names = names.unwrap();
            let values = values.unwrap();
            let n = names.len().min(values.len());

            for i in 0..n {
                let ch_name = names[i].as_str().unwrap_or("");
                let ch_val = values[i].as_f64().unwrap_or(0.0);
                let ch_sev = sevs
                    .and_then(|s| s.get(i))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i16;
                let name_bytes = ch_name.as_bytes();

                buf.extend_from_slice(&9i16.to_be_bytes()); // 9 columns
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&pg_us.to_be_bytes());
                buf.extend_from_slice(&4i32.to_be_bytes());
                buf.extend_from_slice(&row.pv_id.to_be_bytes());
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&cid.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&(i as i16).to_be_bytes());
                buf.extend_from_slice(&(name_bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(name_bytes);
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&ch_val.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&ch_sev.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&row.severity.to_be_bytes());
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&row.status.to_be_bytes());
                elem_count += 1;
            }
        }

        buf.extend_from_slice(&PGCOPY_TRAILER);
        if elem_count == 0 {
            return None;
        }
        Some((buf, elem_count))
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
    pub fn total_json_bytes(&self) -> u64 {
        self.total_json_bytes
    }
    #[inline]
    pub fn total_backpressure(&self) -> u64 {
        self.total_backpressure
    }
    #[inline]
    pub fn copy_calls(&self) -> u64 {
        self.copy_calls
    }
    #[inline]
    pub fn total_build_us(&self) -> u64 {
        self.total_build_us
    }
    #[inline]
    pub fn total_send_us(&self) -> u64 {
        self.total_send_us
    }

    // Compat
    #[inline]
    pub fn row_insert_calls(&self) -> u64 {
        0
    }

    pub fn memory_pressure(&self) -> f64 {
        if self.max_buffer_bytes == 0 {
            return 0.0;
        }
        self.current_bytes as f64 / self.max_buffer_bytes as f64
    }

    pub fn avg_json_bytes(&self) -> f64 {
        if self.total_written == 0 {
            return 0.0;
        }
        self.total_json_bytes as f64 / self.total_written as f64
    }

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

    pub fn written_by_table(&self, table: JsonTable) -> u64 {
        self.per_table_written[table.index()]
    }

    pub fn discard(&mut self) {
        self.buffer.clear();
        self.current_bytes = 0;
    }
}

impl fmt::Debug for JsonWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonWriter")
            .field("buffered", &self.buffer.len())
            .field(
                "pressure",
                &format_args!("{:.1}%", self.memory_pressure() * 100.0),
            )
            .field("written", &self.total_written)
            .field("copy_calls", &self.copy_calls)
            .field("backpressure", &self.total_backpressure)
            .finish()
    }
}

impl fmt::Display for JsonWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JsonWriter: {}/{} ({:.1}KB/{:.1}MB, {:.0}%), {} written (avg {:.0}B), \
             {} flushes, {} COPY calls, {} backpressure",
            self.buffer.len(),
            self.batch_size,
            self.current_bytes as f64 / 1024.0,
            self.max_buffer_bytes as f64 / (1024.0 * 1024.0),
            self.memory_pressure() * 100.0,
            self.total_written,
            self.avg_json_bytes(),
            self.total_flushes,
            self.copy_calls,
            self.total_backpressure
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    // ── estimate_json_size ───────────────────────────────────────

    #[test]
    fn est_null() {
        assert_eq!(estimate_json_size(&json!(null)), 4);
    }
    #[test]
    fn est_true() {
        assert_eq!(estimate_json_size(&json!(true)), 4);
    }
    #[test]
    fn est_false() {
        assert_eq!(estimate_json_size(&json!(false)), 5);
    }
    #[test]
    fn est_number() {
        assert_eq!(estimate_json_size(&json!(42)), 8);
    }
    #[test]
    fn est_string() {
        assert_eq!(estimate_json_size(&json!("hello")), 7);
    }
    #[test]
    fn est_array() {
        assert_eq!(estimate_json_size(&json!([1, 2, 3])), 28);
    }
    #[test]
    fn est_object() {
        assert_eq!(estimate_json_size(&json!({"a": 1})), 14);
    }

    #[test]
    fn est_vs_actual() {
        let values = vec![
            json!({"names": ["CRYO:T1", "CRYO:T2"], "values": [4.217, 2.003]}),
            json!({"ranges": [0.0, 1.0, 2.0], "counts": [100, 200, 150]}),
        ];
        for v in &values {
            let est = estimate_json_size(v);
            let actual = serde_json::to_string(v).unwrap().len();
            assert!(
                est >= actual / 2 && est < actual * 5,
                "est={est} actual={actual}"
            );
        }
    }

    // ── JsonTable ────────────────────────────────────────────────

    #[test]
    fn table_names() {
        assert_eq!(JsonTable::Table.table_name(), "samples_table");
        assert_eq!(JsonTable::Custom.table_name(), "samples_custom");
        assert_eq!(JsonTable::NameValue.table_name(), "samples_nv");
        assert_eq!(JsonTable::Histogram.table_name(), "samples_hist");
        assert_eq!(JsonTable::Continuum.table_name(), "samples_cont");
        assert_eq!(JsonTable::Multi.table_name(), "samples_mch");
    }

    #[test]
    fn table_is_jsonb() {
        assert!(JsonTable::Table.is_jsonb());
        assert!(JsonTable::Custom.is_jsonb());
        assert!(!JsonTable::NameValue.is_jsonb());
        assert!(!JsonTable::Histogram.is_jsonb());
        assert!(!JsonTable::Continuum.is_jsonb());
        assert!(!JsonTable::Multi.is_jsonb());
    }

    #[test]
    fn table_all_count() {
        assert_eq!(JsonTable::ALL.len(), 6);
    }

    // ── JsonRow ──────────────────────────────────────────────────

    #[test]
    fn row_constructors() {
        assert_eq!(
            JsonRow::table(now(), 1, json!({}), 0, 0).table,
            JsonTable::Table
        );
        assert_eq!(
            JsonRow::custom(now(), 1, "X", json!({}), 0, 0).table,
            JsonTable::Custom
        );
        assert_eq!(
            JsonRow::namevalue(now(), 1, json!({}), 0, 0).table,
            JsonTable::NameValue
        );
        assert_eq!(
            JsonRow::histogram(now(), 1, json!({}), 0, 0).table,
            JsonTable::Histogram
        );
        assert_eq!(
            JsonRow::continuum(now(), 1, json!({}), 0, 0).table,
            JsonTable::Continuum
        );
        assert_eq!(
            JsonRow::multi(now(), 1, json!({}), 0, 0).table,
            JsonTable::Multi
        );
    }

    #[test]
    fn row_custom_nt_type() {
        let r = JsonRow::custom(now(), 1, "NTUnion", json!({}), 0, 0);
        assert_eq!(r.nt_type.as_deref(), Some("NTUnion"));
    }

    #[test]
    fn row_mem_size_scales() {
        let s = JsonRow::table(now(), 1, json!({}), 0, 0);
        let l = JsonRow::table(now(), 1, json!({"a": [1,2,3,4,5]}), 0, 0);
        assert!(l.mem_size() > s.mem_size());
    }

    // ── JsonWriter construction ──────────────────────────────────

    #[test]
    fn writer_defaults() {
        let w = JsonWriter::with_defaults();
        assert!(w.is_empty());
        assert_eq!(w.batch_size(), 200);
        assert_eq!(w.copy_calls(), 0);
    }

    #[test]
    fn min_batch() {
        assert_eq!(JsonWriter::new(0).batch_size(), 1);
    }

    // ── Push ─────────────────────────────────────────────────────

    #[test]
    fn push_one() {
        let mut w = JsonWriter::with_defaults();
        assert_eq!(
            w.push(JsonRow::table(now(), 1, json!({}), 0, 0)),
            PushResult::Accepted
        );
        assert_eq!(w.buffered(), 1);
    }

    #[test]
    fn push_until_full() {
        let mut w = JsonWriter::new(3);
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        w.push(JsonRow::table(now(), 2, json!({}), 0, 0));
        assert_eq!(
            w.push(JsonRow::table(now(), 3, json!({}), 0, 0)),
            PushResult::Full
        );
    }

    #[test]
    fn push_mixed() {
        let mut w = JsonWriter::new(100);
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        w.push(JsonRow::custom(now(), 2, "X", json!({}), 0, 0));
        w.push(JsonRow::namevalue(now(), 3, json!({}), 0, 0));
        w.push(JsonRow::histogram(now(), 4, json!({}), 0, 0));
        w.push(JsonRow::continuum(now(), 5, json!({}), 0, 0));
        w.push(JsonRow::multi(now(), 6, json!({}), 0, 0));
        assert_eq!(w.buffered(), 6);
    }

    // ── Backpressure ─────────────────────────────────────────────

    #[test]
    fn backpressure() {
        let mut w = JsonWriter::with_limits(100, 200);
        let mut hit = false;
        for i in 0..50 {
            if w.push(JsonRow::table(now(), i, json!({}), 0, 0)) == PushResult::BackpressureExceeded
            {
                hit = true;
                break;
            }
        }
        assert!(hit);
    }

    #[test]
    fn backpressure_first_accepted() {
        let mut w = JsonWriter::with_limits(100, 1);
        assert!(
            w.push(JsonRow::table(now(), 1, json!({"big": "data"}), 0, 0))
                .is_accepted()
        );
    }

    // ── Discard ──────────────────────────────────────────────────

    #[test]
    fn discard() {
        let mut w = JsonWriter::with_defaults();
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        w.discard();
        assert!(w.is_empty());
        assert_eq!(w.buffered_bytes(), 0);
    }

    // ── Counts ───────────────────────────────────────────────────

    #[test]
    fn counts_by_table() {
        let mut w = JsonWriter::new(100);
        w.push(JsonRow::table(now(), 1, json!({}), 0, 0));
        w.push(JsonRow::table(now(), 2, json!({}), 0, 0));
        w.push(JsonRow::histogram(now(), 3, json!({}), 0, 0));
        let c = w.counts_by_table();
        assert_eq!(c[0].1, 2);
        assert_eq!(c[3].1, 1);
    }

    // ── Binary payload builders ──────────────────────────────────

    #[test]
    fn nv_payload_empty() {
        assert!(JsonWriter::build_nv_payload(&[]).is_none());
    }

    #[test]
    fn nv_payload_with_data() {
        let row = JsonRow::namevalue(
            now(),
            1,
            json!({"name": ["T1", "T2"], "value": [4.2, 2.0]}),
            0,
            0,
        );
        let result = JsonWriter::build_nv_payload(&[row]);
        assert!(result.is_some());
        let (buf, count) = result.unwrap();
        assert_eq!(count, 2); // 2 elements
        assert!(buf.starts_with(&PGCOPY_HEADER));
        assert!(buf.ends_with(&PGCOPY_TRAILER));
    }

    #[test]
    fn hist_payload_with_data() {
        let row = JsonRow::histogram(
            now(),
            1,
            json!({"ranges": [0.0, 1.0, 2.0], "counts": [100, 200, 150]}),
            0,
            0,
        );
        let result = JsonWriter::build_hist_payload(&[row]);
        assert!(result.is_some());
        let (_, count) = result.unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn cont_payload_with_data() {
        let row = JsonRow::continuum(
            now(),
            1,
            json!({"base": [0.0, 1.0], "value": [10.0, 20.0]}),
            0,
            0,
        );
        let result = JsonWriter::build_cont_payload(&[row]);
        assert!(result.is_some());
        let (_, count) = result.unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn mch_payload_with_data() {
        let row = JsonRow::multi(
            now(),
            1,
            json!({"channel_names": ["PV:A", "PV:B"], "channel_values": [1.0, 2.0], "severities": [0, 1]}),
            0,
            0,
        );
        let result = JsonWriter::build_mch_payload(&[row]);
        assert!(result.is_some());
        let (_, count) = result.unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn nv_payload_skips_bad_data() {
        let row = JsonRow::namevalue(now(), 1, json!({"wrong": "format"}), 0, 0);
        assert!(JsonWriter::build_nv_payload(&[row]).is_none());
    }

    #[test]
    fn binary_copy_table() {
        let row = JsonRow::table(now(), 42, json!({"x": 1}), 0, 0);
        let result = JsonWriter::build_table_payload(&[row]);
        assert!(result.is_some());
        let (buf, count) = result.unwrap();
        assert_eq!(count, 1);
        assert!(buf.starts_with(&PGCOPY_HEADER));
        assert!(buf.ends_with(&PGCOPY_TRAILER));
    }

    #[test]
    fn binary_copy_custom() {
        let row = JsonRow::custom(now(), 1, "NTUnion", json!({}), 0, 0);
        let result = JsonWriter::build_custom_payload(&[row]);
        assert!(result.is_some());
        let (buf, _) = result.unwrap();
        assert!(buf.starts_with(&PGCOPY_HEADER));
        assert!(buf.ends_with(&PGCOPY_TRAILER));
    }

    // ── SQL constants ────────────────────────────────────────────

    #[test]
    fn sql_copy_table() {
        assert!(COPY_TABLE.contains("samples_table"));
    }
    #[test]
    fn sql_copy_custom() {
        assert!(COPY_CUSTOM.contains("samples_custom"));
    }
    #[test]
    fn sql_copy_nv() {
        assert!(COPY_NV.contains("samples_nv") && COPY_NV.contains("FORMAT binary"));
    }
    #[test]
    fn sql_copy_hist() {
        assert!(COPY_HIST.contains("samples_hist"));
    }
    #[test]
    fn sql_copy_cont() {
        assert!(COPY_CONT.contains("samples_cont"));
    }
    #[test]
    fn sql_copy_mch() {
        assert!(COPY_MCH.contains("samples_mch"));
    }

    // ── Display / Debug ──────────────────────────────────────────

    #[test]
    fn display() {
        let s = JsonWriter::with_defaults().to_string();
        assert!(s.contains("JsonWriter") && s.contains("0/200"));
    }

    #[test]
    fn debug() {
        let d = format!("{:?}", JsonWriter::with_defaults());
        assert!(d.contains("JsonWriter") && d.contains("copy_calls"));
    }
}