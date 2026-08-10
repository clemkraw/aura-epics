//! Query functions for `aura-api`.
//!
//! Provides read access to archived data with automatic tier selection:
//! - `< 6 hours` -> raw `samples` table
//! - `6h - 7 days` -> `samples_hourly` continuous aggregate
//! - `> 7 days` -> `samples_daily` continuous aggregate
//!
//! This keeps query latency under 100ms for any time range, from 1 minute to 10 years.

use chrono::{DateTime, Duration, Utc};
use std::fmt;

use sqlx::postgres::PgPool;

use aura_core::error::{AuraError, AuraResult};

/// Query tier - which table to read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTier {
    /// Raw samples (full resolution).
    Raw,
    /// Hourly pre-aggregated data.
    Hourly,
    /// Daily pre-aggregated data.
    Daily,
}

impl QueryTier {
    /// Automatically select the best tier for the given time range.
    ///
    /// - `< 6 hours` -> Raw
    /// - `6h - 7 days` -> Hourly
    /// - `> 7 days` -> Daily
    pub fn auto_select(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        let span = end.signed_duration_since(start);
        if span <= Duration::hours(6) {
            Self::Raw
        } else if span <= Duration::days(7) {
            Self::Hourly
        } else {
            Self::Daily
        }
    }

    /// The SQL table/view name for this tier.
    pub const fn table_name(&self) -> &'static str {
        match self {
            Self::Raw => "samples",
            Self::Hourly => "samples_hourly",
            Self::Daily => "samples_daily",
        }
    }
}

impl fmt::Display for QueryTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Raw => "raw",
            Self::Hourly => "hourly",
            Self::Daily => "daily",
        })
    }
}

/// A single data point returned by a raw query.
#[derive(Debug, Clone)]
pub struct RawSample {
    pub time: DateTime<Utc>,
    pub pv_id: i32,
    pub value: f64,
    pub severity: i16,
    pub status: i16,
}

/// An aggregated data point (hourly or daily).
#[derive(Debug, Clone)]
pub struct AggSample {
    pub bucket: DateTime<Utc>,
    pub pv_id: i32,
    pub avg_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub stddev_value: Option<f64>,
    pub sample_count: i64,
    pub first_value: Option<f64>,
    pub last_value: Option<f64>,
    pub max_severity: i16,
}

/// Parameters for a data query.
#[derive(Debug, Clone)]
pub struct QueryParams {
    /// PV name to query.
    pub pv_name: String,
    /// Start of time range (inclusive).
    pub start: DateTime<Utc>,
    /// End of time range (exclusive).
    pub end: DateTime<Utc>,
    /// Max number of points to return (0 = unlimited).
    pub limit: usize,
    /// Force a specific tier (None = auto-select).
    pub tier: Option<QueryTier>,
}

impl QueryParams {
    /// Create a simple time-range query.
    pub fn new(pv_name: impl Into<String>, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            pv_name: pv_name.into(),
            start,
            end,
            limit: 0,
            tier: None,
        }
    }

    /// Set maximum result count.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Force a specific query tier.
    pub fn with_tier(mut self, tier: QueryTier) -> Self {
        self.tier = Some(tier);
        self
    }

    /// The effective tier (explicit or auto-selected).
    pub fn effective_tier(&self) -> QueryTier {
        self.tier
            .unwrap_or_else(|| QueryTier::auto_select(self.start, self.end))
    }
}

/// Data reader - provides query functions for aura-api.
///
/// Stateless: all queries go directly to the pool.
pub struct DataReader;

impl DataReader {
    /// Query raw samples for a PV in a time range.
    pub async fn query_raw(
        pool: &PgPool,
        pv_id: i32,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
    ) -> AuraResult<Vec<RawSample>> {
        let limit_clause = if limit > 0 {
            format!("LIMIT {}", limit)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT time, pv_id, value, severity, status FROM samples \
             WHERE pv_id = $1 AND time >= $2 AND time < $3 \
             ORDER BY time ASC {}",
            limit_clause
        );
        let rows =
            sqlx::query_as::<_, (DateTime<Utc>, i32, f64, i16, i16)>(sqlx::AssertSqlSafe(&*sql))
                .bind(pv_id)
                .bind(start)
                .bind(end)
                .fetch_all(pool)
                .await
                .map_err(|e| AuraError::database(format!("raw query failed: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|(time, pv_id, value, severity, status)| RawSample {
                time,
                pv_id,
                value,
                severity,
                status,
            })
            .collect())
    }

    /// Query aggregated samples (hourly or daily) for a PV.
    pub async fn query_agg(
        pool: &PgPool,
        pv_id: i32,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        tier: QueryTier,
    ) -> AuraResult<Vec<AggSample>> {
        let table = tier.table_name();

        let sql = format!(
            "SELECT bucket, pv_id, avg_value, min_value, max_value, \
                 stddev_value, sample_count, first_value, last_value, max_severity \
                 FROM {} WHERE pv_id = $1 AND bucket >= $2 AND bucket < $3 \
                 ORDER BY bucket ASC",
            table
        );
        let rows = sqlx::query_as::<
            _,
            (
                DateTime<Utc>,
                i32,
                f64,
                f64,
                f64,
                Option<f64>,
                i64,
                Option<f64>,
                Option<f64>,
                i16,
            ),
        >(sqlx::AssertSqlSafe(&*sql))
        .bind(pv_id)
        .bind(start)
        .bind(end)
        .fetch_all(pool)
        .await
        .map_err(|e| AuraError::database(format!("{} query failed: {e}", table)))?;

        Ok(rows
            .into_iter()
            .map(
                |(bucket, pv_id, avg, min, max, stddev, count, first, last, sev)| AggSample {
                    bucket,
                    pv_id,
                    avg_value: avg,
                    min_value: min,
                    max_value: max,
                    stddev_value: stddev,
                    sample_count: count,
                    first_value: first,
                    last_value: last,
                    max_severity: sev,
                },
            )
            .collect())
    }

    /// Query with automatic tier selection.
    ///
    /// Returns either raw or aggregated data based on the time range.
    pub async fn query_auto(
        pool: &PgPool,
        pv_id: i32,
        params: &QueryParams,
    ) -> AuraResult<QueryResult> {
        let tier = params.effective_tier();
        match tier {
            QueryTier::Raw => {
                let samples =
                    Self::query_raw(pool, pv_id, params.start, params.end, params.limit).await?;
                Ok(QueryResult::Raw(samples))
            }
            QueryTier::Hourly | QueryTier::Daily => {
                let samples = Self::query_agg(pool, pv_id, params.start, params.end, tier).await?;
                Ok(QueryResult::Aggregated(samples))
            }
        }
    }

    /// Get the latest sample for a PV (current value).
    pub async fn query_latest(pool: &PgPool, pv_id: i32) -> AuraResult<Option<RawSample>> {
        let row = sqlx::query_as::<_, (DateTime<Utc>, i32, f64, i16, i16)>(
            "SELECT time, pv_id, value, severity, status FROM samples \
             WHERE pv_id = $1 ORDER BY time DESC LIMIT 1",
        )
        .bind(pv_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| AuraError::database(format!("latest query failed: {e}")))?;

        Ok(row.map(|(time, pv_id, value, severity, status)| RawSample {
            time,
            pv_id,
            value,
            severity,
            status,
        }))
    }

    /// Count total samples for a PV in a time range.
    pub async fn count_samples(
        pool: &PgPool,
        pv_id: i32,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> AuraResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM samples \
             WHERE pv_id = $1 AND time >= $2 AND time < $3",
        )
        .bind(pv_id)
        .bind(start)
        .bind(end)
        .fetch_one(pool)
        .await
        .map_err(|e| AuraError::database(format!("count query failed: {e}")))?;

        Ok(count)
    }
}

/// Result of an auto-tier query.
#[derive(Debug)]
pub enum QueryResult {
    Raw(Vec<RawSample>),
    Aggregated(Vec<AggSample>),
}

impl QueryResult {
    /// Number of data points returned.
    pub fn len(&self) -> usize {
        match self {
            Self::Raw(v) => v.len(),
            Self::Aggregated(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether this is raw data.
    pub fn is_raw(&self) -> bool {
        matches!(self, Self::Raw(_))
    }

    /// Whether this is aggregated data.
    pub fn is_aggregated(&self) -> bool {
        matches!(self, Self::Aggregated(_))
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raw(v) => write!(f, "Raw({} samples)", v.len()),
            Self::Aggregated(v) => write!(f, "Aggregated({} buckets)", v.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    #[test]
    fn test_tier_auto_short() {
        let start = t(1000);
        let end = start + Duration::hours(2);
        assert_eq!(QueryTier::auto_select(start, end), QueryTier::Raw);
    }

    #[test]
    fn test_tier_auto_6h_boundary() {
        let start = t(1000);
        let end = start + Duration::hours(6);
        assert_eq!(QueryTier::auto_select(start, end), QueryTier::Raw);
    }

    #[test]
    fn test_tier_auto_medium() {
        let start = t(1000);
        let end = start + Duration::days(3);
        assert_eq!(QueryTier::auto_select(start, end), QueryTier::Hourly);
    }

    #[test]
    fn test_tier_auto_7d_boundary() {
        let start = t(1000);
        let end = start + Duration::days(7);
        assert_eq!(QueryTier::auto_select(start, end), QueryTier::Hourly);
    }

    #[test]
    fn test_tier_auto_long() {
        let start = t(1000);
        let end = start + Duration::days(30);
        assert_eq!(QueryTier::auto_select(start, end), QueryTier::Daily);
    }

    #[test]
    fn test_tier_table_names() {
        assert_eq!(QueryTier::Raw.table_name(), "samples");
        assert_eq!(QueryTier::Hourly.table_name(), "samples_hourly");
        assert_eq!(QueryTier::Daily.table_name(), "samples_daily");
    }

    #[test]
    fn test_tier_display() {
        assert_eq!(QueryTier::Raw.to_string(), "raw");
        assert_eq!(QueryTier::Hourly.to_string(), "hourly");
        assert_eq!(QueryTier::Daily.to_string(), "daily");
    }

    #[test]
    fn test_params_new() {
        let p = QueryParams::new("CRYO:TEMP", t(1000), t(2000));
        assert_eq!(p.pv_name, "CRYO:TEMP");
        assert_eq!(p.limit, 0);
        assert!(p.tier.is_none());
    }

    #[test]
    fn test_params_with_limit() {
        let p = QueryParams::new("PV", t(1000), t(2000)).with_limit(1000);
        assert_eq!(p.limit, 1000);
    }

    #[test]
    fn test_params_with_tier() {
        let p = QueryParams::new("PV", t(1000), t(2000)).with_tier(QueryTier::Hourly);
        assert_eq!(p.tier, Some(QueryTier::Hourly));
    }

    #[test]
    fn test_params_effective_tier_auto() {
        let start = t(1000);
        let p = QueryParams::new("PV", start, start + Duration::hours(1));
        assert_eq!(p.effective_tier(), QueryTier::Raw);
    }

    #[test]
    fn test_params_effective_tier_forced() {
        let start = t(1000);
        let p =
            QueryParams::new("PV", start, start + Duration::hours(1)).with_tier(QueryTier::Daily);
        assert_eq!(p.effective_tier(), QueryTier::Daily); // forced overrides auto
    }

    #[test]
    fn test_raw_sample() {
        let s = RawSample {
            time: t(1000),
            pv_id: 42,
            value: 4.2,
            severity: 0,
            status: 0,
        };
        assert_eq!(s.pv_id, 42);
        let c = s.clone();
        assert_eq!(c.value, 4.2);
    }

    #[test]
    fn test_agg_sample() {
        let s = AggSample {
            bucket: t(1000),
            pv_id: 1,
            avg_value: 10.0,
            min_value: 5.0,
            max_value: 15.0,
            stddev_value: Some(2.5),
            sample_count: 100,
            first_value: Some(9.0),
            last_value: Some(11.0),
            max_severity: 0,
        };
        assert_eq!(s.sample_count, 100);
        let c = s.clone();
        assert_eq!(c.avg_value, 10.0);
    }

    #[test]
    fn test_result_raw() {
        let r = QueryResult::Raw(vec![
            RawSample {
                time: t(1000),
                pv_id: 1,
                value: 1.0,
                severity: 0,
                status: 0,
            },
            RawSample {
                time: t(1001),
                pv_id: 1,
                value: 2.0,
                severity: 0,
                status: 0,
            },
        ]);
        assert!(r.is_raw());
        assert!(!r.is_aggregated());
        assert_eq!(r.len(), 2);
        assert!(!r.is_empty());
    }

    #[test]
    fn test_result_aggregated() {
        let r = QueryResult::Aggregated(vec![]);
        assert!(r.is_aggregated());
        assert!(!r.is_raw());
        assert!(r.is_empty());
    }

    #[test]
    fn test_result_display() {
        let r = QueryResult::Raw(vec![RawSample {
            time: t(1000),
            pv_id: 1,
            value: 0.0,
            severity: 0,
            status: 0,
        }]);
        assert_eq!(r.to_string(), "Raw(1 samples)");

        let r = QueryResult::Aggregated(vec![]);
        assert_eq!(r.to_string(), "Aggregated(0 buckets)");
    }
}
