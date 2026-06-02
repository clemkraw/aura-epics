//! CRUD operations on the `pv_config` table.
//!
//! Provides functions to list, insert, update, and delete PV configurations.

use chrono::{DateTime, Utc};
use std::fmt;

use sqlx::postgres::PgPool;

use aura_core::error::{AuraError, AuraResult};
use aura_core::pv::PvConfig;

mod sql {
    /// Column list reused across all SELECT queries (DRY).
    pub const COLUMNS: &str = "pv_name, description, unit, epsilon, heartbeat_s, \
         expected_ioc, shard_id, enabled, created_at, updated_at";

    pub const GET: &str = "SELECT pv_name, description, unit, epsilon, heartbeat_s, \
         expected_ioc, shard_id, enabled, created_at, updated_at \
         FROM pv_config WHERE pv_name = $1";

    pub const GET_ALL_ENABLED: &str = "SELECT pv_name, description, unit, epsilon, heartbeat_s, \
         expected_ioc, shard_id, enabled, created_at, updated_at \
         FROM pv_config WHERE enabled = TRUE ORDER BY pv_name";

    pub const GET_CHANGED: &str = "SELECT pv_name, description, unit, epsilon, heartbeat_s, \
         expected_ioc, shard_id, enabled, created_at, updated_at \
         FROM pv_config WHERE updated_at > $1 ORDER BY updated_at";

    pub const GET_BY_SHARD: &str = "SELECT pv_name, description, unit, epsilon, heartbeat_s, \
         expected_ioc, shard_id, enabled, created_at, updated_at \
         FROM pv_config WHERE shard_id = $1 AND enabled = TRUE ORDER BY pv_name";

    pub const GET_BY_IOC: &str = "SELECT pv_name, description, unit, epsilon, heartbeat_s, \
         expected_ioc, shard_id, enabled, created_at, updated_at \
         FROM pv_config WHERE expected_ioc = $1 ORDER BY pv_name";

    pub const INSERT: &str = "INSERT INTO pv_config (pv_name, description, unit, epsilon, heartbeat_s, enabled) \
         VALUES ($1, $2, $3, $4, $5, $6)";

    pub const UPSERT: &str = "INSERT INTO pv_config (pv_name, description, unit, epsilon, heartbeat_s, enabled) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (pv_name) DO UPDATE SET \
             description = EXCLUDED.description, \
             unit = EXCLUDED.unit, \
             epsilon = EXCLUDED.epsilon, \
             heartbeat_s = EXCLUDED.heartbeat_s, \
             enabled = EXCLUDED.enabled, \
             updated_at = NOW()";

    pub const UPDATE_FILTER: &str = "UPDATE pv_config SET epsilon = $1, heartbeat_s = $2, updated_at = NOW() \
         WHERE pv_name = $3";

    pub const SET_ENABLED: &str =
        "UPDATE pv_config SET enabled = $1, updated_at = NOW() WHERE pv_name = $2";

    pub const ASSIGN_SHARD: &str =
        "UPDATE pv_config SET shard_id = $1, updated_at = NOW() WHERE pv_name = $2";

    pub const DELETE: &str = "DELETE FROM pv_config WHERE pv_name = $1";

    pub const COUNT_ALL: &str = "SELECT COUNT(*) FROM pv_config";

    pub const COUNT_ENABLED: &str = "SELECT COUNT(*) FROM pv_config WHERE enabled = TRUE";

    pub const SEARCH: &str = "SELECT pv_name, description, unit, epsilon, heartbeat_s, \
         expected_ioc, shard_id, enabled, created_at, updated_at \
         FROM pv_config WHERE pv_name ILIKE $1 ORDER BY pv_name LIMIT $2";
}

/// Validate and clamp config values before insert/upsert.
fn validate_config(config: &PvConfig) -> (Option<f64>, f64) {
    let epsilon = config.epsilon.map(|e| e.max(0.0));
    let heartbeat = config.heartbeat_s.max(1.0);
    (epsilon, heartbeat)
}

/// PV config data access object.
pub struct PvConfigDao;

impl PvConfigDao {
    /// Get a single PV configuration by name.
    pub async fn get(pool: &PgPool, pv_name: &str) -> AuraResult<Option<PvConfig>> {
        let row = sqlx::query_as::<_, PvConfigRow>(sql::GET)
            .bind(pv_name)
            .fetch_optional(pool)
            .await
            .map_err(|e| AuraError::database(format!("get pv_config: {e}")))?;
        Ok(row.map(PvConfigRow::into_pv_config))
    }

    /// Get all enabled PV configurations.
    pub async fn get_all_enabled(pool: &PgPool) -> AuraResult<Vec<PvConfig>> {
        let rows = sqlx::query_as::<_, PvConfigRow>(sql::GET_ALL_ENABLED)
            .fetch_all(pool)
            .await
            .map_err(|e| AuraError::database(format!("get_all_enabled: {e}")))?;
        Ok(rows.into_iter().map(PvConfigRow::into_pv_config).collect())
    }

    /// Get PV configurations changed since a given timestamp.
    /// Returns both enabled and disabled PVs (so discover can detect disables).
    pub async fn get_changed_since(
        pool: &PgPool,
        since: DateTime<Utc>,
    ) -> AuraResult<Vec<PvConfig>> {
        let rows = sqlx::query_as::<_, PvConfigRow>(sql::GET_CHANGED)
            .bind(since)
            .fetch_all(pool)
            .await
            .map_err(|e| AuraError::database(format!("get_changed_since: {e}")))?;
        Ok(rows.into_iter().map(PvConfigRow::into_pv_config).collect())
    }

    /// Get all PVs assigned to a specific shard.
    pub async fn get_by_shard(pool: &PgPool, shard_id: i32) -> AuraResult<Vec<PvConfig>> {
        let rows = sqlx::query_as::<_, PvConfigRow>(sql::GET_BY_SHARD)
            .bind(shard_id)
            .fetch_all(pool)
            .await
            .map_err(|e| AuraError::database(format!("get_by_shard: {e}")))?;
        Ok(rows.into_iter().map(PvConfigRow::into_pv_config).collect())
    }

    /// Get all PVs expected on a specific IOC.
    pub async fn get_by_ioc(pool: &PgPool, ioc_guid: &str) -> AuraResult<Vec<PvConfig>> {
        let rows = sqlx::query_as::<_, PvConfigRow>(sql::GET_BY_IOC)
            .bind(ioc_guid)
            .fetch_all(pool)
            .await
            .map_err(|e| AuraError::database(format!("get_by_ioc: {e}")))?;
        Ok(rows.into_iter().map(PvConfigRow::into_pv_config).collect())
    }

    /// Search PVs by name pattern (ILIKE, case-insensitive).
    pub async fn search(pool: &PgPool, pattern: &str, limit: i64) -> AuraResult<Vec<PvConfig>> {
        let like_pattern = format!("%{pattern}%");
        let rows = sqlx::query_as::<_, PvConfigRow>(sql::SEARCH)
            .bind(&like_pattern)
            .bind(limit.max(1).min(1000))
            .fetch_all(pool)
            .await
            .map_err(|e| AuraError::database(format!("search: {e}")))?;
        Ok(rows.into_iter().map(PvConfigRow::into_pv_config).collect())
    }

    /// Insert a new PV configuration.
    pub async fn insert(pool: &PgPool, config: &PvConfig) -> AuraResult<()> {
        let (epsilon, heartbeat) = validate_config(config);
        sqlx::query(sql::INSERT)
            .bind(&config.pv_name)
            .bind(&config.description)
            .bind(&config.unit)
            .bind(epsilon)
            .bind(heartbeat)
            .bind(config.enabled)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("insert pv_config: {e}")))?;
        Ok(())
    }

    /// Insert or update a PV configuration (idempotent).
    pub async fn upsert(pool: &PgPool, config: &PvConfig) -> AuraResult<()> {
        let (epsilon, heartbeat) = validate_config(config);
        sqlx::query(sql::UPSERT)
            .bind(&config.pv_name)
            .bind(&config.description)
            .bind(&config.unit)
            .bind(epsilon)
            .bind(heartbeat)
            .bind(config.enabled)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("upsert pv_config: {e}")))?;
        Ok(())
    }

    /// Batch upsert: N configs in a single transaction.
    pub async fn upsert_batch(pool: &PgPool, configs: &[PvConfig]) -> AuraResult<usize> {
        if configs.is_empty() {
            return Ok(0);
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| AuraError::database(format!("batch begin: {e}")))?;

        for config in configs {
            let (epsilon, heartbeat) = validate_config(config);
            sqlx::query(sql::UPSERT)
                .bind(&config.pv_name)
                .bind(&config.description)
                .bind(&config.unit)
                .bind(epsilon)
                .bind(heartbeat)
                .bind(config.enabled)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    AuraError::database(format!("batch upsert ({}): {e}", config.pv_name))
                })?;
        }

        tx.commit()
            .await
            .map_err(|e| AuraError::database(format!("batch commit: {e}")))?;

        Ok(configs.len())
    }

    /// Update epsilon and heartbeat for an existing PV.
    pub async fn update_filter_params(
        pool: &PgPool,
        pv_name: &str,
        epsilon: Option<f64>,
        heartbeat_s: f64,
    ) -> AuraResult<bool> {
        let epsilon = epsilon.map(|e| e.max(0.0));
        let heartbeat = heartbeat_s.max(1.0);
        let result = sqlx::query(sql::UPDATE_FILTER)
            .bind(epsilon)
            .bind(heartbeat)
            .bind(pv_name)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("update_filter: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// Enable or disable a PV.
    pub async fn set_enabled(pool: &PgPool, pv_name: &str, enabled: bool) -> AuraResult<bool> {
        let result = sqlx::query(sql::SET_ENABLED)
            .bind(enabled)
            .bind(pv_name)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("set_enabled: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// Assign a shard to a PV (called by aura-discover).
    pub async fn assign_shard(pool: &PgPool, pv_name: &str, shard_id: i32) -> AuraResult<bool> {
        let result = sqlx::query(sql::ASSIGN_SHARD)
            .bind(shard_id)
            .bind(pv_name)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("assign_shard: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// Delete a PV configuration.
    pub async fn delete(pool: &PgPool, pv_name: &str) -> AuraResult<bool> {
        let result = sqlx::query(sql::DELETE)
            .bind(pv_name)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("delete: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// Count all PVs (enabled + disabled).
    pub async fn count_all(pool: &PgPool) -> AuraResult<i64> {
        let count: Option<i64> = sqlx::query_scalar(sql::COUNT_ALL)
            .fetch_one(pool)
            .await
            .map_err(|e| AuraError::database(format!("count_all: {e}")))?;
        Ok(count.unwrap_or(0))
    }

    /// Count enabled PVs only.
    pub async fn count_enabled(pool: &PgPool) -> AuraResult<i64> {
        let count: Option<i64> = sqlx::query_scalar(sql::COUNT_ENABLED)
            .fetch_one(pool)
            .await
            .map_err(|e| AuraError::database(format!("count_enabled: {e}")))?;
        Ok(count.unwrap_or(0))
    }
}

/// Internal sqlx row — moves into PvConfig (zero clone).
#[derive(Debug, sqlx::FromRow)]
struct PvConfigRow {
    pv_name: String,
    description: Option<String>,
    unit: Option<String>,
    epsilon: Option<f64>,
    heartbeat_s: f64,
    expected_ioc: Option<String>,
    shard_id: Option<i32>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl PvConfigRow {
    /// Move fields into PvConfig (zero clone).
    fn into_pv_config(self) -> PvConfig {
        let mut cfg = PvConfig::new(self.pv_name); // moved
        cfg.description = self.description; // moved
        cfg.unit = self.unit; // moved
        cfg.epsilon = self.epsilon;
        cfg.heartbeat_s = self.heartbeat_s;
        cfg.enabled = self.enabled;
        cfg
    }
}

impl fmt::Debug for PvConfigDao {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PvConfigDao").finish()
    }
}

impl fmt::Display for PvConfigDao {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PvConfigDao")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(pv: &str, enabled: bool) -> PvConfigRow {
        PvConfigRow {
            pv_name: pv.to_string(),
            description: Some("test desc".to_string()),
            unit: Some("K".to_string()),
            epsilon: Some(0.01),
            heartbeat_s: 30.0,
            expected_ioc: Some("guid-123".to_string()),
            shard_id: Some(2),
            enabled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_row_to_config_full() {
        let cfg = make_row("CRYO:TEMP", true).into_pv_config();
        assert_eq!(cfg.pv_name, "CRYO:TEMP");
        assert_eq!(cfg.description.as_deref(), Some("test desc"));
        assert_eq!(cfg.unit.as_deref(), Some("K"));
        assert_eq!(cfg.epsilon, Some(0.01));
        assert_eq!(cfg.heartbeat_s, 30.0);
        assert!(cfg.enabled);
    }

    #[test]
    fn test_row_to_config_minimal() {
        let row = PvConfigRow {
            pv_name: "PV:TEST".to_string(),
            description: None,
            unit: None,
            epsilon: None,
            heartbeat_s: 60.0,
            expected_ioc: None,
            shard_id: None,
            enabled: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let cfg = row.into_pv_config();
        assert_eq!(cfg.pv_name, "PV:TEST");
        assert!(cfg.description.is_none());
        assert!(cfg.epsilon.is_none());
        assert!(!cfg.enabled);
    }

    #[test]
    fn test_row_to_config_disabled() {
        let cfg = make_row("PV:OFF", false).into_pv_config();
        assert!(!cfg.enabled);
    }

    #[test]
    fn test_validate_normal() {
        let cfg = PvConfig {
            pv_name: "PV".into(),
            epsilon: Some(0.5),
            heartbeat_s: 30.0,
            ..PvConfig::new("PV")
        };
        let (eps, hb) = validate_config(&cfg);
        assert_eq!(eps, Some(0.5));
        assert_eq!(hb, 30.0);
    }

    #[test]
    fn test_validate_negative_epsilon() {
        let cfg = PvConfig {
            pv_name: "PV".into(),
            epsilon: Some(-1.0),
            heartbeat_s: 30.0,
            ..PvConfig::new("PV")
        };
        let (eps, _) = validate_config(&cfg);
        assert_eq!(eps, Some(0.0));
    }

    #[test]
    fn test_validate_none_epsilon() {
        let cfg = PvConfig {
            pv_name: "PV".into(),
            epsilon: None,
            heartbeat_s: 30.0,
            ..PvConfig::new("PV")
        };
        let (eps, _) = validate_config(&cfg);
        assert!(eps.is_none());
    }

    #[test]
    fn test_validate_low_heartbeat() {
        let cfg = PvConfig {
            pv_name: "PV".into(),
            epsilon: None,
            heartbeat_s: 0.1,
            ..PvConfig::new("PV")
        };
        let (_, hb) = validate_config(&cfg);
        assert_eq!(hb, 1.0);
    }

    #[test]
    fn test_validate_zero_heartbeat() {
        let cfg = PvConfig {
            pv_name: "PV".into(),
            epsilon: None,
            heartbeat_s: 0.0,
            ..PvConfig::new("PV")
        };
        let (_, hb) = validate_config(&cfg);
        assert_eq!(hb, 1.0);
    }

    #[test]
    fn test_sql_columns() {
        assert!(sql::COLUMNS.contains("pv_name"));
        assert!(sql::COLUMNS.contains("updated_at"));
        assert!(sql::COLUMNS.contains("shard_id"));
    }

    #[test]
    fn test_sql_get() {
        assert!(sql::GET.contains("pv_config"));
        assert!(sql::GET.contains("pv_name = $1"));
    }

    #[test]
    fn test_sql_get_all_enabled() {
        assert!(sql::GET_ALL_ENABLED.contains("enabled = TRUE"));
        assert!(sql::GET_ALL_ENABLED.contains("ORDER BY pv_name"));
    }

    #[test]
    fn test_sql_get_changed() {
        assert!(sql::GET_CHANGED.contains("updated_at > $1"));
        assert!(sql::GET_CHANGED.contains("ORDER BY updated_at"));
    }

    #[test]
    fn test_sql_upsert() {
        assert!(sql::UPSERT.contains("ON CONFLICT (pv_name)"));
        assert!(sql::UPSERT.contains("DO UPDATE SET"));
        assert!(sql::UPSERT.contains("updated_at = NOW()"));
    }

    #[test]
    fn test_sql_update_filter() {
        assert!(sql::UPDATE_FILTER.contains("epsilon = $1"));
        assert!(sql::UPDATE_FILTER.contains("updated_at = NOW()"));
    }

    #[test]
    fn test_sql_search() {
        assert!(sql::SEARCH.contains("ILIKE"));
        assert!(sql::SEARCH.contains("LIMIT"));
    }

    #[test]
    fn test_sql_by_shard() {
        assert!(sql::GET_BY_SHARD.contains("shard_id = $1"));
    }

    #[test]
    fn test_sql_by_ioc() {
        assert!(sql::GET_BY_IOC.contains("expected_ioc = $1"));
    }

    #[test]
    fn test_dao_display() {
        assert_eq!(PvConfigDao.to_string(), "PvConfigDao");
    }

    #[test]
    fn test_dao_debug() {
        assert!(format!("{:?}", PvConfigDao).contains("PvConfigDao"));
    }
}