//! CRUD operations on the `pv_config` table.
//!
//! Provides functions to list, insert, update, and delete PV configurations.

use chrono::{DateTime, Utc};
use std::fmt;

use sqlx::postgres::PgPool;

use aura_core::error::{AuraError, AuraResult};
use aura_core::pv::PvConfig;

mod sql {
    pub const GET: &str = "SELECT pv_name, description, unit, heartbeat_s, \
         expected_ioc, enabled, created_at, updated_at \
         FROM pv_config WHERE pv_name = $1";

    pub const GET_ALL_ENABLED: &str = "SELECT pv_name, description, unit, heartbeat_s, \
         expected_ioc, enabled, created_at, updated_at \
         FROM pv_config WHERE enabled = TRUE ORDER BY pv_name";

    pub const GET_CHANGED: &str = "SELECT pv_name, description, unit, heartbeat_s, \
         expected_ioc, enabled, created_at, updated_at \
         FROM pv_config WHERE updated_at > $1 ORDER BY updated_at";

    pub const GET_BY_IOC: &str = "SELECT pv_name, description, unit, heartbeat_s, \
         expected_ioc, enabled, created_at, updated_at \
         FROM pv_config WHERE expected_ioc = $1 ORDER BY pv_name";

    pub const INSERT: &str = "INSERT INTO pv_config (pv_name, description, unit, heartbeat_s, enabled) \
         VALUES ($1, $2, $3, $4, $5)";

    pub const UPSERT: &str = "INSERT INTO pv_config (pv_name, description, unit, heartbeat_s, enabled) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (pv_name) DO UPDATE SET \
             description = EXCLUDED.description, \
             unit = EXCLUDED.unit, \
             heartbeat_s = EXCLUDED.heartbeat_s, \
             enabled = EXCLUDED.enabled, \
             updated_at = NOW()";

    pub const UPDATE_HEARTBEAT: &str = "UPDATE pv_config SET heartbeat_s = $1, updated_at = NOW() \
         WHERE pv_name = $2";

    pub const SET_ENABLED: &str =
        "UPDATE pv_config SET enabled = $1, updated_at = NOW() WHERE pv_name = $2";

    pub const DELETE: &str = "DELETE FROM pv_config WHERE pv_name = $1";

    pub const COUNT_ALL: &str = "SELECT COUNT(*) FROM pv_config";

    pub const COUNT_ENABLED: &str = "SELECT COUNT(*) FROM pv_config WHERE enabled = TRUE";

    pub const SEARCH: &str = "SELECT pv_name, description, unit, heartbeat_s, \
         expected_ioc, enabled, created_at, updated_at \
         FROM pv_config WHERE pv_name ILIKE $1 ORDER BY pv_name LIMIT $2";
}

/// Map the in-memory heartbeat to its SQL representation.
///
/// The DAO's historical contract is kept: `heartbeat_s <= 0.0` in a
/// `PvConfig` means "use the global default", which is now stored as SQL
/// `NULL`. Positive values are clamped to a 1 s minimum (anything below is
/// likely a mistake). An explicit per-PV disable (SQL `0`) is intentionally
/// not expressible through this DAO — set it with a direct UPDATE.
fn validate_heartbeat(heartbeat_s: f64) -> Option<f64> {
    if heartbeat_s <= 0.0 {
        None
    } else {
        Some(heartbeat_s.max(1.0))
    }
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

    /// Get all PVs expected on a specific IOC.
    pub async fn get_by_ioc(pool: &PgPool, ioc_addr: &str) -> AuraResult<Vec<PvConfig>> {
        let rows = sqlx::query_as::<_, PvConfigRow>(sql::GET_BY_IOC)
            .bind(ioc_addr)
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
        let heartbeat = validate_heartbeat(config.heartbeat_s);
        sqlx::query(sql::INSERT)
            .bind(&config.pv_name)
            .bind(&config.description)
            .bind(&config.unit)
            .bind(heartbeat)
            .bind(config.enabled)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("insert pv_config: {e}")))?;
        Ok(())
    }

    /// Insert or update a PV configuration (idempotent).
    pub async fn upsert(pool: &PgPool, config: &PvConfig) -> AuraResult<()> {
        let heartbeat = validate_heartbeat(config.heartbeat_s);
        sqlx::query(sql::UPSERT)
            .bind(&config.pv_name)
            .bind(&config.description)
            .bind(&config.unit)
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
            let heartbeat = validate_heartbeat(config.heartbeat_s);
            sqlx::query(sql::UPSERT)
                .bind(&config.pv_name)
                .bind(&config.description)
                .bind(&config.unit)
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

    /// Update heartbeat for an existing PV.
    pub async fn update_heartbeat(
        pool: &PgPool,
        pv_name: &str,
        heartbeat_s: f64,
    ) -> AuraResult<bool> {
        let heartbeat = validate_heartbeat(heartbeat_s);
        let result = sqlx::query(sql::UPDATE_HEARTBEAT)
            .bind(heartbeat)
            .bind(pv_name)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("update_heartbeat: {e}")))?;
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
    /// Nullable: NULL = use global default (see 003_pv_config.sql).
    heartbeat_s: Option<f64>,
    expected_ioc: Option<String>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl PvConfigRow {
    fn into_pv_config(self) -> PvConfig {
        let mut cfg = PvConfig::new(self.pv_name);
        cfg.description = self.description;
        cfg.unit = self.unit;
        // In the in-memory PvConfig, 0.0 keeps its historical meaning of
        // "use global default" — only the SQL representation differs
        // (NULL = default, 0 = per-PV disable; see 003_pv_config.sql).
        cfg.heartbeat_s = self.heartbeat_s.unwrap_or(0.0);
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
            heartbeat_s: Some(30.0),
            expected_ioc: Some("10.0.1.5:5075".to_string()),
            enabled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_row_to_config() {
        let cfg = make_row("CRYO:TEMP", true).into_pv_config();
        assert_eq!(cfg.pv_name, "CRYO:TEMP");
        assert_eq!(cfg.description.as_deref(), Some("test desc"));
        assert_eq!(cfg.unit.as_deref(), Some("K"));
        assert_eq!(cfg.heartbeat_s, 30.0);
        assert!(cfg.enabled);
    }

    #[test]
    fn test_row_to_config_minimal() {
        let row = PvConfigRow {
            pv_name: "PV:TEST".to_string(),
            description: None,
            unit: None,
            heartbeat_s: None, // SQL NULL = use global default
            expected_ioc: None,
            enabled: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let cfg = row.into_pv_config();
        assert!(cfg.description.is_none());
        assert!(!cfg.enabled);
    }

    #[test]
    fn test_validate_normal() {
        assert_eq!(validate_heartbeat(30.0), Some(30.0));
    }

    #[test]
    fn test_validate_zero_heartbeat() {
        assert_eq!(validate_heartbeat(0.0), None); // use global default -> SQL NULL
    }

    #[test]
    fn test_validate_sub_second_clamped() {
        assert_eq!(validate_heartbeat(0.5), Some(1.0)); // positive but too low → clamp to 1s
    }

    #[test]
    fn test_validate_negative_heartbeat() {
        assert_eq!(validate_heartbeat(-1.0), None); // negative → use default -> SQL NULL
    }

    #[test]
    fn test_sql_upsert() {
        assert!(sql::UPSERT.contains("ON CONFLICT (pv_name)"));
        assert!(sql::UPSERT.contains("updated_at = NOW()"));
        assert!(!sql::UPSERT.contains("epsilon"));
    }

    #[test]
    fn test_sql_update_heartbeat() {
        assert!(sql::UPDATE_HEARTBEAT.contains("heartbeat_s = $1"));
        assert!(!sql::UPDATE_HEARTBEAT.contains("epsilon"));
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
