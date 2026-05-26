//!
//! Migrations are embedded in the binary via `sqlx::migrate!()` and
//! run in order on startup. This ensures the schema is always up-to-date
//! without requiring external migration files at runtime.
//!
//! ## Migration order
//!
//! ```text
//! 001_extensions              — TimescaleDB + pg_stat_statements
//! 002_pv_lookup               — pv_id ↔ pv_name normalization table
//! 003_pv_config               — PV archiving configuration (name, epsilon, heartbeat)
//! 004_pv_metadata             — PV metadata from PVA Normative Types (units, alarms, enum choices)
//! 005_samples                 — main scalar + string hypertables (~90% of traffic)
//! 006_samples_typed           — per-NT-type hypertables (image, json)
//! 007_compression             — TimescaleDB compression policies (gorilla + delta-of-delta)
//! 008_retention               — tiered data retention (raw → downsampled → purge)
//! 009_continuous_aggs         — hourly and daily materialized continuous aggregates
//! 010_ioc_registry            — discovered IOC tracking (from PVA beacons)
//! 011_alert_log               — system alert history (IOC offline, PV disconnected)
//! 012_ingest_registry         — ingest instance heartbeat tracking
//! 013_array_destructured      — element-per-row array tables for gorilla compression
//! 014_destructure_json_tables — element-per-row JSON tables for gorilla compression
//! ```

use std::fmt;

use sqlx::postgres::PgPool;

use aura_core::error::{AuraError, AuraResult};

/// All SQL migrations as static strings, in order.
///
/// Each migration is idempotent (uses IF NOT EXISTS / CREATE OR REPLACE).
/// This allows safe re-runs without manual version tracking in dev.
pub struct Migrations;

impl Migrations {
    /// All migration SQL statements in order.
    pub const ALL: &'static [MigrationStep] = &[
        MigrationStep {
            version: 1,
            name: "extensions",
            sql: include_str!("../../../migrations/001_extensions.sql"),
        },
        MigrationStep {
            version: 2,
            name: "pv_lookup",
            sql: include_str!("../../../migrations/002_pv_lookup.sql"),
        },
        MigrationStep {
            version: 3,
            name: "pv_config",
            sql: include_str!("../../../migrations/003_pv_config.sql"),
        },
        MigrationStep {
            version: 4,
            name: "pv_metadata",
            sql: include_str!("../../../migrations/004_pv_metadata.sql"),
        },
        MigrationStep {
            version: 5,
            name: "samples",
            sql: include_str!("../../../migrations/005_samples.sql"),
        },
        MigrationStep {
            version: 6,
            name: "samples_typed",
            sql: include_str!("../../../migrations/006_samples_typed.sql"),
        },
        MigrationStep {
            version: 7,
            name: "compression",
            sql: include_str!("../../../migrations/007_compression.sql"),
        },
        MigrationStep {
            version: 8,
            name: "retention",
            sql: include_str!("../../../migrations/008_retention.sql"),
        },
        MigrationStep {
            version: 9,
            name: "continuous_aggs",
            sql: include_str!("../../../migrations/009_continuous_aggs.sql"),
        },
        MigrationStep {
            version: 10,
            name: "ioc_registry",
            sql: include_str!("../../../migrations/010_ioc_registry.sql"),
        },
        MigrationStep {
            version: 11,
            name: "alert_log",
            sql: include_str!("../../../migrations/011_alert_log.sql"),
        },
        MigrationStep {
            version: 12,
            name: "ingest_registry",
            sql: include_str!("../../../migrations/012_ingest_registry.sql"),
        },
        MigrationStep {
            version: 13,
            name: "array_destructured",
            sql: include_str!("../../../migrations/013_array_destructured.sql"),
        },
        MigrationStep {
            version: 14,
            name: "destructure_json_tables",
            sql: include_str!("../../../migrations/014_destructure_json_tables.sql"),
        },
    ];

    /// Run all migrations in order.
    ///
    /// Uses a `_aura_migrations` table to track which migrations
    /// have already been applied.
    pub async fn run(pool: &PgPool) -> AuraResult<MigrationReport> {
        // Create the migrations tracking table if it doesn't exist.
        sqlx::query(Self::TRACKING_TABLE_SQL)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("failed to create migrations table: {e}")))?;

        let mut report = MigrationReport {
            applied: Vec::new(),
            skipped: Vec::new(),
            total: Self::ALL.len(),
        };

        for step in Self::ALL {
            let already_applied = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM _aura_migrations WHERE version = $1)",
            )
            .bind(step.version)
            .fetch_one(pool)
            .await
            .map_err(|e| AuraError::database(format!("migration check failed: {e}")))?;

            if already_applied {
                report.skipped.push(step.version);
                continue;
            }

            // Execute the migration SQL.
            sqlx::raw_sql(step.sql).execute(pool).await.map_err(|e| {
                AuraError::database(format!(
                    "migration {:03}_{} failed: {e}",
                    step.version, step.name
                ))
            })?;

            // Record the migration.
            sqlx::query(
                "INSERT INTO _aura_migrations (version, name, applied_at) VALUES ($1, $2, NOW())",
            )
            .bind(step.version)
            .bind(step.name)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("failed to record migration: {e}")))?;

            report.applied.push(step.version);

            tracing::info!(
                version = step.version,
                name = step.name,
                "migration applied"
            );
        }

        Ok(report)
    }

    /// Get a specific migration by version number.
    pub fn get(version: i32) -> Option<&'static MigrationStep> {
        Self::ALL.iter().find(|m| m.version == version)
    }

    /// Get a specific migration by name.
    pub fn get_by_name(name: &str) -> Option<&'static MigrationStep> {
        Self::ALL.iter().find(|m| m.name == name)
    }

    /// Total number of migrations.
    pub const fn count() -> usize {
        14
    }

    /// The SQL to create the migrations tracking table.
    const TRACKING_TABLE_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS _aura_migrations (
            version     INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    "#;
}

/// A single migration step.
#[derive(Debug, Clone, Copy)]
pub struct MigrationStep {
    /// Migration version number.
    pub version: i32,
    /// Human-readable name.
    pub name: &'static str,
    /// SQL content.
    pub sql: &'static str,
}

impl fmt::Display for MigrationStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:03}_{}", self.version, self.name)
    }
}

/// Report of a migration run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// Versions that were applied in this run.
    pub applied: Vec<i32>,
    /// Versions that were already applied (skipped).
    pub skipped: Vec<i32>,
    /// Total number of migrations defined.
    pub total: usize,
}

impl MigrationReport {
    /// Number of migrations applied in this run.
    pub fn applied_count(&self) -> usize {
        self.applied.len()
    }

    /// Number of migrations skipped (already applied).
    pub fn skipped_count(&self) -> usize {
        self.skipped.len()
    }

    /// Whether all migrations were already applied (nothing new).
    pub fn is_up_to_date(&self) -> bool {
        self.applied.is_empty()
    }

    /// Whether this was a fresh database (all migrations applied).
    pub fn is_fresh_install(&self) -> bool {
        self.applied.len() == self.total
    }
}

impl fmt::Display for MigrationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_up_to_date() {
            write!(f, "database up to date ({} migrations)", self.total)
        } else {
            write!(
                f,
                "applied {} migration(s), {} skipped, {} total",
                self.applied_count(),
                self.skipped_count(),
                self.total
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_count() {
        assert_eq!(Migrations::ALL.len(), 14);
        assert_eq!(Migrations::count(), 14);
    }

    #[test]
    fn test_migration_versions_sequential() {
        for (i, step) in Migrations::ALL.iter().enumerate() {
            assert_eq!(
                step.version,
                (i + 1) as i32,
                "migration {} has wrong version {}",
                step.name,
                step.version
            );
        }
    }

    #[test]
    fn test_migration_versions_unique() {
        let mut versions: Vec<i32> = Migrations::ALL.iter().map(|m| m.version).collect();
        versions.sort();
        versions.dedup();
        assert_eq!(versions.len(), Migrations::ALL.len());
    }

    #[test]
    fn test_migration_names_unique() {
        let mut names: Vec<&str> = Migrations::ALL.iter().map(|m| m.name).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), Migrations::ALL.len());
    }

    #[test]
    fn test_migration_names_not_empty() {
        for step in Migrations::ALL {
            assert!(
                !step.name.is_empty(),
                "version {} has empty name",
                step.version
            );
        }
    }

    #[test]
    fn test_migration_sql_not_empty() {
        for step in Migrations::ALL {
            assert!(
                !step.sql.trim().is_empty(),
                "migration {:03}_{} has empty SQL",
                step.version,
                step.name
            );
        }
    }

    #[test]
    fn test_migration_sql_no_drop_table() {
        // Safety check: migrations should never DROP tables
        for step in Migrations::ALL {
            let upper = step.sql.to_uppercase();
            assert!(
                !upper.contains("DROP TABLE"),
                "migration {:03}_{} contains DROP TABLE",
                step.version,
                step.name
            );
        }
    }

    #[test]
    fn test_migration_sql_idempotent_markers() {
        // Each migration should use IF NOT EXISTS or similar
        for step in Migrations::ALL {
            let upper = step.sql.to_uppercase();
            let has_idempotent = upper.contains("IF NOT EXISTS")
                || upper.contains("CREATE OR REPLACE")
                || upper.contains("ON CONFLICT")
                || upper.contains("DO NOTHING")
                || upper.contains("IF EXISTS");
            assert!(
                has_idempotent,
                "migration {:03}_{} may not be idempotent",
                step.version, step.name
            );
        }
    }

    // ── Lookup ───────────────────────────────────────────────────────

    #[test]
    fn test_get_by_version() {
        let step = Migrations::get(1).unwrap();
        assert_eq!(step.name, "extensions");
        assert_eq!(step.version, 1);
    }

    #[test]
    fn test_get_by_version_last() {
        let step = Migrations::get(12).unwrap();
        assert_eq!(step.name, "ingest_registry");
    }

    #[test]
    fn test_get_by_version_missing() {
        assert!(Migrations::get(999).is_none());
        assert!(Migrations::get(0).is_none());
        assert!(Migrations::get(-1).is_none());
    }

    #[test]
    fn test_get_by_name() {
        let step = Migrations::get_by_name("samples").unwrap();
        assert_eq!(step.version, 5);
    }

    #[test]
    fn test_get_by_name_missing() {
        assert!(Migrations::get_by_name("nonexistent").is_none());
    }

    // ── MigrationStep Display ────────────────────────────────────────

    #[test]
    fn test_step_display() {
        let step = Migrations::get(1).unwrap();
        assert_eq!(step.to_string(), "001_extensions");

        let step = Migrations::get(12).unwrap();
        assert_eq!(step.to_string(), "012_ingest_registry");
    }

    #[test]
    fn test_step_debug() {
        let step = Migrations::get(1).unwrap();
        let d = format!("{:?}", step);
        assert!(d.contains("MigrationStep"));
        assert!(d.contains("extensions"));
    }

    // ── Expected migration names ─────────────────────────────────────

    #[test]
    fn test_expected_migration_order() {
        let names: Vec<&str> = Migrations::ALL.iter().map(|m| m.name).collect();
        assert_eq!(
            names,
            vec![
                "extensions",
                "pv_lookup",
                "pv_config",
                "pv_metadata",
                "samples",
                "samples_typed",
                "compression",
                "retention",
                "continuous_aggs",
                "ioc_registry",
                "alert_log",
                "ingest_registry",
                "array_destructured",
                "destructure_json_tables"
            ]
        );
    }

    // ── SQL content checks ───────────────────────────────────────────

    #[test]
    fn test_extensions_creates_timescaledb() {
        let step = Migrations::get(1).unwrap();
        assert!(step.sql.to_uppercase().contains("TIMESCALEDB"));
    }

    #[test]
    fn test_pv_lookup_creates_table() {
        let step = Migrations::get(2).unwrap();
        let upper = step.sql.to_uppercase();
        assert!(upper.contains("PV_LOOKUP"));
        assert!(upper.contains("PV_ID"));
        assert!(upper.contains("PV_NAME"));
    }

    #[test]
    fn test_samples_creates_hypertable() {
        let step = Migrations::get(5).unwrap();
        let upper = step.sql.to_uppercase();
        assert!(upper.contains("SAMPLES"));
        assert!(upper.contains("CREATE_HYPERTABLE") || upper.contains("HYPERTABLE"));
    }

    #[test]
    fn test_report_fresh_install() {
        let report = MigrationReport {
            applied: (1..=12).collect(),
            skipped: vec![],
            total: 12,
        };
        assert_eq!(report.applied_count(), 12);
        assert_eq!(report.skipped_count(), 0);
        assert!(report.is_fresh_install());
        assert!(!report.is_up_to_date());
    }

    #[test]
    fn test_report_up_to_date() {
        let report = MigrationReport {
            applied: vec![],
            skipped: (1..=12).collect(),
            total: 12,
        };
        assert_eq!(report.applied_count(), 0);
        assert_eq!(report.skipped_count(), 12);
        assert!(report.is_up_to_date());
        assert!(!report.is_fresh_install());
    }

    #[test]
    fn test_report_partial() {
        let report = MigrationReport {
            applied: vec![11, 12],
            skipped: (1..=10).collect(),
            total: 12,
        };
        assert_eq!(report.applied_count(), 2);
        assert_eq!(report.skipped_count(), 10);
        assert!(!report.is_up_to_date());
        assert!(!report.is_fresh_install());
    }

    #[test]
    fn test_report_display_up_to_date() {
        let report = MigrationReport {
            applied: vec![],
            skipped: (1..=12).collect(),
            total: 12,
        };
        let s = report.to_string();
        assert!(s.contains("up to date"));
        assert!(s.contains("12"));
    }

    #[test]
    fn test_report_display_applied() {
        let report = MigrationReport {
            applied: vec![11, 12],
            skipped: (1..=10).collect(),
            total: 12,
        };
        let s = report.to_string();
        assert!(s.contains("applied 2"));
        assert!(s.contains("10 skipped"));
    }

    #[test]
    fn test_report_clone_eq() {
        let a = MigrationReport {
            applied: vec![1],
            skipped: vec![],
            total: 12,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_report_debug() {
        let report = MigrationReport {
            applied: vec![1],
            skipped: vec![],
            total: 12,
        };
        let d = format!("{:?}", report);
        assert!(d.contains("MigrationReport"));
        assert!(d.contains("applied"));
    }
}