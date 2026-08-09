//! Application configuration, deserialized from `aura.toml`.
//!
//! Read once at startup. Runtime changes (heartbeat intervals, name_servers) are
//! handled via the `pv_config` and `ioc_config` table in TimescaleDB, not by reloading this file.
//!
//! Every field has a sensible default - a minimal TOML with empty
//! sections (`[redis]\n[database]\n...`) produces a valid config.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use crate::AuraError;

/// Top-level configuration for the AURA system.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct AuraConfig {
    pub redis: RedisConfig,
    pub database: DatabaseConfig,
    pub discover: DiscoverConfig,
    pub ingest: IngestConfig,
    #[serde(default)]
    pub store: StoreConfig,
    pub api: ApiConfig,
    pub metrics: MetricsConfig,
    pub telemetry: TelemetryConfig,
}

impl AuraConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, crate::AuraError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| crate::AuraError::config(format!("cannot read config: {e}")))?;
        Self::from_str(&content)
    }

    /// Validate configuration for logical consistency.
    pub fn validate(&self) -> Vec<ConfigIssue> {
        let mut issues = Vec::new();

        if self.store.batch_size == 0 {
            issues.push(ConfigIssue::error("store", "batch_size must be > 0"));
        }

        if self.ingest.default_heartbeat_s < 0.0 {
            issues.push(ConfigIssue::error(
                "ingest",
                "default_heartbeat_s must be >= 0 (0 = disabled)",
            ));
        }

        issues
    }
}

impl FromStr for AuraConfig {
    type Err = AuraError;

    /// Parse configuration from a TOML string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        toml::from_str(s).map_err(|e| AuraError::config(format!("invalid TOML: {e}")))
    }
}

/// A configuration validation issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigIssue {
    pub section: String,
    pub message: String,
    pub is_error: bool,
}

impl ConfigIssue {
    pub fn error(section: &str, message: &str) -> Self {
        Self {
            section: section.into(),
            message: message.into(),
            is_error: true,
        }
    }

    pub fn warning(section: &str, message: &str) -> Self {
        Self {
            section: section.into(),
            message: message.into(),
            is_error: false,
        }
    }
}

impl fmt::Display for ConfigIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = if self.is_error { "ERROR" } else { "WARN" };
        write!(f, "[{}] {}: {}", level, self.section, self.message)
    }
}

/// Redis connection configuration.
///
/// Redis is used for the `aura:commands` pub/sub channel (discover -> ingest).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RedisConfig {
    #[serde(default = "d::redis_url")]
    pub url: String,
}

/// PostgreSQL / TimescaleDB connection configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DatabaseConfig {
    #[serde(default = "d::database_url")]
    pub url: String,
    #[serde(default = "d::max_connections")]
    pub max_connections: u32,
}

/// IOC discovery and PV registry polling configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DiscoverConfig {
    /// Poll interval for pv_config changes (seconds).
    #[serde(default = "d::config_poll_interval")]
    pub config_poll_interval_s: u64,
    /// TCP name servers (IOC addresses) for PV search.
    #[serde(default)]
    pub name_servers: Vec<String>,
}

/// Ingest pipeline configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct IngestConfig {
    /// Default heartbeat interval (seconds). 0 = disabled.
    #[serde(default = "d::heartbeat")]
    pub default_heartbeat_s: f64,
}

/// Database writer (aura-store) configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct StoreConfig {
    /// Rows per COPY batch to TimescaleDB.
    #[serde(default = "d::store_batch_size")]
    pub batch_size: usize,
    /// Max ms before flushing incomplete batch.
    #[serde(default = "d::store_flush_interval")]
    pub flush_interval_ms: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            batch_size: d::store_batch_size(),
            flush_interval_ms: d::store_flush_interval(),
        }
    }
}

/// REST API server configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ApiConfig {
    #[serde(default = "d::api_bind")]
    pub bind: String,
}

/// Prometheus metrics endpoint configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MetricsConfig {
    #[serde(default = "d::metrics_bind")]
    pub bind: String,
}

/// Logging / tracing configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct TelemetryConfig {
    #[serde(default = "d::log_level")]
    pub log_level: String,
    #[serde(default = "d::log_format")]
    pub log_format: String,
}

mod d {
    pub fn redis_url() -> String {
        "redis://127.0.0.1:6379".into()
    }
    pub fn database_url() -> String {
        "postgresql://aura:aura@localhost/aura".into()
    }
    pub fn max_connections() -> u32 {
        20
    }
    pub fn config_poll_interval() -> u64 {
        30
    }
    pub fn heartbeat() -> f64 {
        0.0
    }
    pub fn store_batch_size() -> usize {
        200_000
    }
    pub fn store_flush_interval() -> u64 {
        100
    }
    pub fn api_bind() -> String {
        "0.0.0.0:8080".into()
    }
    pub fn metrics_bind() -> String {
        "0.0.0.0:9090".into()
    }
    pub fn log_level() -> String {
        "info".into()
    }
    pub fn log_format() -> String {
        "json".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_TOML: &str = r#"
[redis]
[database]
[discover]
[ingest]
[store]
[api]
[metrics]
[telemetry]
"#;

    const FULL_TOML: &str = r#"
[redis]
url = "redis://prod:6379"

[database]
url = "postgresql://prod:secret@db/aura"
max_connections = 50

[discover]
config_poll_interval_s = 10
name_servers = ["10.0.1.1:5075", "10.0.1.2:5075"]

[ingest]
default_heartbeat_s = 30.0

[store]
batch_size = 1000
flush_interval_ms = 200

[api]
bind = "0.0.0.0:9000"

[metrics]
bind = "0.0.0.0:9191"

[telemetry]
log_level = "debug"
log_format = "pretty"
"#;

    #[test]
    fn test_minimal_defaults() {
        let cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        assert_eq!(cfg.redis.url, "redis://127.0.0.1:6379");
        assert_eq!(cfg.database.url, "postgresql://aura:aura@localhost/aura");
        assert_eq!(cfg.database.max_connections, 20);
        assert_eq!(cfg.discover.config_poll_interval_s, 30);
        assert!(cfg.discover.name_servers.is_empty());
        assert!((cfg.ingest.default_heartbeat_s - 0.0).abs() < f64::EPSILON);
        assert_eq!(cfg.store.batch_size, 200_000);
        assert_eq!(cfg.store.flush_interval_ms, 100);
        assert_eq!(cfg.api.bind, "0.0.0.0:8080");
        assert_eq!(cfg.metrics.bind, "0.0.0.0:9090");
        assert_eq!(cfg.telemetry.log_level, "info");
        assert_eq!(cfg.telemetry.log_format, "json");
    }

    #[test]
    fn test_full_custom_config() {
        let cfg = AuraConfig::from_str(FULL_TOML).unwrap();
        assert_eq!(cfg.redis.url, "redis://prod:6379");
        assert_eq!(cfg.database.url, "postgresql://prod:secret@db/aura");
        assert_eq!(cfg.database.max_connections, 50);
        assert_eq!(cfg.discover.config_poll_interval_s, 10);
        assert_eq!(cfg.discover.name_servers.len(), 2);
        assert!((cfg.ingest.default_heartbeat_s - 30.0).abs() < f64::EPSILON);
        assert_eq!(cfg.store.batch_size, 1000);
        assert_eq!(cfg.store.flush_interval_ms, 200);
        assert_eq!(cfg.api.bind, "0.0.0.0:9000");
        assert_eq!(cfg.metrics.bind, "0.0.0.0:9191");
        assert_eq!(cfg.telemetry.log_level, "debug");
        assert_eq!(cfg.telemetry.log_format, "pretty");
    }

    #[test]
    fn test_partial_override_keeps_defaults() {
        let toml = r#"
[redis]
url = "redis://custom:6379"
[database]
[discover]
[ingest]
[store]
[api]
[metrics]
[telemetry]
"#;
        let cfg = AuraConfig::from_str(toml).unwrap();
        assert_eq!(cfg.redis.url, "redis://custom:6379");
    }

    #[test]
    fn test_invalid_toml() {
        let result = AuraConfig::from_str("not valid toml {{{");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid TOML"));
    }

    #[test]
    fn test_missing_file() {
        let result = AuraConfig::from_file("/nonexistent/aura.toml");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot read config")
        );
    }

    #[test]
    fn test_missing_section() {
        assert!(AuraConfig::from_str("[redis]\n").is_err());
    }

    #[test]
    fn test_validate_default_passes() {
        let cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        let errors: Vec<_> = cfg.validate().into_iter().filter(|i| i.is_error).collect();
        assert!(
            errors.is_empty(),
            "default config should have no errors: {:?}",
            errors
        );
    }

    #[test]
    fn test_validate_zero_batch_size() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.store.batch_size = 0;
        assert!(
            cfg.validate()
                .iter()
                .any(|i| i.is_error && i.message.contains("batch_size"))
        );
    }

    #[test]
    fn test_validate_negative_heartbeat() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.ingest.default_heartbeat_s = -1.0;
        assert!(
            cfg.validate()
                .iter()
                .any(|i| i.is_error && i.message.contains("heartbeat"))
        );
    }

    #[test]
    fn test_validate_zero_heartbeat_valid() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.ingest.default_heartbeat_s = 0.0;
        assert!(!cfg.validate().iter().any(|i| i.is_error));
    }

    #[test]
    fn test_serde_roundtrip() {
        let cfg = AuraConfig::from_str(FULL_TOML).unwrap();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: AuraConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn test_toml_roundtrip() {
        let cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        let toml_out = toml::to_string(&cfg).unwrap();
        let back: AuraConfig = toml::from_str(&toml_out).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn test_config_issue_display() {
        assert_eq!(
            ConfigIssue::error("ingest", "bad").to_string(),
            "[ERROR] ingest: bad"
        );
        assert_eq!(
            ConfigIssue::warning("store", "low").to_string(),
            "[WARN] store: low"
        );
    }

    #[test]
    fn test_clone_eq() {
        let a = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        assert_eq!(a, a.clone());
    }

    #[test]
    fn test_debug() {
        let debug = format!("{:?}", AuraConfig::from_str(MINIMAL_TOML).unwrap());
        assert!(debug.contains("redis") && debug.contains("ingest"));
    }
}
