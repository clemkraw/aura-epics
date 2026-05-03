//! Application configuration, deserialized from `aura.toml`.
//!
//! Read once at startup. Runtime changes (PV epsilon, heartbeat) are
//! handled via the `pv_config` table in TimescaleDB, not by reloading this file.
//!
//! Every field has a sensible default — a minimal TOML with empty
//! sections (`[redis]\n[database]\n...`) produces a valid config.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;

/// Top-level configuration for the AURA system.
///
/// Mirrors the structure of `aura.toml` exactly — one field per TOML section.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct AuraConfig {
    pub redis: RedisConfig,
    pub database: DatabaseConfig,
    pub discover: DiscoverConfig,
    pub ingest: IngestConfig,
    pub filter: FilterConfig,
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

    /// Parse configuration from a TOML string.
    pub fn from_str(toml: &str) -> Result<Self, crate::AuraError> {
        toml::from_str(toml)
            .map_err(|e| crate::AuraError::config(format!("invalid TOML: {e}")))
    }

    /// Validate configuration for logical consistency.
    /// Returns a list of warnings (non-fatal) and errors (fatal).
    pub fn validate(&self) -> Vec<ConfigIssue> {
        let mut issues = Vec::new();

        if self.discover.liveness_suspect_s >= self.discover.liveness_offline_s {
            issues.push(ConfigIssue::error(
                "discover", "liveness_suspect_s must be < liveness_offline_s"
            ));
        }

        if self.filter.calibration_sigma <= 0.0 {
            issues.push(ConfigIssue::error(
                "filter", "calibration_sigma must be > 0"
            ));
        }

        if self.filter.calibration_window < 4 {
            issues.push(ConfigIssue::error(
                "filter", "calibration_window must be >= 4"
            ));
        }

        if self.store.batch_size == 0 {
            issues.push(ConfigIssue::error(
                "store", "batch_size must be > 0"
            ));
        }

        if self.filter.default_epsilon < 0.0 {
            issues.push(ConfigIssue::error(
                "filter", "default_epsilon must be >= 0"
            ));
        }

        if self.filter.default_heartbeat_s <= 0.0 {
            issues.push(ConfigIssue::error(
                "filter", "default_heartbeat_s must be > 0"
            ));
        }

        if self.ingest.redis_batch_size == 0 {
            issues.push(ConfigIssue::warning(
                "ingest", "redis_batch_size = 0, every sample will be an individual XADD"
            ));
        }

        issues
    }
}

/// A configuration validation issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigIssue {
    /// The config section where the issue was found.
    pub section: String,
    /// Description of the issue.
    pub message: String,
    /// Whether this is fatal.
    pub is_error: bool,
}

impl ConfigIssue {
    pub fn error(section: &str, message: &str) -> Self {
        Self { section: section.into(), message: message.into(), is_error: true }
    }

    pub fn warning(section: &str, message: &str) -> Self {
        Self { section: section.into(), message: message.into(), is_error: false }
    }
}

impl fmt::Display for ConfigIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = if self.is_error { "ERROR" } else { "WARN" };
        write!(f, "[{}] {}: {}", level, self.section, self.message)
    }
}

/// Redis connection and stream configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RedisConfig {
    /// Redis connection URL.
    #[serde(default = "d::redis_url")]
    pub url: String,
    /// Redis Stream name for sample transport.
    #[serde(default = "d::stream_name")]
    pub stream: String,
    /// Consumer group for aura-store instances.
    #[serde(default = "d::consumer_group")]
    pub consumer_group: String,
    /// Maximum stream length (MAXLEN ~). Oldest trimmed.
    #[serde(default = "d::max_stream_len")]
    pub max_stream_len: u64,
}

/// PostgreSQL / TimescaleDB connection configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DatabaseConfig {
    /// Connection URL.
    #[serde(default = "d::database_url")]
    pub url: String,
    /// Max connections in the pool.
    #[serde(default = "d::max_connections")]
    pub max_connections: u32,
}

/// IOC discovery and PV registry polling configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DiscoverConfig {
    /// Poll interval for pv_config changes (seconds).
    #[serde(default = "d::config_poll_interval")]
    pub config_poll_interval_s: u64,
    /// Seconds without beacon → SUSPECT.
    #[serde(default = "d::liveness_suspect")]
    pub liveness_suspect_s: u64,
    /// Seconds without beacon → OFFLINE.
    #[serde(default = "d::liveness_offline")]
    pub liveness_offline_s: u64,
}

/// Ingest shard configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct IngestConfig {
    /// Shard ID for this instance.
    #[serde(default)]
    pub shard_id: i32,
    /// PVA connection timeout (ms).
    #[serde(default = "d::pva_timeout")]
    pub pva_timeout_ms: u64,
    /// WAL directory for Redis failure recovery.
    #[serde(default = "d::wal_dir")]
    pub wal_dir: String,
    /// Samples per Redis XADD batch.
    #[serde(default = "d::redis_batch_size")]
    pub redis_batch_size: usize,
    /// Max ms before flushing incomplete batch to Redis.
    #[serde(default = "d::redis_flush_interval")]
    pub redis_flush_interval_ms: u64,
}

/// Epsilon-deadband filter configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FilterConfig {
    /// Default epsilon (0.0 = store everything pre-calibration).
    #[serde(default)]
    pub default_epsilon: f64,
    /// Default heartbeat interval (seconds).
    #[serde(default = "d::heartbeat")]
    pub default_heartbeat_s: f64,
    /// Auto-calibrate epsilon from noise.
    #[serde(default = "d::auto_calibrate")]
    pub auto_calibrate: bool,
    /// Window size for noise estimation (samples).
    #[serde(default = "d::calibration_window")]
    pub calibration_window: usize,
    /// Sigma multiplier: ε = k × σ.
    #[serde(default = "d::calibration_sigma")]
    pub calibration_sigma: f64,
    /// Recalibration interval (seconds, 0 = once).
    #[serde(default = "d::recalibrate_interval")]
    pub recalibrate_interval_s: u64,
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

/// REST API server configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ApiConfig {
    /// HTTP bind address.
    #[serde(default = "d::api_bind")]
    pub bind: String,
}

/// Prometheus metrics endpoint configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MetricsConfig {
    /// HTTP bind address.
    #[serde(default = "d::metrics_bind")]
    pub bind: String,
}

/// Logging / tracing configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct TelemetryConfig {
    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "d::log_level")]
    pub log_level: String,
    /// Log format: "json" (prod) or "pretty" (dev).
    #[serde(default = "d::log_format")]
    pub log_format: String,
}

// ── Defaults (namespaced to avoid pollution) ─────────────────────────

mod d {
    pub fn redis_url() -> String           { "redis://127.0.0.1:6379".into() }
    pub fn stream_name() -> String         { "aura:samples".into() }
    pub fn consumer_group() -> String      { "aura-writers".into() }
    pub fn max_stream_len() -> u64         { 1_000_000 }
    pub fn database_url() -> String        { "postgresql://aura:aura@localhost/aura".into() }
    pub fn max_connections() -> u32        { 20 }
    pub fn config_poll_interval() -> u64   { 30 }
    pub fn liveness_suspect() -> u64       { 45 }
    pub fn liveness_offline() -> u64       { 150 }
    pub fn pva_timeout() -> u64            { 5000 }
    pub fn wal_dir() -> String             { "/var/lib/aura/wal".into() }
    pub fn redis_batch_size() -> usize     { 256 }
    pub fn redis_flush_interval() -> u64   { 50 }
    pub fn heartbeat() -> f64              { 60.0 }
    pub fn auto_calibrate() -> bool        { true }
    pub fn calibration_window() -> usize   { 128 }
    pub fn calibration_sigma() -> f64      { 3.0 }
    pub fn recalibrate_interval() -> u64   { 3600 }
    pub fn store_batch_size() -> usize     { 500 }
    pub fn store_flush_interval() -> u64   { 100 }
    pub fn api_bind() -> String            { "0.0.0.0:8080".into() }
    pub fn metrics_bind() -> String        { "0.0.0.0:9090".into() }
    pub fn log_level() -> String           { "info".into() }
    pub fn log_format() -> String          { "json".into() }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_TOML: &str = r#"
[redis]
[database]
[discover]
[ingest]
[filter]
[store]
[api]
[metrics]
[telemetry]
"#;

    const FULL_TOML: &str = r#"
[redis]
url = "redis://prod:6379"
stream = "aura:prod"
consumer_group = "writers-prod"
max_stream_len = 5_000_000

[database]
url = "postgresql://prod:secret@db/aura"
max_connections = 50

[discover]
config_poll_interval_s = 10
liveness_suspect_s = 30
liveness_offline_s = 120

[ingest]
shard_id = 3
pva_timeout_ms = 3000
wal_dir = "/data/wal"
redis_batch_size = 512
redis_flush_interval_ms = 25

[filter]
default_epsilon = 0.01
default_heartbeat_s = 30.0
auto_calibrate = false
calibration_window = 256
calibration_sigma = 4.0
recalibrate_interval_s = 7200

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

    // ── Parsing ──────────────────────────────────────────────────────

    #[test]
    fn test_minimal_defaults() {
        let cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();

        // Redis
        assert_eq!(cfg.redis.url, "redis://127.0.0.1:6379");
        assert_eq!(cfg.redis.stream, "aura:samples");
        assert_eq!(cfg.redis.consumer_group, "aura-writers");
        assert_eq!(cfg.redis.max_stream_len, 1_000_000);

        // Database
        assert_eq!(cfg.database.url, "postgresql://aura:aura@localhost/aura");
        assert_eq!(cfg.database.max_connections, 20);

        // Discover
        assert_eq!(cfg.discover.config_poll_interval_s, 30);
        assert_eq!(cfg.discover.liveness_suspect_s, 45);
        assert_eq!(cfg.discover.liveness_offline_s, 150);

        // Ingest
        assert_eq!(cfg.ingest.shard_id, 0);
        assert_eq!(cfg.ingest.pva_timeout_ms, 5000);
        assert_eq!(cfg.ingest.wal_dir, "/var/lib/aura/wal");
        assert_eq!(cfg.ingest.redis_batch_size, 256);
        assert_eq!(cfg.ingest.redis_flush_interval_ms, 50);

        // Filter
        assert_eq!(cfg.filter.default_epsilon, 0.0);
        assert_eq!(cfg.filter.default_heartbeat_s, 60.0);
        assert!(cfg.filter.auto_calibrate);
        assert_eq!(cfg.filter.calibration_window, 128);
        assert_eq!(cfg.filter.calibration_sigma, 3.0);
        assert_eq!(cfg.filter.recalibrate_interval_s, 3600);

        // Store
        assert_eq!(cfg.store.batch_size, 500);
        assert_eq!(cfg.store.flush_interval_ms, 100);

        // API
        assert_eq!(cfg.api.bind, "0.0.0.0:8080");

        // Metrics
        assert_eq!(cfg.metrics.bind, "0.0.0.0:9090");

        // Telemetry
        assert_eq!(cfg.telemetry.log_level, "info");
        assert_eq!(cfg.telemetry.log_format, "json");
    }

    #[test]
    fn test_full_custom_config() {
        let cfg = AuraConfig::from_str(FULL_TOML).unwrap();

        assert_eq!(cfg.redis.url, "redis://prod:6379");
        assert_eq!(cfg.redis.stream, "aura:prod");
        assert_eq!(cfg.redis.consumer_group, "writers-prod");
        assert_eq!(cfg.redis.max_stream_len, 5_000_000);

        assert_eq!(cfg.database.url, "postgresql://prod:secret@db/aura");
        assert_eq!(cfg.database.max_connections, 50);

        assert_eq!(cfg.discover.config_poll_interval_s, 10);
        assert_eq!(cfg.discover.liveness_suspect_s, 30);
        assert_eq!(cfg.discover.liveness_offline_s, 120);

        assert_eq!(cfg.ingest.shard_id, 3);
        assert_eq!(cfg.ingest.pva_timeout_ms, 3000);
        assert_eq!(cfg.ingest.wal_dir, "/data/wal");
        assert_eq!(cfg.ingest.redis_batch_size, 512);
        assert_eq!(cfg.ingest.redis_flush_interval_ms, 25);

        assert_eq!(cfg.filter.default_epsilon, 0.01);
        assert_eq!(cfg.filter.default_heartbeat_s, 30.0);
        assert!(!cfg.filter.auto_calibrate);
        assert_eq!(cfg.filter.calibration_window, 256);
        assert_eq!(cfg.filter.calibration_sigma, 4.0);
        assert_eq!(cfg.filter.recalibrate_interval_s, 7200);

        assert_eq!(cfg.store.batch_size, 1000);
        assert_eq!(cfg.store.flush_interval_ms, 200);

        assert_eq!(cfg.api.bind, "0.0.0.0:9000");
        assert_eq!(cfg.metrics.bind, "0.0.0.0:9191");

        assert_eq!(cfg.telemetry.log_level, "debug");
        assert_eq!(cfg.telemetry.log_format, "pretty");
    }

    // ── Partial overrides ────────────────────────────────────────────

    #[test]
    fn test_partial_override_keeps_defaults() {
        let toml = r#"
[redis]
url = "redis://custom:6379"

[database]
[discover]
[ingest]
[filter]
[store]
[api]
[metrics]
[telemetry]
"#;
        let cfg = AuraConfig::from_str(toml).unwrap();
        assert_eq!(cfg.redis.url, "redis://custom:6379");
        assert_eq!(cfg.redis.stream, "aura:samples"); // default kept
        assert_eq!(cfg.redis.max_stream_len, 1_000_000); // default kept
    }

    // ── Error handling ───────────────────────────────────────────────

    #[test]
    fn test_invalid_toml() {
        let result = AuraConfig::from_str("not valid toml {{{");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("invalid TOML"));
    }

    #[test]
    fn test_missing_file() {
        let result = AuraConfig::from_file("/nonexistent/aura.toml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot read config"));
    }

    #[test]
    fn test_missing_section() {
        let result = AuraConfig::from_str("[redis]\n");
        assert!(result.is_err()); // missing required sections
    }

    // ── Validation ───────────────────────────────────────────────────

    #[test]
    fn test_validate_default_config_passes() {
        let cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        let issues = cfg.validate();
        let errors: Vec<_> = issues.iter().filter(|i| i.is_error).collect();
        assert!(errors.is_empty(), "default config should have no errors: {:?}", errors);
    }

    #[test]
    fn test_validate_liveness_order() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.discover.liveness_suspect_s = 200;
        cfg.discover.liveness_offline_s = 100; // suspect > offline → error
        let issues = cfg.validate();
        assert!(issues.iter().any(|i| i.is_error && i.section == "discover"));
    }

    #[test]
    fn test_validate_negative_sigma() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.filter.calibration_sigma = -1.0;
        let issues = cfg.validate();
        assert!(issues.iter().any(|i| i.is_error && i.message.contains("sigma")));
    }

    #[test]
    fn test_validate_tiny_window() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.filter.calibration_window = 2;
        let issues = cfg.validate();
        assert!(issues.iter().any(|i| i.is_error && i.message.contains("window")));
    }

    #[test]
    fn test_validate_zero_batch_size() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.store.batch_size = 0;
        let issues = cfg.validate();
        assert!(issues.iter().any(|i| i.is_error && i.message.contains("batch_size")));
    }

    #[test]
    fn test_validate_negative_epsilon() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.filter.default_epsilon = -0.5;
        let issues = cfg.validate();
        assert!(issues.iter().any(|i| i.is_error && i.message.contains("epsilon")));
    }

    #[test]
    fn test_validate_zero_heartbeat() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.filter.default_heartbeat_s = 0.0;
        let issues = cfg.validate();
        assert!(issues.iter().any(|i| i.is_error && i.message.contains("heartbeat")));
    }

    #[test]
    fn test_validate_zero_redis_batch_warning() {
        let mut cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        cfg.ingest.redis_batch_size = 0;
        let issues = cfg.validate();
        let warnings: Vec<_> = issues.iter().filter(|i| !i.is_error).collect();
        assert!(!warnings.is_empty());
    }

    // ── Serde roundtrip ──────────────────────────────────────────────

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

    // ── ConfigIssue ──────────────────────────────────────────────────

    #[test]
    fn test_config_issue_display_error() {
        let issue = ConfigIssue::error("filter", "bad sigma");
        assert_eq!(issue.to_string(), "[ERROR] filter: bad sigma");
        assert!(issue.is_error);
    }

    #[test]
    fn test_config_issue_display_warning() {
        let issue = ConfigIssue::warning("ingest", "batch=0");
        assert_eq!(issue.to_string(), "[WARN] ingest: batch=0");
        assert!(!issue.is_error);
    }

    // ── Clone / PartialEq ────────────────────────────────────────────

    #[test]
    fn test_clone_eq() {
        let a = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_ne() {
        let a = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        let b = AuraConfig::from_str(FULL_TOML).unwrap();
        assert_ne!(a, b);
    }

    // ── Debug ────────────────────────────────────────────────────────

    #[test]
    fn test_debug() {
        let cfg = AuraConfig::from_str(MINIMAL_TOML).unwrap();
        let debug = format!("{:?}", cfg);
        assert!(debug.contains("redis"));
        assert!(debug.contains("filter"));
    }
}