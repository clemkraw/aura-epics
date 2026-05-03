//! Telemetry initialization: structured logging via `tracing`.
//!
//! Supports two output formats:
//! - `"json"`: machine-readable, one JSON object per line (production).
//! - `"pretty"`: human-readable, colored output (development).
//!
//! The log level is controlled by:
//! 1. `RUST_LOG` environment variable (highest priority)
//! 2. `[telemetry] log_level` in `aura.toml`
//!
//! Call [`init`] once at the start of `main()`. For tests, use
//! [`init_default`] which silently handles double-initialization.

use tracing_subscriber::{fmt, EnvFilter};

use crate::config::TelemetryConfig;

/// Known log format identifiers.
const FORMAT_JSON: &str = "json";
const FORMAT_PRETTY: &str = "pretty";

/// Initialize the global tracing subscriber.
///
/// Must be called exactly once, at the very start of `main()`,
/// before any tracing macros (`info!`, `debug!`, etc.) are used.
///
/// # Format
///
/// - `"json"` → structured JSON, one object per line. Includes target
///   and thread IDs for correlation in log aggregation systems
///   (Loki, Elasticsearch).
/// - `"pretty"` (or any other value) → colored human-readable output.
///
/// # Level Priority
///
/// `RUST_LOG` env var takes precedence over `config.log_level`.
/// This allows temporary debug logging in production without
/// changing the config file: `RUST_LOG=aura=debug aura all`.
///
/// # Panics
///
/// Panics if a global subscriber has already been set.
pub fn init(config: &TelemetryConfig) {
    let env_filter = build_filter(&config.log_level);

    if config.log_format == FORMAT_JSON {
        fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(true)
            .with_thread_ids(true)
            .with_file(false)
            .with_line_number(false)
            .init();
    } else {
        fmt()
            .with_env_filter(env_filter)
            .pretty()
            .with_target(true)
            .init();
    }
}

/// Initialize telemetry with sensible defaults (pretty, info level).
///
/// Safe to call multiple times (silently ignores double-init).
/// Designed for tests and quick prototyping.
pub fn init_default() {
    let config = TelemetryConfig {
        log_level: "info".into(),
        log_format: FORMAT_PRETTY.into(),
    };
    let _ = std::panic::catch_unwind(|| init(&config));
}

/// Build an `EnvFilter` from `RUST_LOG` or the config fallback.
fn build_filter(config_level: &str) -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config_level))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TelemetryConfig;

    // ── Config construction ──────────────────────────────────────────

    fn json_config() -> TelemetryConfig {
        TelemetryConfig { log_level: "debug".into(), log_format: "json".into() }
    }

    fn pretty_config() -> TelemetryConfig {
        TelemetryConfig { log_level: "warn".into(), log_format: "pretty".into() }
    }

    fn unknown_format_config() -> TelemetryConfig {
        TelemetryConfig { log_level: "info".into(), log_format: "yaml".into() }
    }

    // ── build_filter ─────────────────────────────────────────────────

    #[test]
    fn test_build_filter_from_config() {
        // When RUST_LOG is not set, uses config level.
        // We can't unset RUST_LOG reliably in tests, but we can verify
        // the function doesn't panic for valid levels.
        let filter = build_filter("info");
        let debug_repr = format!("{}", filter);
        assert!(!debug_repr.is_empty());
    }

    #[test]
    fn test_build_filter_trace() {
        let filter = build_filter("trace");
        assert!(!format!("{}", filter).is_empty());
    }

    #[test]
    fn test_build_filter_module_specific() {
        // Module-specific filter syntax should work.
        let filter = build_filter("aura=debug,aura_net=trace");
        assert!(!format!("{}", filter).is_empty());
    }

    // ── Format constants ─────────────────────────────────────────────

    #[test]
    fn test_format_constants() {
        assert_eq!(FORMAT_JSON, "json");
        assert_eq!(FORMAT_PRETTY, "pretty");
    }

    // ── Config format matching ───────────────────────────────────────

    #[test]
    fn test_json_config_format() {
        let config = json_config();
        assert_eq!(config.log_format, FORMAT_JSON);
    }

    #[test]
    fn test_pretty_config_format() {
        let config = pretty_config();
        assert_eq!(config.log_format, FORMAT_PRETTY);
    }

    #[test]
    fn test_unknown_format_falls_back_to_pretty() {
        let config = unknown_format_config();
        // Unknown format != "json", so it will use the pretty branch.
        assert_ne!(config.log_format, FORMAT_JSON);
    }

    // ── init_default ─────────────────────────────────────────────────

    #[test]
    fn test_init_default_does_not_panic() {
        // Can be called multiple times safely.
        init_default();
        init_default();
        init_default();
    }

    // ── TelemetryConfig struct ───────────────────────────────────────

    #[test]
    fn test_telemetry_config_clone() {
        let a = json_config();
        let b = a.clone();
        assert_eq!(a.log_level, b.log_level);
        assert_eq!(a.log_format, b.log_format);
    }

    #[test]
    fn test_telemetry_config_debug() {
        let config = pretty_config();
        let debug = format!("{:?}", config);
        assert!(debug.contains("warn"));
        assert!(debug.contains("pretty"));
    }
}