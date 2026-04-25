//! Centralized error types for the AURA system.
//!
//! All crates use [`AuraError`] for error propagation.
//! This ensures consistent error handling and reporting across
//! the entire pipeline.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum AuraError {
    /// I/O error (file, network socket, WAL).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Redis connection or command error.
    #[error("Redis error: {0}")]
    Redis(String),

    /// TimescaleDB/PostgreSQL error.
    #[error("Database error: {0}")]
    Database(String),

    /// PVAccess protocol error.
    #[error("PVA protocol error: {0}")]
    Pva(String),

    /// Configuration error (missing file, invalid TOML, bad values).
    #[error("Configuration error: {0}")]
    Config(String),

    /// Serialization/deserialization error (JSON, Redis messages).
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// A PV operation failed (subscribe, unsubscribe, lookup).
    #[error("PV error: {detail} (pv: {pv_name})")]
    Pv { pv_name: String, detail: String },

    /// A required service is unavailable (Redis down, DB unreachable).
    #[error("Service unavailable: {service} — {reason}")]
    ServiceUnavailable { service: String, reason: String },

    /// Operation timed out.
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Internal logic error.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Convenience alias used throughout the codebase.
pub type AuraResult<T> = Result<T, AuraError>;

impl AuraError {
    /// Create a Redis error.
    pub fn redis(msg: impl Into<String>) -> Self {
        Self::Redis(msg.into())
    }

    /// Create a database error.
    pub fn database(msg: impl Into<String>) -> Self {
        Self::Database(msg.into())
    }

    /// Create a PVA protocol error.
    pub fn pva(msg: impl Into<String>) -> Self {
        Self::Pva(msg.into())
    }

    /// Create a configuration error.
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a serialization error.
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
    }

    /// Create a PV-specific error.
    pub fn pv(pv_name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Pv {
            pv_name: pv_name.into(),
            detail: detail.into(),
        }
    }

    /// Create a service unavailable error.
    pub fn service_unavailable(service: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ServiceUnavailable {
            service: service.into(),
            reason: reason.into(),
        }
    }

    /// Create a timeout error.
    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::Timeout(msg.into())
    }

    /// Create an internal error.
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;

    // ── Thread safety ────────────────────────────────────────────────

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<AuraError>();
    }

    // ── Display messages ───────────────────────────

    #[test]
    fn test_msg_io() {
        let e: AuraError = std::io::Error::new(std::io::ErrorKind::NotFound, "gone").into();
        assert_eq!(e.to_string(), "I/O error: gone");
    }

    #[test]
    fn test_msg_redis() {
        assert_eq!(AuraError::redis("timeout").to_string(), "Redis error: timeout");
    }

    #[test]
    fn test_msg_database() {
        assert_eq!(AuraError::database("syntax").to_string(), "Database error: syntax");
    }

    #[test]
    fn test_msg_pva() {
        assert_eq!(AuraError::pva("bad magic").to_string(), "PVA protocol error: bad magic");
    }

    #[test]
    fn test_msg_config() {
        assert_eq!(AuraError::config("missing key").to_string(), "Configuration error: missing key");
    }

    #[test]
    fn test_msg_serialization() {
        assert_eq!(AuraError::serialization("bad json").to_string(), "Serialization error: bad json");
    }

    #[test]
    fn test_msg_pv() {
        let e = AuraError::pv("CRYO:TEMP", "not found");
        assert_eq!(e.to_string(), "PV error: not found (pv: CRYO:TEMP)");
    }

    #[test]
    fn test_msg_service_unavailable() {
        let e = AuraError::service_unavailable("Redis", "refused");
        assert_eq!(e.to_string(), "Service unavailable: Redis — refused");
    }

    #[test]
    fn test_msg_timeout() {
        assert_eq!(AuraError::timeout("5000ms").to_string(), "Timeout: 5000ms");
    }

    #[test]
    fn test_msg_internal() {
        assert_eq!(AuraError::internal("bad state").to_string(), "Internal error: bad state");
    }

    // ── Error source chain (#[from]) ─────────────────────────────────

    #[test]
    fn test_io_from_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe");
        let e: AuraError = io_err.into();
        assert!(matches!(e, AuraError::Io(_)));
    }

    #[test]
    fn test_io_source_preserved() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let e: AuraError = io_err.into();
        let source = StdError::source(&e);
        assert!(source.is_some());
        assert!(source.unwrap().to_string().contains("denied"));
    }

    #[test]
    fn test_non_from_variants_have_no_source() {
        let variants: Vec<AuraError> = vec![
            AuraError::redis("x"),
            AuraError::database("x"),
            AuraError::pva("x"),
            AuraError::config("x"),
            AuraError::serialization("x"),
            AuraError::pv("PV", "x"),
            AuraError::service_unavailable("S", "x"),
            AuraError::timeout("x"),
            AuraError::internal("x"),
        ];
        for e in &variants {
            assert!(StdError::source(e).is_none(),
                    "expected no source for {}", e);
        }
    }

    // ── AuraResult alias ─────────────────────────────────────────────

    #[test]
    fn test_result_ok() {
        let r: AuraResult<i32> = Ok(42);
        assert_eq!(r.unwrap(), 42);
    }

    #[test]
    fn test_result_err() {
        let r: AuraResult<()> = Err(AuraError::internal("fail"));
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("fail"));
    }

    // ── Debug trait ──────────────────────────────────────────────────

    #[test]
    fn test_debug_output() {
        let e = AuraError::pv("MAG:I", "timeout");
        let debug = format!("{:?}", e);
        assert!(debug.contains("MAG:I"));
        assert!(debug.contains("timeout"));
    }

    // ── Helpers accept &str and String ───────────────────────────────

    #[test]
    fn test_helpers_accept_str_and_string() {
        // &str
        let _ = AuraError::redis("msg");
        let _ = AuraError::pv("pv", "detail");
        // String
        let _ = AuraError::redis(String::from("msg"));
        let _ = AuraError::pv(String::from("pv"), String::from("detail"));
        // format!
        let _ = AuraError::redis(format!("failed: {}", 42));
    }
}