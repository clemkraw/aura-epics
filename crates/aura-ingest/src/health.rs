//! K8s health probes and status endpoint.
//!
//! All state is atomic - readable from any thread without locks.
//! Updated by the main loop and store loop; read by the future HTTP server.
//!
//! Probes:
//! - Liveness: DB reachable AND store loop flushed recently
//! - Readiness: initial PV subscribe completed

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

/// Shared health state - `Arc<HealthState>` passed to HTTP handlers.
pub struct HealthState {
    pub db_connected: AtomicBool,
    pub pva_connected: AtomicBool,
    pub initial_subscribe_done: AtomicBool,
    /// Wall-clock milliseconds since UNIX epoch of last successful DB COPY.
    last_store_epoch_ms: AtomicU64,
    /// Active PVA session count.
    pub active_sessions: AtomicU64,
    started_at: Instant,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            db_connected: AtomicBool::new(false),
            pva_connected: AtomicBool::new(false),
            initial_subscribe_done: AtomicBool::new(false),
            last_store_epoch_ms: AtomicU64::new(0),
            active_sessions: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }
}

impl HealthState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_db_connected(&self, v: bool) {
        self.db_connected.store(v, Relaxed);
    }
    pub fn set_pva_connected(&self, v: bool) {
        self.pva_connected.store(v, Relaxed);
    }
    pub fn set_subscribe_done(&self) {
        self.initial_subscribe_done.store(true, Relaxed);
    }
    pub fn set_active_sessions(&self, n: u64) {
        self.active_sessions.store(n, Relaxed);
    }

    /// Record a successful store flush.
    pub fn record_store_flush(&self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_store_epoch_ms.store(now_ms, Relaxed);
    }

    /// Liveness: DB connected AND store flushed within 10s.
    pub fn is_healthy(&self) -> bool {
        if !self.db_connected.load(Relaxed) {
            return false;
        }
        let last = self.last_store_epoch_ms.load(Relaxed);
        if last == 0 {
            return true;
        } // still starting
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now_ms.saturating_sub(last) < 10_000
    }

    /// Readiness: initial subscribe completed.
    pub fn is_ready(&self) -> bool {
        self.initial_subscribe_done.load(Relaxed)
    }

    pub fn uptime_s(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// JSON status for the /status endpoint.
    pub fn status_json(&self, store_rate: u64, buf_size: u64, pv_count: u64) -> serde_json::Value {
        serde_json::json!({
            "healthy": self.is_healthy(),
            "ready": self.is_ready(),
            "db_connected": self.db_connected.load(Relaxed),
            "pva_connected": self.pva_connected.load(Relaxed),
            "active_sessions": self.active_sessions.load(Relaxed),
            "uptime_s": self.uptime_s(),
            "store_rate": store_rate,
            "buffer_size": buf_size,
            "pv_count": pv_count,
        })
    }
}

impl std::fmt::Debug for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthState")
            .field("healthy", &self.is_healthy())
            .field("ready", &self.is_ready())
            .field("sessions", &self.active_sessions.load(Relaxed))
            .field("uptime_s", &self.uptime_s())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let h = HealthState::new();
        assert!(!h.db_connected.load(Relaxed));
        assert!(!h.is_ready());
    }

    #[test]
    fn test_not_healthy_no_db() {
        let h = HealthState::new();
        assert!(!h.is_healthy());
    }

    #[test]
    fn test_healthy_db_no_flush() {
        let h = HealthState::new();
        h.set_db_connected(true);
        assert!(h.is_healthy());
    }

    #[test]
    fn test_healthy_db_recent_flush() {
        let h = HealthState::new();
        h.set_db_connected(true);
        h.record_store_flush();
        assert!(h.is_healthy());
    }

    #[test]
    fn test_not_healthy_stale_flush() {
        let h = HealthState::new();
        h.set_db_connected(true);
        let old_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 20_000;
        h.last_store_epoch_ms.store(old_ms, Relaxed);
        assert!(!h.is_healthy());
    }

    #[test]
    fn test_ready_after_subscribe() {
        let h = HealthState::new();
        assert!(!h.is_ready());
        h.set_subscribe_done();
        assert!(h.is_ready());
    }

    #[test]
    fn test_setters() {
        let h = HealthState::new();
        h.set_db_connected(true);
        assert!(h.db_connected.load(Relaxed));
        h.set_pva_connected(true);
        assert!(h.pva_connected.load(Relaxed));
        h.set_active_sessions(4);
        assert_eq!(h.active_sessions.load(Relaxed), 4);
    }

    #[test]
    fn test_uptime() {
        let h = HealthState::new();
        assert!(h.uptime_s() < 2);
    }

    #[test]
    fn test_status_json() {
        let h = HealthState::new();
        h.set_db_connected(true);
        h.set_subscribe_done();
        h.set_active_sessions(3);
        let json = h.status_json(500_000, 1024, 100_000);
        assert_eq!(json["ready"], true);
        assert_eq!(json["db_connected"], true);
        assert_eq!(json["active_sessions"], 3);
        assert_eq!(json["store_rate"], 500_000);
        assert_eq!(json["pv_count"], 100_000);
    }

    #[test]
    fn test_debug() {
        assert!(format!("{:?}", HealthState::new()).contains("HealthState"));
    }

    #[test]
    fn test_thread_safe() {
        use std::sync::Arc;
        let h = Arc::new(HealthState::new());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let h = h.clone();
                std::thread::spawn(move || {
                    h.set_db_connected(true);
                    h.record_store_flush();
                    assert!(h.is_healthy());
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
