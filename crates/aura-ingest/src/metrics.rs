//! Prometheus-compatible metrics for AURA ingest pipeline.
//!
//! Single `Arc<IngestMetrics>` replaces 8 separate `Arc<AtomicU64>`.
//! All counters use `Relaxed` ordering.
//!
//! Used by:
//! - Ingest threads: increment events_received, events_published, fast_path, slow_path
//! - Main loop: increment disconnects/reconnects, set gauges, read snapshot for stats line
//! - Future HTTP server: render_prometheus() for /metrics endpoint

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Ingest pipeline metrics — all atomic, lock-free.
pub struct IngestMetrics {
    // Counters (monotonic, incremented by ingest threads):
    pub events_received: AtomicU64,
    pub events_published: AtomicU64,
    pub events_skipped: AtomicU64,
    /// ScalarDelta/StringDelta/ArrayDelta handled without converter.
    pub fast_path: AtomicU64,
    /// Value events that went through the converter (first event per PV, rare types).
    pub slow_path: AtomicU64,

    // Counters (monotonic, incremented by main loop lifecycle handler):
    pub disconnects: AtomicU64,
    pub reconnects: AtomicU64,
}

impl IngestMetrics {
    pub fn new() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_published: AtomicU64::new(0),
            events_skipped: AtomicU64::new(0),
            fast_path: AtomicU64::new(0),
            slow_path: AtomicU64::new(0),
            disconnects: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
        }
    }

    /// Read all metrics into an immutable snapshot.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let fp = self.fast_path.load(Relaxed);
        let sp = self.slow_path.load(Relaxed);
        MetricsSnapshot {
            events_received: self.events_received.load(Relaxed),
            events_published: self.events_published.load(Relaxed),
            events_skipped: self.events_skipped.load(Relaxed),
            fast_path: fp,
            slow_path: sp,
            fast_path_pct: if fp + sp > 0 { fp * 100 / (fp + sp) } else { 0 },
            disconnects: self.disconnects.load(Relaxed),
            reconnects: self.reconnects.load(Relaxed),
        }
    }

    /// Render Prometheus text exposition format.
    pub fn render_prometheus(&self, sessions: u64, active_pvs: u64, buf_pending: u64) -> String {
        let s = self.snapshot();
        format!(
            "# HELP aura_events_received Total PVA events received\n\
             # TYPE aura_events_received counter\n\
             aura_events_received {}\n\
             # HELP aura_events_published Total samples written to DB\n\
             # TYPE aura_events_published counter\n\
             aura_events_published {}\n\
             # HELP aura_events_skipped Total events skipped\n\
             # TYPE aura_events_skipped counter\n\
             aura_events_skipped {}\n\
             # HELP aura_fast_path Fast path events (zero-alloc decode)\n\
             # TYPE aura_fast_path counter\n\
             aura_fast_path {}\n\
             # HELP aura_slow_path Slow path events (converter decode)\n\
             # TYPE aura_slow_path counter\n\
             aura_slow_path {}\n\
             # HELP aura_disconnects Total IOC disconnects\n\
             # TYPE aura_disconnects counter\n\
             aura_disconnects {}\n\
             # HELP aura_reconnects Total IOC reconnects\n\
             # TYPE aura_reconnects counter\n\
             aura_reconnects {}\n\
             # HELP aura_active_sessions Current PVA TCP sessions\n\
             # TYPE aura_active_sessions gauge\n\
             aura_active_sessions {}\n\
             # HELP aura_active_pvs Current subscribed PVs\n\
             # TYPE aura_active_pvs gauge\n\
             aura_active_pvs {}\n\
             # HELP aura_buffer_pending Rows pending in SharedBuffer\n\
             # TYPE aura_buffer_pending gauge\n\
             aura_buffer_pending {}\n",
            s.events_received,
            s.events_published,
            s.events_skipped,
            s.fast_path,
            s.slow_path,
            s.disconnects,
            s.reconnects,
            sessions,
            active_pvs,
            buf_pending,
        )
    }
}

impl Default for IngestMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable snapshot of all metrics at a point in time.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub events_received: u64,
    pub events_published: u64,
    pub events_skipped: u64,
    pub fast_path: u64,
    pub slow_path: u64,
    /// Pre-computed fast_path percentage for display.
    pub fast_path_pct: u64,
    pub disconnects: u64,
    pub reconnects: u64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "events={} pub={} skip={} fast={}% dc={} rc={}",
            self.events_received,
            self.events_published,
            self.events_skipped,
            self.fast_path_pct,
            self.disconnects,
            self.reconnects
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_zeros() {
        let s = IngestMetrics::new().snapshot();
        assert_eq!(s.events_received, 0);
        assert_eq!(s.events_published, 0);
        assert_eq!(s.fast_path, 0);
    }

    #[test]
    fn test_counters() {
        let m = IngestMetrics::new();
        m.events_received.fetch_add(100, Relaxed);
        m.events_published.fetch_add(95, Relaxed);
        m.events_skipped.fetch_add(5, Relaxed);
        m.fast_path.fetch_add(90, Relaxed);
        m.slow_path.fetch_add(10, Relaxed);
        m.disconnects.fetch_add(1, Relaxed);
        m.reconnects.fetch_add(1, Relaxed);
        let s = m.snapshot();
        assert_eq!(s.events_received, 100);
        assert_eq!(s.events_published, 95);
        assert_eq!(s.events_skipped, 5);
        assert_eq!(s.fast_path, 90);
        assert_eq!(s.slow_path, 10);
        assert_eq!(s.fast_path_pct, 90);
        assert_eq!(s.disconnects, 1);
        assert_eq!(s.reconnects, 1);
    }

    #[test]
    fn test_fast_path_pct_zero() {
        let s = IngestMetrics::new().snapshot();
        assert_eq!(s.fast_path_pct, 0); // 0/0 = 0, not panic
    }

    #[test]
    fn test_fast_path_pct_100() {
        let m = IngestMetrics::new();
        m.fast_path.fetch_add(1000, Relaxed);
        assert_eq!(m.snapshot().fast_path_pct, 100);
    }

    #[test]
    fn test_prometheus_format() {
        let m = IngestMetrics::new();
        m.events_received.fetch_add(42, Relaxed);
        m.fast_path.fetch_add(40, Relaxed);
        let prom = m.render_prometheus(3, 100_000, 5000);
        assert!(prom.contains("aura_events_received 42"));
        assert!(prom.contains("aura_fast_path 40"));
        assert!(prom.contains("aura_active_sessions 3"));
        assert!(prom.contains("aura_active_pvs 100000"));
        assert!(prom.contains("aura_buffer_pending 5000"));
        assert!(prom.contains("# TYPE aura_events_received counter"));
        assert!(prom.contains("# TYPE aura_active_sessions gauge"));
    }

    #[test]
    fn test_prometheus_all_metrics() {
        let prom = IngestMetrics::new().render_prometheus(0, 0, 0);
        for name in [
            "events_received",
            "events_published",
            "events_skipped",
            "fast_path",
            "slow_path",
            "disconnects",
            "reconnects",
            "active_sessions",
            "active_pvs",
            "buffer_pending",
        ] {
            assert!(prom.contains(&format!("aura_{name}")), "missing {name}");
        }
    }

    #[test]
    fn test_snapshot_display() {
        let m = IngestMetrics::new();
        m.events_received.fetch_add(1000, Relaxed);
        m.fast_path.fetch_add(990, Relaxed);
        m.slow_path.fetch_add(10, Relaxed);
        let s = m.snapshot();
        let display = s.to_string();
        assert!(display.contains("events=1000"));
        assert!(display.contains("fast=99%"));
    }

    #[test]
    fn test_snapshot_serialize() {
        let s = IngestMetrics::new().snapshot();
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("events_received"));
        assert!(json.contains("fast_path_pct"));
    }

    #[test]
    fn test_default() {
        assert_eq!(IngestMetrics::default().snapshot().events_received, 0);
    }

    #[test]
    fn test_thread_safe() {
        use std::sync::Arc;
        let m = Arc::new(IngestMetrics::new());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let m = m.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        m.events_received.fetch_add(1, Relaxed);
                        m.fast_path.fetch_add(1, Relaxed);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.snapshot().events_received, 4000);
        assert_eq!(m.snapshot().fast_path, 4000);
    }
}