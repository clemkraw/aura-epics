//! Prometheus metrics for `aura-store`.
//!
//! Exposes key performance indicators for monitoring:
//! - Samples received/stored/dropped per second
//! - Batch write latency and size
//! - Consumer lag and throughput
//! - PV cache hit ratio
//! - Per-PV filter epsilon and compression ratio
//!
//! Metrics are collected by Prometheus via scraping the `/metrics`
//! endpoint served by `aura-api`.

use std::fmt;
use std::time::Instant;

/// Snapshot of all aura-store metrics.
///
/// Collected periodically and exported to Prometheus.
#[derive(Debug, Clone)]
pub struct StoreMetrics {
    /// Total raw samples consumed from Redis.
    pub samples_received: u64,
    /// Total samples that passed the filter.
    pub samples_stored: u64,
    /// Total samples dropped by the filter.
    pub samples_dropped: u64,

    /// Total batch flush operations.
    pub batch_flushes: u64,
    /// Total rows written to TimescaleDB.
    pub rows_written: u64,
    /// Last batch write duration in microseconds.
    pub last_flush_us: u64,
    /// Average batch size (rows per flush).
    pub avg_batch_size: f64,

    /// Redis consumer lag (pending messages).
    pub consumer_lag: u64,
    /// Total XREADGROUP calls.
    pub consumer_batches: u64,
    /// Total empty reads (block timeout).
    pub consumer_empty_reads: u64,
    /// Consumer read efficiency (fraction of reads with data).
    pub consumer_efficiency: f64,

    /// PV cache entries.
    pub cache_entries: usize,
    /// PV cache hit ratio.
    pub cache_hit_ratio: f64,

    /// Number of active PV filters.
    pub active_filters: usize,
    /// Global compression ratio (stored/received).
    pub compression_ratio: f64,
}

impl Default for StoreMetrics {
    fn default() -> Self {
        Self {
            samples_received: 0,
            samples_stored: 0,
            samples_dropped: 0,
            batch_flushes: 0,
            rows_written: 0,
            last_flush_us: 0,
            avg_batch_size: 0.0,
            consumer_lag: 0,
            consumer_batches: 0,
            consumer_empty_reads: 0,
            consumer_efficiency: 0.0,
            cache_entries: 0,
            cache_hit_ratio: 1.0,
            active_filters: 0,
            compression_ratio: 1.0,
        }
    }
}

impl fmt::Display for StoreMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "recv={} stored={} dropped={} lag={} filters={} cache_hit={:.1}% compress={:.1}%",
            self.samples_received,
            self.samples_stored,
            self.samples_dropped,
            self.consumer_lag,
            self.active_filters,
            self.cache_hit_ratio * 100.0,
            self.compression_ratio * 100.0
        )
    }
}

/// Simple duration measurement helper.
///
/// Measures elapsed time between `start()` and `elapsed_us()`.
/// Zero-allocation, inline-friendly.
#[derive(Debug, Clone, Copy)]
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Start a new timer.
    #[inline]
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Elapsed time in microseconds.
    #[inline]
    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }

    /// Elapsed time in milliseconds.
    #[inline]
    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Elapsed time as fractional seconds.
    #[inline]
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

impl fmt::Display for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let us = self.elapsed_us();
        if us < 1_000 {
            write!(f, "{}µs", us)
        } else if us < 1_000_000 {
            write!(f, "{:.1}ms", us as f64 / 1_000.0)
        } else {
            write!(f, "{:.2}s", us as f64 / 1_000_000.0)
        }
    }
}

/// Per-PV metrics snapshot (for the /api/v1/metrics/:pv endpoint).
#[derive(Debug, Clone)]
pub struct PvMetricsSnapshot {
    pub pv_name: String,
    pub received: u64,
    pub stored: u64,
    pub dropped: u64,
    pub compression_ratio: f64,
    pub effective_epsilon: f64,
    pub is_calibrated: bool,
}

impl PvMetricsSnapshot {
    /// Create from a PvFilter.
    pub fn from_filter(filter: &crate::filter::pv_filter::PvFilter) -> Self {
        Self {
            pv_name: filter.pv_name().to_string(),
            received: filter.received(),
            stored: filter.stored(),
            dropped: filter.dropped(),
            compression_ratio: filter.compression_ratio(),
            effective_epsilon: filter.effective_epsilon(),
            is_calibrated: filter.is_calibrated(),
        }
    }
}

impl fmt::Display for PvMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: recv={} stored={} ε={:.6} compress={:.1}%",
            self.pv_name,
            self.received,
            self.stored,
            self.effective_epsilon,
            self.compression_ratio * 100.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── StoreMetrics ─────────────────────────────────────────────────

    #[test]
    fn test_metrics_default() {
        let m = StoreMetrics::default();
        assert_eq!(m.samples_received, 0);
        assert_eq!(m.samples_stored, 0);
        assert_eq!(m.consumer_lag, 0);
        assert_eq!(m.cache_hit_ratio, 1.0);
    }

    #[test]
    fn test_metrics_display() {
        let m = StoreMetrics {
            samples_received: 1000,
            samples_stored: 100,
            samples_dropped: 900,
            consumer_lag: 50,
            active_filters: 10,
            cache_hit_ratio: 0.999,
            compression_ratio: 0.1,
            ..Default::default()
        };
        let s = m.to_string();
        assert!(s.contains("recv=1000"));
        assert!(s.contains("stored=100"));
        assert!(s.contains("lag=50"));
        assert!(s.contains("99.9%")); // cache hit
    }

    #[test]
    fn test_metrics_clone() {
        let a = StoreMetrics::default();
        let b = a.clone();
        assert_eq!(a.samples_received, b.samples_received);
    }

    // ── Timer ────────────────────────────────────────────────────────

    #[test]
    fn test_timer_start() {
        let t = Timer::start();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(t.elapsed_us() >= 4_000); // at least 4ms
        assert!(t.elapsed_ms() >= 4);
        assert!(t.elapsed_secs() >= 0.004);
    }

    #[test]
    fn test_timer_display_microseconds() {
        let t = Timer::start();
        // Immediately — should be < 1ms
        let s = t.to_string();
        assert!(s.contains("µs") || s.contains("ms"));
    }

    #[test]
    fn test_timer_display_milliseconds() {
        let t = Timer::start();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let s = t.to_string();
        assert!(s.contains("ms") || s.contains("µs"));
    }

    #[test]
    fn test_timer_copy() {
        let a = Timer::start();
        let b = a; // Copy
        assert!(b.elapsed_us() >= a.elapsed_us().saturating_sub(100));
    }

    // ── PvMetricsSnapshot ────────────────────────────────────────────

    #[test]
    fn test_pv_metrics_display() {
        let snap = PvMetricsSnapshot {
            pv_name: "CRYO:TEMP".to_string(),
            received: 10000,
            stored: 500,
            dropped: 9500,
            compression_ratio: 0.05,
            effective_epsilon: 0.003,
            is_calibrated: true,
        };
        let s = snap.to_string();
        assert!(s.contains("CRYO:TEMP"));
        assert!(s.contains("recv=10000"));
        assert!(s.contains("stored=500"));
        assert!(s.contains("ε=0.003"));
    }

    #[test]
    fn test_pv_metrics_clone() {
        let a = PvMetricsSnapshot {
            pv_name: "PV".to_string(),
            received: 0,
            stored: 0,
            dropped: 0,
            compression_ratio: 1.0,
            effective_epsilon: 0.0,
            is_calibrated: false,
        };
        let b = a.clone();
        assert_eq!(a.pv_name, b.pv_name);
    }
}