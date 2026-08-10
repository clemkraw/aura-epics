//! Metrics for aura-store.

use std::fmt;
use std::time::Instant;

/// Snapshot of all aura-store metrics.
#[derive(Debug, Clone)]
pub struct StoreMetrics {
    pub samples_received: u64,
    pub samples_stored: u64,
    pub samples_dropped: u64,
    pub batch_flushes: u64,
    pub rows_written: u64,
    pub last_flush_us: u64,
    pub avg_batch_size: f64,
    pub consumer_lag: u64,
    pub consumer_batches: u64,
    pub consumer_empty_reads: u64,
    pub consumer_efficiency: f64,
    pub cache_entries: usize,
    pub cache_hit_ratio: f64,
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
        }
    }
}

impl fmt::Display for StoreMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "recv={} stored={} dropped={} lag={} cache_hit={:.1}%",
            self.samples_received,
            self.samples_stored,
            self.samples_dropped,
            self.consumer_lag,
            self.cache_hit_ratio * 100.0
        )
    }
}

/// Simple duration measurement helper.
#[derive(Debug, Clone, Copy)]
pub struct Timer {
    start: Instant,
}

impl Timer {
    #[inline]
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }
    #[inline]
    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
    #[inline]
    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
    #[inline]
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

impl fmt::Display for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let us = self.elapsed_us();
        if us < 1_000 {
            write!(f, "{us}\u{b5}s")
        } else if us < 1_000_000 {
            write!(f, "{:.1}ms", us as f64 / 1_000.0)
        } else {
            write!(f, "{:.2}s", us as f64 / 1_000_000.0)
        }
    }
}
