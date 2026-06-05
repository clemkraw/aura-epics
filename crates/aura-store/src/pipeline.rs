//! Main processing pipeline for `aura-store`.
//!
//! Orchestrates the data flow from SharedBuffer to TimescaleDB:
//! ```text
//! SharedBuffer → take_scalars()/take_others()
//!     → writer.ingest_scalars() / ingest_other_rows()
//!     → writer.dispatch_sync()  <- fallback async on cache miss
//!     → writer.flush_all()      <- parallel tokio::try_join!
//! ```
//!
//! ## Performance
//!
//! - **Parallel flush**: `flush_all()` uses `tokio::try_join!` to flush
//!   all 5 writers concurrently. Total latency = max(writers).
//! - **Flush timer**: if the batch is small but time exceeds
//!   `max_flush_interval_ms`, flush anyway to bound latency.
//! - **Ping-pong buffers**: `take_flush_bundle()` swaps buffers in O(1)
//!   so dispatch continues while the previous batch flushes in background.

use std::fmt;
use std::time::Instant;

use sqlx::postgres::PgPool;

use aura_core::config::AuraConfig;
use aura_core::error::AuraResult;

use crate::writer::{BatchWriter, BatchWriterConfig};

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum time (ms) between flushes, even with partial batches.
    pub max_flush_interval_ms: u64,
    /// Minimum batch size for adaptive batching.
    pub min_batch_size: usize,
    /// Maximum batch size for adaptive batching.
    pub max_batch_size: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_flush_interval_ms: 100,
            min_batch_size: 500,
            max_batch_size: 200_000,
        }
    }
}

impl PipelineConfig {
    pub fn from_aura_config(config: &AuraConfig) -> Self {
        Self {
            max_flush_interval_ms: config.store.flush_interval_ms,
            min_batch_size: 500,
            max_batch_size: config.store.batch_size,
        }
    }
}

/// The main processing pipeline. One instance per `aura-store` process.
pub struct Pipeline {
    config: PipelineConfig,
    writer: BatchWriter,

    iterations: u64,
    last_flush: Instant,
    total_process_us: u64,
    total_flush_us: u64,
    total_received: u64,
    total_stored: u64,
}

impl Pipeline {
    pub fn new(config: PipelineConfig) -> Self {
        let writer_config = BatchWriterConfig {
            scalar_batch_size: config.max_batch_size,
            string_batch_size: config.max_batch_size,
            array_batch_size: config.max_batch_size,
            json_batch_size: config.max_batch_size / 10,
            image_batch_size: 50,
        };
        Self {
            config,
            writer: BatchWriter::new(writer_config),
            iterations: 0,
            last_flush: Instant::now(),
            total_process_us: 0,
            total_flush_us: 0,
            total_received: 0,
            total_stored: 0,
        }
    }

    /// Warm PV cache at startup.
    pub async fn startup(&mut self, pool: &PgPool) -> AuraResult<()> {
        let cached = self.writer.warm_cache(pool).await?;
        tracing::info!(cached_pvs = cached, "PV cache warmed");
        self.last_flush = Instant::now();
        Ok(())
    }

    #[inline]
    fn any_writer_full(&self) -> bool {
        self.writer.scalar_writer().is_full()
            || self.writer.string_writer().is_full()
            || self.writer.array_writer().is_full()
            || self.writer.json_writer().is_full()
            || self.writer.image_writer().is_full()
    }

    #[inline]
    fn should_time_flush(&self) -> bool {
        self.last_flush.elapsed().as_millis() as u64 >= self.config.max_flush_interval_ms
    }

    /// Whether any writer buffer is full (public wrapper for store_loop).
    #[inline]
    pub fn any_writer_full_pub(&self) -> bool {
        self.any_writer_full()
    }

    /// Whether time-based flush is needed (public wrapper for store_loop).
    #[inline]
    pub fn should_time_flush_pub(&self) -> bool {
        self.should_time_flush()
    }


    #[inline]
    pub fn reset_last_flush(&mut self) {
        self.last_flush = Instant::now();
    }

    #[inline]
    pub fn add_flush_us(&mut self, us: u64) {
        self.total_flush_us += us;
    }

    #[inline]
    pub fn add_process_us(&mut self, us: u64) {
        self.total_process_us += us;
    }

    #[inline]
    pub fn add_received(&mut self, n: u64) {
        self.total_received += n;
    }

    #[inline]
    pub fn add_stored(&mut self, n: u64) {
        self.total_stored += n;
    }

    #[inline]
    pub fn inc_iterations(&mut self) {
        self.iterations += 1;
    }

    /// Register a tokio-postgres connection. Call N times at startup.
    pub fn add_copy_connection(&mut self, client: std::sync::Arc<tokio_postgres::Client>) {
        self.writer.copy_pool_mut().add(client);
    }

    /// Distribute the COPY pool to all writers. Call once after all connections added.
    pub fn finalize_copy_pool(&mut self) {
        let pool = self.writer.copy_pool().clone();
        self.writer.scalar_writer_mut().set_copy_pool(pool.clone());
        self.writer.string_writer_mut().set_copy_pool(pool.clone());
        self.writer.array_writer_mut().set_copy_pool(pool.clone());
        self.writer.json_writer_mut().set_copy_pool(pool.clone());
        self.writer.image_writer_mut().set_copy_pool(pool);
    }

    #[inline]
    pub fn writer(&self) -> &BatchWriter {
        &self.writer
    }

    #[inline]
    pub fn writer_mut(&mut self) -> &mut BatchWriter {
        &mut self.writer
    }

    #[inline]
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    #[inline]
    pub fn iterations(&self) -> u64 {
        self.iterations
    }

    #[inline]
    pub fn total_received(&self) -> u64 {
        self.total_received
    }

    #[inline]
    pub fn total_stored(&self) -> u64 {
        self.total_stored
    }

    #[inline]
    pub fn total_process_us(&self) -> u64 {
        self.total_process_us
    }

    #[inline]
    pub fn total_flush_us(&self) -> u64 {
        self.total_flush_us
    }

    #[inline]
    pub fn scalar_buffer_len(&self) -> usize {
        self.writer.scalar_writer().buffer_len()
    }

    #[inline]
    pub fn pv_cache_size(&self) -> usize {
        self.writer.pv_cache().len()
    }

    #[inline]
    pub fn total_build_us(&self) -> u64 {
        self.writer.scalar_writer().total_build_us()
    }

    #[inline]
    pub fn total_send_us(&self) -> u64 {
        self.writer.scalar_writer().total_send_us()
    }

    #[inline]
    pub fn total_written(&self) -> u64 {
        self.writer.scalar_writer().total_written() + self.writer.string_writer().total_written()
    }

    pub fn avg_process_us(&self) -> f64 {
        if self.iterations == 0 {
            return 0.0;
        }
        self.total_process_us as f64 / self.iterations as f64
    }

    pub fn avg_flush_us(&self) -> f64 {
        if self.iterations == 0 {
            return 0.0;
        }
        self.total_flush_us as f64 / self.iterations as f64
    }

    /// Fraction of total time spent in I/O (flush).
    pub fn io_ratio(&self) -> f64 {
        let total = self.total_process_us + self.total_flush_us;
        if total == 0 {
            return 0.0;
        }
        self.total_flush_us as f64 / total as f64
    }
}

impl fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipeline")
            .field("iterations", &self.iterations)
            .field("total_stored", &self.total_stored)
            .field("buffered", &self.writer.total_buffered())
            .field("io_ratio", &format!("{:.0}%", self.io_ratio() * 100.0))
            .finish()
    }
}

impl fmt::Display for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pipeline: {} iterations, {} received, {} stored, I/O ratio {:.0}%",
            self.iterations,
            self.total_received,
            self.total_stored,
            self.io_ratio() * 100.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_default() {
        let cfg = PipelineConfig::default();
        assert_eq!(cfg.max_flush_interval_ms, 100);
        assert_eq!(cfg.min_batch_size, 500);
        assert_eq!(cfg.max_batch_size, 200_000);
    }

    #[test]
    fn pipeline_new() {
        let p = Pipeline::new(PipelineConfig::default());
        assert_eq!(p.iterations(), 0);
        assert_eq!(p.total_process_us(), 0);
        assert_eq!(p.total_flush_us(), 0);
        assert_eq!(p.writer().total_buffered(), 0);
    }

    #[test]
    fn time_flush_fresh() {
        assert!(!Pipeline::new(PipelineConfig::default()).should_time_flush());
    }

    #[test]
    fn any_writer_full_empty() {
        assert!(!Pipeline::new(PipelineConfig::default()).any_writer_full());
    }

    #[test]
    fn io_ratio_zero() {
        assert_eq!(Pipeline::new(PipelineConfig::default()).io_ratio(), 0.0);
    }

    #[test]
    fn io_ratio_all_flush() {
        let mut p = Pipeline::new(PipelineConfig::default());
        p.total_flush_us = 100_000;
        assert_eq!(p.io_ratio(), 1.0);
    }

    #[test]
    fn io_ratio_balanced() {
        let mut p = Pipeline::new(PipelineConfig::default());
        p.total_process_us = 50_000;
        p.total_flush_us = 50_000;
        assert!((p.io_ratio() - 0.5).abs() < 0.001);
    }

    #[test]
    fn display() {
        let s = Pipeline::new(PipelineConfig::default()).to_string();
        assert!(s.contains("Pipeline") && s.contains("0 iterations"));
    }

    #[test]
    fn debug() {
        assert!(format!("{:?}", Pipeline::new(PipelineConfig::default())).contains("Pipeline"));
    }
}