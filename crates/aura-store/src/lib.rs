//! # aura-store
//!
//! Database access layer for AURA-EPICS.
//!
//! 1. Drains PV updates from per-thread SharedBuffers
//! 2. Batch-writes samples to TimescaleDB via binary COPY
//! 3. Provides query functions for `aura-api`
//!
//! ## Architecture
//!
//! ```text
//! SharedBuffer (per ingest thread)
//!       │
//!       ▼
//!   store_loop ──► writer/ ──► TimescaleDB
//!       │              │
//!       │              └── binary COPY (parallel, N connections)
//!       └── Redis spill drain (safety valve)
//! ```
//!
//! ## Modules
//!
//! - [`writer`] — Batch writers for all hypertables (scalar, array, image, JSON)
//! - [`pipeline`] — Store pipeline (SharedBuffer -> writers → COPY)
//! - [`store_loop`] — Main tokio task: drain + flush + background COPY
//! - [`migrations`] — Embedded SQL migrations
//! - [`pv_config`] — CRUD on pv_config table
//! - [`metadata`] — PV metadata read/write
//! - [`alerts`] — Alert log read/write
//! - [`reader`] — Query functions for aura-api (raw + aggregated)
//! - [`metrics`] — Store metrics snapshot

pub mod alerts;
pub mod metadata;
pub mod metrics;
pub mod migrations;
pub mod pipeline;
pub mod pv_config;
pub mod reader;
pub mod store_loop;
pub mod writer;

pub use migrations::Migrations;
pub use pipeline::Pipeline;
