//! aura-ingest — PVAccess monitor event processing pipeline.
//!
//! Receives PVA monitor events from EPICS IOCs via the MonitorBus,
//! decodes them through a zero-alloc fast path (ScalarDelta, StringDelta,
//! ArrayDelta) or a converter slow path (Value → NormativeType), and
//! pushes WriterRows to the SharedBuffer for binary COPY to TimescaleDB.
//!
//! Also handles per-PV heartbeat emission for stable PVs (MDEL deadband).

pub mod converter;
pub mod engine;
pub mod health;
pub mod heartbeat;
pub mod metrics;
pub mod thread;
