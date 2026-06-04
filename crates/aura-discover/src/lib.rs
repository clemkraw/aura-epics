//! aura-discover - PV resolution and config reconciliation.
//!
//! Reconciles the desired state (pv_config table) with the real state (EPICS network).
//! Operators insert PV names in the DB; Discover does the rest: search, introspect, subscribe.
//!
//! ## Modules
//!
//! - `config_poller`: incremental DB polling (O(delta), not O(N))
//! - `command_publisher`: subscribe/unsubscribe commands to Redis
//! - `reconciler`: diff desired vs actual, generate alerts
//! - `orchestrator`: spawn tasks, run reconciliation loop
//! - `pg_notify`: PostgreSQL LISTEN/NOTIFY for real-time config changes

pub mod config_poller;
pub mod command_publisher;
pub mod reconciler;
pub mod orchestrator;
pub mod pg_notify;

pub use orchestrator::Orchestrator;
pub use config_poller::{ConfigPoller, PvChange};
pub use reconciler::{Reconciler, Action};
pub use command_publisher::{CommandBatch, IngestCommand, COMMAND_CHANNEL};