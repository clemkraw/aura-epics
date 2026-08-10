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

pub mod command_publisher;
pub mod config_poller;
pub mod orchestrator;
pub mod pg_notify;
pub mod reconciler;

pub use command_publisher::{COMMAND_CHANNEL, CommandBatch, IngestCommand};
pub use config_poller::{ConfigPoller, PvChange};
pub use orchestrator::Orchestrator;
pub use reconciler::{Action, Reconciler};
