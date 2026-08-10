//! aura-net — Pure Rust PVAccess client library.
//!
//! Implements the PVAccess (PVA) wire protocol from the EPICS 7
//! specification. Zero dependency on EPICS Base or PVXS C++ libraries.
//!
//! ## Layers
//!
//! ```text
//! runtime/  — PvaDriver: async TCP I/O (search → connect → subscribe)
//! client/   — PvaClientConfig (EPICS_PVA_* env vars)
//! monitor/  — Subscriptions, delta decoding, MonitorHandle
//! types/    — PvaValue → NormativeType conversion
//! messages/ — Typed protocol request/response structs
//! codec/    — Wire encoding, framing, BitSet, FieldDesc
//! ```
//!
//! ## Entry point
//!
//! ```ignore
//! use aura_net::PvaDriver;
//! use aura_net::client::PvaClientConfig;
//!
//! let mut driver = PvaDriver::new(PvaClientConfig::from_env());
//! let results = driver.monitor_batch(&["PERLE:Gun:Vacuum".into()]).await;
//! ```

pub mod client;
pub mod codec;
pub mod messages;
pub mod monitor;
pub mod runtime;
pub mod types;

pub use client::PvaClientConfig;
pub use runtime::PvaDriver;

pub use monitor::bus::{TaggedEvent, create_bus, shard_for_pv};
pub use monitor::{MonitorEvent, MonitorHandle};
pub use types::PvaValue;
