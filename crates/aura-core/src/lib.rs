//! # aura-core
//!
//! Shared types, configuration, error handling, and telemetry for
//! AURA-EPICS. Full PVAccess Normative Types support.
//!
//! ## Modules
//!
//! - [`pva_types`] — Complete PVAccess type system (all Normative Types)
//! - [`sample`] — PV update flowing through the pipeline
//! - [`metadata`] — PV metadata auto-captured from PVA
//! - [`pv`] — PV configuration and IOC status types
//! - [`config`] — Application configuration (from `aura.toml`)
//! - [`error`] — Centralized error types
//! - [`telemetry`] — Structured logging initialization

pub mod config;
pub mod error;
pub mod metadata;
pub mod pv;
pub mod pva;
pub mod sample;
pub mod telemetry;

// Re-export the most-used types.
pub use config::AuraConfig;
pub use error::{AuraError, AuraResult};
pub use metadata::PvMetadata;
pub use pv::{IocInfo, IocState, PvConfig, PvStatus};
pub use pva::{
    Alarm, AlarmSeverity, AlarmStatus, ArrayValue, Codec, Control,
    CustomStructure, Dimension, Display, DisplayForm,
    EnumValue, HistogramValue,
    NTAggregate, NTContinuum, NTEnum, NTHistogram, NTMatrix,
    NTMultiChannel, NTNDArray, NTNameValue, NTScalar, NTScalarArray,
    NTTable, NTUnion, NdAttribute,
    NormativeType, PvDataType, ScalarType, ScalarValue,
    TableColumn, TimeStamp, UnionValue, ValueAlarm,
};
pub use sample::{FilterDecision, PvUpdate, StoreReason};