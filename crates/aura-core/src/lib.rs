//! # aura-core
//!
//! Shared types, configuration, error handling, and telemetry for AURA-EPICS. Full PVAccess Normative Types support.

pub mod alert;
pub mod config;
pub mod error;
pub mod metadata;
pub mod pv;
pub mod pva;
pub mod sample;
pub mod telemetry;

pub use alert::AlertLevel;
pub use config::AuraConfig;
pub use error::{AuraError, AuraResult};
pub use metadata::PvMetadata;
pub use pv::{IocInfo, IocState, PvConfig, PvStatus};
pub use pva::{
    Alarm, AlarmSeverity, AlarmStatus, ArrayValue, Codec, Control, CustomStructure, Dimension,
    Display, DisplayForm, EnumValue, HistogramValue, NTAggregate, NTContinuum, NTEnum, NTHistogram,
    NTMatrix, NTMultiChannel, NTNDArray, NTNameValue, NTScalar, NTScalarArray, NTTable, NTUnion,
    NdAttribute, NormativeType, PvDataType, ScalarType, ScalarValue, TableColumn, TimeStamp,
    UnionValue, ValueAlarm,
};
pub use sample::{PvUpdate, StoreReason};
