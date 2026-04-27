//! # aura-core
//!
//! Shared types for the AURA-EPICS archiving system.
//! Full EPICS 7 PVAccess Normative Types support.
//! ```

pub mod error;
pub mod pva;

pub use error::{AuraError, AuraResult};

pub use pva::{
    Alarm, AlarmSeverity, AlarmStatus, ArrayValue, Codec, Control,
    CustomStructure, Dimension, Display, DisplayForm, EnumValue,
    HistogramValue, NTAggregate, NTContinuum, NTEnum, NTHistogram,
    NTMatrix, NTMultiChannel, NTNDArray, NTNameValue, NTScalar,
    NTScalarArray, NTTable, NTUnion, NdAttribute, NormativeType,
    PvDataType, ScalarType, ScalarValue, TableColumn, TimeStamp,
    UnionValue, ValueAlarm,
};