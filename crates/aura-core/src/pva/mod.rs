//! Complete PVAccess type system for EPICS 7.
//!
//! Implements all Normative Types from the March 2015 specification (EPICS v4):
//! NTScalar, NTScalarArray, NTEnum, NTMatrix, NTHistogram, NTContinuum,
//! NTNameValue, NTTable, NTNDArray, NTMultiChannel, NTAggregate, NTUnion.
//!
//! Plus Custom for non-standard structures.
//! NTURI is omitted (RPC request type, not archivable data).

pub mod alarm;
pub mod arrays;
pub mod classify;
pub mod display;
pub mod enums;
pub mod ndarray;
pub mod normative;
pub mod scalars;
pub mod table;
pub mod time;
pub mod union;

pub use alarm::{Alarm, AlarmSeverity, AlarmStatus};
pub use arrays::ArrayValue;
pub use classify::PvDataType;
pub use display::{Control, Display, DisplayForm, ValueAlarm};
pub use enums::EnumValue;
pub use ndarray::{Codec, Dimension, NdAttribute};
pub use normative::{
    CustomStructure, NTAggregate, NTContinuum, NTEnum, NTHistogram,
    NTMatrix, NTMultiChannel, NTNDArray, NTNameValue, NTScalar,
    NTScalarArray, NTTable, NTUnion, NormativeType,
};
pub use scalars::{ScalarType, ScalarValue};
pub use table::{HistogramValue, TableColumn};
pub use time::TimeStamp;
pub use union::UnionValue;