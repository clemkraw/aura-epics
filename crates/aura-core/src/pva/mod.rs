//! Complete PVAccess type system for EPICS 7.
//!
//! Implements all Normative Types from the March 2015 specification (EPICS v4):
//! NTScalar, NTScalarArray, NTEnum, NTMatrix, NTHistogram, NTContinuum,
//! NTNameValue, NTTable, NTNDArray, NTMultiChannel, NTAggregate, NTUnion.
//!
//! Plus Custom for non-standard structures.
//! NTURI is omitted (RPC request type, not archivable data).

pub mod alarm;
pub mod scalars;

pub use alarm::{Alarm, AlarmSeverity, AlarmStatus};
pub use scalars::{ScalarType, ScalarValue};