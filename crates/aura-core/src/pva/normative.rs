//! All EPICS 7 Normative Types.
//!
//! Complete list per the March 2015 specification + EPICS 7 extensions:
//! NTScalar, NTScalarArray, NTEnum, NTMatrix, NTHistogram, NTContinuum,
//! NTNameValue, NTTable, NTNDArray, NTMultiChannel, NTAggregate, NTUnion.
//! Plus Custom for non-standard structures.
//!
//! NTURI is omitted (RPC request type, not archivable).

use super::alarm::Alarm;
use super::arrays::ArrayValue;
use super::display::{Control, Display, ValueAlarm};
use super::enums::EnumValue;
use super::ndarray::{Codec, Dimension, NdAttribute};
use super::scalars::ScalarValue;
use super::table::{HistogramValue, TableColumn};
use super::time::TimeStamp;
use super::union::UnionValue;
use serde::{Deserialize, Serialize};

/// `epics:nt/NTScalar:1.0`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTScalar {
    pub value: ScalarValue,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
    #[serde(default)]
    pub display: Option<Display>,
    #[serde(default)]
    pub control: Option<Control>,
    #[serde(default)]
    pub value_alarm: Option<ValueAlarm>,
}

/// `epics:nt/NTEnum:1.0`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTEnum {
    pub value: EnumValue,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

/// `epics:nt/NTScalarArray:1.0`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTScalarArray {
    pub value: ArrayValue,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
    #[serde(default)]
    pub display: Option<Display>,
    #[serde(default)]
    pub control: Option<Control>,
    #[serde(default)]
    pub value_alarm: Option<ValueAlarm>,
}

/// `epics:nt/NTMatrix:1.0` — 2D real matrix, row-major.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTMatrix {
    pub value: Vec<f64>,
    #[serde(default)]
    pub dim: Vec<i32>,
    #[serde(default)]
    pub descriptor: String,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
    #[serde(default)]
    pub display: Option<Display>,
}

impl NTMatrix {
    pub fn rows(&self) -> usize {
        if self.dim.len() >= 2 {
            self.dim[0] as usize
        } else {
            1
        }
    }
    pub fn cols(&self) -> usize {
        if self.dim.len() >= 2 {
            self.dim[1] as usize
        } else if self.dim.len() == 1 {
            self.dim[0] as usize
        } else {
            self.value.len()
        }
    }
    pub fn get(&self, row: usize, col: usize) -> Option<f64> {
        self.value.get(row * self.cols() + col).copied()
    }
}

/// `epics:nt/NTHistogram:1.0` — 1D histogram.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTHistogram {
    pub ranges: Vec<f64>,
    pub value: HistogramValue,
    #[serde(default)]
    pub descriptor: String,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

impl NTHistogram {
    pub fn bin_count(&self) -> usize {
        self.value.len()
    }
}

/// `epics:nt/NTContinuum:1.0` — multi-trace time/frequency domain data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTContinuum {
    pub base: Vec<f64>,
    pub value: Vec<f64>,
    pub units: Vec<String>,
    #[serde(default)]
    pub descriptor: String,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

impl NTContinuum {
    pub fn point_count(&self) -> usize {
        self.base.len()
    }
    pub fn trace_count(&self) -> usize {
        if self.base.is_empty() {
            0
        } else {
            self.value.len() / self.base.len()
        }
    }
    pub fn get(&self, point: usize, trace: usize) -> Option<f64> {
        let n = self.trace_count();
        if n == 0 {
            return None;
        }
        self.value.get(point * n + trace).copied()
    }
}

/// `epics:nt/NTNameValue:1.0` — named scalar parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTNameValue {
    pub name: Vec<String>,
    pub value: ArrayValue,
    #[serde(default)]
    pub descriptor: String,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

impl NTNameValue {
    pub fn len(&self) -> usize {
        self.name.len()
    }
    pub fn is_empty(&self) -> bool {
        self.name.is_empty()
    }
}

/// `epics:nt/NTTable:1.0` — columnar table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTTable {
    pub labels: Vec<String>,
    pub columns: Vec<TableColumn>,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

impl NTTable {
    pub fn row_count(&self) -> usize {
        self.columns.first().map(|c| c.values.len()).unwrap_or(0)
    }
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// `epics:nt/NTNDArray:1.0` — N-dimensional image/detector data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTNDArray {
    pub value: ArrayValue,
    #[serde(default)]
    pub codec: Codec,
    #[serde(default)]
    pub compressed_size: i64,
    #[serde(default)]
    pub uncompressed_size: i64,
    pub dimension: Vec<Dimension>,
    #[serde(default)]
    pub unique_id: i32,
    #[serde(default)]
    pub data_timestamp: Option<TimeStamp>,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
    #[serde(default)]
    pub attribute: Vec<NdAttribute>,
}

/// `epics:nt/NTMultiChannel:1.0` — snapshot of multiple PVs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTMultiChannel {
    pub values: Vec<ScalarValue>,
    pub channel_name: Vec<String>,
    #[serde(default)]
    pub is_connected: Vec<bool>,
    #[serde(default)]
    pub severity: Vec<super::alarm::AlarmSeverity>,
    #[serde(default)]
    pub status: Vec<super::alarm::AlarmStatus>,
    #[serde(default)]
    pub message: Vec<String>,
    #[serde(default)]
    pub seconds_past_epoch: Vec<i64>,
    #[serde(default)]
    pub nanoseconds: Vec<i32>,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

/// `epics:nt/NTAggregate:1.0` — pre-computed statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTAggregate {
    pub value: f64,
    #[serde(default)]
    pub n: i64,
    #[serde(default)]
    pub dispersion: f64,
    #[serde(default)]
    pub first: f64,
    #[serde(default)]
    pub last: f64,
    #[serde(default)]
    pub max: f64,
    #[serde(default)]
    pub min: f64,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

/// `epics:nt/NTUnion:1.0` — runtime polymorphic value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NTUnion {
    pub value: UnionValue,
    #[serde(default)]
    pub descriptor: String,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

/// Non-standard structure — stored as raw JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomStructure {
    pub data: serde_json::Value,
    #[serde(default)]
    pub alarm: Alarm,
    #[serde(default)]
    pub timestamp: TimeStamp,
}

/// Every PV monitor update deserializes into one of these variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "nt_type")]
pub enum NormativeType {
    NTScalar(NTScalar),
    NTEnum(NTEnum),
    NTScalarArray(NTScalarArray),
    NTMatrix(NTMatrix),
    NTHistogram(NTHistogram),
    NTContinuum(NTContinuum),
    NTNameValue(NTNameValue),
    NTTable(NTTable),
    NTNDArray(NTNDArray),
    NTMultiChannel(NTMultiChannel),
    NTAggregate(NTAggregate),
    NTUnion(NTUnion),
    Custom(CustomStructure),
}

impl NormativeType {
    pub fn alarm(&self) -> &Alarm {
        match self {
            Self::NTScalar(nt) => &nt.alarm,
            Self::NTEnum(nt) => &nt.alarm,
            Self::NTScalarArray(nt) => &nt.alarm,
            Self::NTMatrix(nt) => &nt.alarm,
            Self::NTHistogram(nt) => &nt.alarm,
            Self::NTContinuum(nt) => &nt.alarm,
            Self::NTNameValue(nt) => &nt.alarm,
            Self::NTTable(nt) => &nt.alarm,
            Self::NTNDArray(nt) => &nt.alarm,
            Self::NTMultiChannel(nt) => &nt.alarm,
            Self::NTAggregate(nt) => &nt.alarm,
            Self::NTUnion(nt) => &nt.alarm,
            Self::Custom(nt) => &nt.alarm,
        }
    }

    pub fn timestamp(&self) -> &TimeStamp {
        match self {
            Self::NTScalar(nt) => &nt.timestamp,
            Self::NTEnum(nt) => &nt.timestamp,
            Self::NTScalarArray(nt) => &nt.timestamp,
            Self::NTMatrix(nt) => &nt.timestamp,
            Self::NTHistogram(nt) => &nt.timestamp,
            Self::NTContinuum(nt) => &nt.timestamp,
            Self::NTNameValue(nt) => &nt.timestamp,
            Self::NTTable(nt) => &nt.timestamp,
            Self::NTNDArray(nt) => &nt.timestamp,
            Self::NTMultiChannel(nt) => &nt.timestamp,
            Self::NTAggregate(nt) => &nt.timestamp,
            Self::NTUnion(nt) => &nt.timestamp,
            Self::Custom(nt) => &nt.timestamp,
        }
    }

    /// Extract a single f64 (for epsilon filter). Works for scalar, enum, aggregate, union.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::NTScalar(nt) => nt.value.as_f64(),
            Self::NTEnum(nt) => Some(nt.value.as_f64()),
            Self::NTAggregate(nt) => Some(nt.value),
            Self::NTUnion(nt) => nt.value.as_f64(),
            _ => None,
        }
    }

    /// Short type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::NTScalar(_) => "NTScalar",
            Self::NTEnum(_) => "NTEnum",
            Self::NTScalarArray(_) => "NTScalarArray",
            Self::NTMatrix(_) => "NTMatrix",
            Self::NTHistogram(_) => "NTHistogram",
            Self::NTContinuum(_) => "NTContinuum",
            Self::NTNameValue(_) => "NTNameValue",
            Self::NTTable(_) => "NTTable",
            Self::NTNDArray(_) => "NTNDArray",
            Self::NTMultiChannel(_) => "NTMultiChannel",
            Self::NTAggregate(_) => "NTAggregate",
            Self::NTUnion(_) => "NTUnion",
            Self::Custom(_) => "Custom",
        }
    }

    /// Formal type identifier string.
    pub fn type_id(&self) -> &'static str {
        match self {
            Self::NTScalar(_) => "epics:nt/NTScalar:1.0",
            Self::NTEnum(_) => "epics:nt/NTEnum:1.0",
            Self::NTScalarArray(_) => "epics:nt/NTScalarArray:1.0",
            Self::NTMatrix(_) => "epics:nt/NTMatrix:1.0",
            Self::NTHistogram(_) => "epics:nt/NTHistogram:1.0",
            Self::NTContinuum(_) => "epics:nt/NTContinuum:1.0",
            Self::NTNameValue(_) => "epics:nt/NTNameValue:1.0",
            Self::NTTable(_) => "epics:nt/NTTable:1.0",
            Self::NTNDArray(_) => "epics:nt/NTNDArray:1.0",
            Self::NTMultiChannel(_) => "epics:nt/NTMultiChannel:1.0",
            Self::NTAggregate(_) => "epics:nt/NTAggregate:1.0",
            Self::NTUnion(_) => "epics:nt/NTUnion:1.0",
            Self::Custom(_) => "custom",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> TimeStamp {
        TimeStamp::new(0, 0)
    }

    #[test]
    fn test_scalar() {
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(4.217),
            alarm: Alarm::default(),
            timestamp: ts(),
            display: None,
            control: None,
            value_alarm: None,
        });
        assert_eq!(nt.as_f64(), Some(4.217));
        assert_eq!(nt.type_id(), "epics:nt/NTScalar:1.0");
    }

    #[test]
    fn test_enum() {
        let nt = NormativeType::NTEnum(NTEnum {
            value: EnumValue {
                index: 2,
                choices: vec!["A".into(), "B".into(), "C".into()],
            },
            alarm: Alarm::default(),
            timestamp: ts(),
        });
        assert_eq!(nt.as_f64(), Some(2.0));
    }

    #[test]
    fn test_matrix() {
        let m = NTMatrix {
            value: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            dim: vec![2, 3],
            descriptor: String::new(),
            alarm: Alarm::default(),
            timestamp: ts(),
            display: None,
        };
        assert_eq!(m.rows(), 2);
        assert_eq!(m.cols(), 3);
        assert_eq!(m.get(1, 2), Some(6.0));
    }

    #[test]
    fn test_histogram() {
        let h = NTHistogram {
            ranges: vec![0.0, 1.0, 2.0, 3.0],
            value: HistogramValue::Int(vec![5, 10, 3]),
            descriptor: String::new(),
            alarm: Alarm::default(),
            timestamp: ts(),
        };
        assert_eq!(h.bin_count(), 3);
    }

    #[test]
    fn test_continuum() {
        let c = NTContinuum {
            base: vec![0.0, 1.0, 2.0],
            value: vec![1.0, 0.5, 2.0, 1.0, 3.0, 1.5],
            units: vec!["s".into(), "V".into(), "A".into()],
            descriptor: String::new(),
            alarm: Alarm::default(),
            timestamp: ts(),
        };
        assert_eq!(c.point_count(), 3);
        assert_eq!(c.trace_count(), 2);
        assert_eq!(c.get(2, 1), Some(1.5));
    }

    #[test]
    fn test_name_value() {
        let nv = NTNameValue {
            name: vec!["gain".into(), "offset".into()],
            value: ArrayValue::DoubleArray(vec![1.5, -0.3]),
            descriptor: String::new(),
            alarm: Alarm::default(),
            timestamp: ts(),
        };
        assert_eq!(nv.len(), 2);
    }

    #[test]
    fn test_table() {
        let t = NTTable {
            labels: vec!["x".into(), "y".into()],
            columns: vec![
                TableColumn {
                    name: "x".into(),
                    values: ArrayValue::DoubleArray(vec![1.0, 2.0]),
                },
                TableColumn {
                    name: "y".into(),
                    values: ArrayValue::DoubleArray(vec![3.0, 4.0]),
                },
            ],
            alarm: Alarm::default(),
            timestamp: ts(),
        };
        assert_eq!(t.row_count(), 2);
        assert_eq!(t.column_count(), 2);
    }

    #[test]
    fn test_union() {
        let nt = NormativeType::NTUnion(NTUnion {
            value: UnionValue::Scalar(ScalarValue::Double(42.0)),
            descriptor: String::new(),
            alarm: Alarm::default(),
            timestamp: ts(),
        });
        assert_eq!(nt.as_f64(), Some(42.0));
        assert_eq!(nt.type_id(), "epics:nt/NTUnion:1.0");
    }

    #[test]
    fn test_aggregate() {
        let nt = NormativeType::NTAggregate(NTAggregate {
            value: 4.2,
            n: 100,
            dispersion: 0.01,
            first: 4.1,
            last: 4.3,
            max: 4.5,
            min: 3.9,
            alarm: Alarm::default(),
            timestamp: ts(),
        });
        assert_eq!(nt.as_f64(), Some(4.2));
    }

    #[test]
    fn test_custom() {
        let nt = NormativeType::Custom(CustomStructure {
            data: serde_json::json!({"x": 1}),
            alarm: Alarm::default(),
            timestamp: ts(),
        });
        assert_eq!(nt.as_f64(), None);
        assert_eq!(nt.type_id(), "custom");
    }

    #[test]
    fn test_all_type_ids_valid() {
        // Every type_id must start with "epics:nt/" or be "custom"
        let types: Vec<NormativeType> = vec![
            NormativeType::NTScalar(NTScalar {
                value: ScalarValue::Int(0),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
            NormativeType::NTEnum(NTEnum {
                value: EnumValue {
                    index: 0,
                    choices: vec![],
                },
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
            NormativeType::NTScalarArray(NTScalarArray {
                value: ArrayValue::DoubleArray(vec![]),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
            NormativeType::NTMatrix(NTMatrix {
                value: vec![],
                dim: vec![],
                descriptor: String::new(),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
            }),
            NormativeType::NTHistogram(NTHistogram {
                ranges: vec![],
                value: HistogramValue::Int(vec![]),
                descriptor: String::new(),
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
            NormativeType::NTContinuum(NTContinuum {
                base: vec![],
                value: vec![],
                units: vec![],
                descriptor: String::new(),
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
            NormativeType::NTNameValue(NTNameValue {
                name: vec![],
                value: ArrayValue::DoubleArray(vec![]),
                descriptor: String::new(),
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
            NormativeType::NTTable(NTTable {
                labels: vec![],
                columns: vec![],
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
            NormativeType::NTAggregate(NTAggregate {
                value: 0.0,
                n: 0,
                dispersion: 0.0,
                first: 0.0,
                last: 0.0,
                max: 0.0,
                min: 0.0,
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
            NormativeType::NTUnion(NTUnion {
                value: UnionValue::Scalar(ScalarValue::Int(0)),
                descriptor: String::new(),
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
            NormativeType::Custom(CustomStructure {
                data: serde_json::Value::Null,
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
        ];
        for nt in &types {
            let id = nt.type_id();
            assert!(
                id.starts_with("epics:nt/") || id == "custom",
                "bad type_id '{}' for {}",
                id,
                nt.type_name()
            );
        }
    }
}
