//! Bridges the raw PVA wire world to AURA's type-safe pipeline.
//! Each converter extracts the standard fields (value, alarm, timestamp, display, control)
//! into the corresponding aura-core struct.
//!
//! ## Supported NT types
//!
//! | PVA type_id                   | aura-core type  | Usage                  |
//! |-------------------------------|------------------|-----------------------|
//! | `epics:nt/NTScalar:1.0`       | NTScalar         | Sensors, setpoints    |
//! | `epics:nt/NTEnum:1.0`         | NTEnum           | States, modes         |
//! | `epics:nt/NTScalarArray:1.0`  | NTScalarArray    | Waveforms, spectra    |
//! | `epics:nt/NTTable:1.0`        | NTTable          | BPM data, diagnostics |
//! | `epics:nt/NTNDArray:1.0`      | NTNDArray        | Camera images         |
//! | Unknown                       | Custom           | Preserved as JSON     |

use super::pva_value::{PvaScalar, PvaValue};
use crate::codec::field_desc::FieldDesc;
use aura_core::pva::alarm::{Alarm, AlarmSeverity, AlarmStatus};
use aura_core::pva::arrays::ArrayValue;
use aura_core::pva::enums::EnumValue;
use aura_core::pva::ndarray::{Codec, Dimension, NdAttribute};
use aura_core::pva::normative::{
    CustomStructure, NTEnum, NTNDArray, NTScalar, NTScalarArray, NTTable, NormativeType,
};
use aura_core::pva::table::TableColumn;
use aura_core::pva::time::TimeStamp;

/// Convert a decoded PVA value + its type description into a NormativeType.
///
/// Returns `None` only if the value is `Null`. All recognized types are
/// converted; unknown types produce `Custom` with JSON representation.
pub fn to_normative(desc: &FieldDesc, value: &PvaValue) -> Option<NormativeType> {
    if value.is_null() {
        return None;
    }

    match desc.type_id.as_str() {
        "epics:nt/NTScalar:1.0" => convert_nt_scalar(value),
        "epics:nt/NTEnum:1.0" => convert_nt_enum(value),
        "epics:nt/NTScalarArray:1.0" => convert_nt_scalar_array(value),
        "epics:nt/NTTable:1.0" => convert_nt_table(value),
        "epics:nt/NTNDArray:1.0" => convert_nt_ndarray(value),
        other => {
            tracing::debug!(type_id = other, "unknown NT type — wrapping as Custom");
            convert_custom(value)
        }
    }
}

/// Convert a PvaValue to NormativeType without a FieldDesc.
///
/// Detects the type from the structure fields:
/// - Has "value" scalar field → NTScalar
/// - Has "value" array field → NTScalarArray  
/// - Has "value" with "index"+"choices" → NTEnum
///
/// Used when the monitor's FieldDesc is not available (e.g. direct TCP connection without prior introspection).
pub fn to_normative_from_value(value: &PvaValue) -> Option<NormativeType> {
    if value.is_null() {
        return None;
    }

    let val_field = value.field("value")?;

    // NTScalar: value is a scalar
    if val_field.as_scalar().is_some() {
        return convert_nt_scalar(value);
    }

    // NTEnum: value has index + choices
    if val_field.field("index").is_some() {
        return convert_nt_enum(value);
    }

    // NTScalarArray: value is an array
    if val_field.is_array() {
        return convert_nt_scalar_array(value);
    }

    None
}

fn convert_nt_scalar(value: &PvaValue) -> Option<NormativeType> {
    if let PvaValue::Structure(fields) = value {
        if fields.len() >= 3 {
            // Index 0: value (scalar)
            let scalar_val = match &fields[0].1 {
                PvaValue::Scalar(s) => s.clone(),
                _ => return convert_nt_scalar_fallback(value),
            };

            // Index 1: alarm structure
            let alarm = if let PvaValue::Structure(alarm_fields) = &fields[1].1 {
                Alarm {
                    severity: AlarmSeverity::from(
                        alarm_fields
                            .get(0)
                            .and_then(|(_, v)| v.as_i32())
                            .unwrap_or(0) as i16,
                    ),
                    status: AlarmStatus::from(
                        alarm_fields
                            .get(1)
                            .and_then(|(_, v)| v.as_i32())
                            .unwrap_or(0) as i16,
                    ),
                    message: alarm_fields
                        .get(2)
                        .and_then(|(_, v)| v.as_string())
                        .unwrap_or("")
                        .to_string(),
                }
            } else {
                Alarm::default()
            };

            // Index 2: timeStamp structure
            let timestamp = if let PvaValue::Structure(ts_fields) = &fields[2].1 {
                TimeStamp {
                    seconds: ts_fields.get(0).and_then(|(_, v)| v.as_i64()).unwrap_or(0),
                    nanoseconds: ts_fields.get(1).and_then(|(_, v)| v.as_i32()).unwrap_or(0) as u32,
                    user_tag: ts_fields.get(2).and_then(|(_, v)| v.as_i32()).unwrap_or(0),
                }
            } else {
                TimeStamp::default()
            };

            return Some(NormativeType::NTScalar(NTScalar {
                value: scalar_val,
                alarm,
                timestamp,
                display: None,
                control: None,
                value_alarm: None,
            }));
        }
    }
    convert_nt_scalar_fallback(value)
}

/// Fallback: use string-based field lookup (for non-standard layouts).
fn convert_nt_scalar_fallback(value: &PvaValue) -> Option<NormativeType> {
    let scalar_val = value.field("value")?.as_scalar()?.clone();
    Some(NormativeType::NTScalar(NTScalar {
        value: scalar_val,
        alarm: extract_alarm(value),
        timestamp: extract_timestamp(value),
        display: None,
        control: None,
        value_alarm: None,
    }))
}

fn convert_nt_enum(value: &PvaValue) -> Option<NormativeType> {
    let enum_struct = value.field("value")?;
    let index = enum_struct.field_i32("index").unwrap_or(0);
    let choices = extract_string_array(enum_struct.field("choices"));

    Some(NormativeType::NTEnum(NTEnum {
        value: EnumValue { index, choices },
        alarm: extract_alarm(value),
        timestamp: extract_timestamp(value),
    }))
}

fn convert_nt_scalar_array(value: &PvaValue) -> Option<NormativeType> {
    let array_val = extract_array_value(value.field("value")?)?;
    Some(NormativeType::NTScalarArray(NTScalarArray {
        value: array_val,
        alarm: extract_alarm(value),
        timestamp: extract_timestamp(value),
        display: None,
        control: None,
        value_alarm: None,
    }))
}

fn convert_nt_table(value: &PvaValue) -> Option<NormativeType> {
    let labels = extract_string_array(value.field("labels"));

    // PVA tables store columns in a "value" structure where each
    // sub-field is a named array column.
    let mut columns = Vec::new();
    if let Some(val_struct) = value.field("value") {
        if let PvaValue::Structure(fields) = val_struct {
            for (name, col_val) in fields {
                if let Some(arr) = extract_array_value(col_val) {
                    columns.push(TableColumn::new(name.to_string(), arr));
                }
            }
        }
    }

    Some(NormativeType::NTTable(NTTable {
        labels,
        columns,
        alarm: extract_alarm(value),
        timestamp: extract_timestamp(value),
    }))
}

fn convert_nt_ndarray(value: &PvaValue) -> Option<NormativeType> {
    let image_data = extract_array_value(value.field("value")?)?;

    // Dimensions: array of structures {size, offset, fullSize, binning, reverse}.
    let dimension = extract_dimensions(value.field("dimension"));

    // Codec info.
    let codec = match value.field("codec") {
        Some(c) => Codec {
            name: c.field_string("name").unwrap_or("").to_string(),
            parameters: serde_json::Value::Null,
        },
        None => Codec::default(),
    };

    let compressed_size = value.field_i64("compressedSize").unwrap_or(0);
    let uncompressed_size = value.field_i64("uncompressedSize").unwrap_or(0);
    let unique_id = value.field_i32("uniqueId").unwrap_or(0);

    // Data timestamp (separate from the main timestamp).
    let data_timestamp = value
        .field("dataTimeStamp")
        .map(|ts| extract_timestamp_from(ts));

    // Attributes: array of {name, value, source, sourceType}.
    let attribute = extract_nd_attributes(value.field("attribute"));

    Some(NormativeType::NTNDArray(NTNDArray {
        value: image_data,
        codec,
        compressed_size,
        uncompressed_size,
        dimension,
        unique_id,
        data_timestamp: Some(data_timestamp.unwrap_or_default()),
        alarm: extract_alarm(value),
        timestamp: extract_timestamp(value),
        attribute,
    }))
}

fn convert_custom(value: &PvaValue) -> Option<NormativeType> {
    let json = pva_value_to_json(value);
    Some(NormativeType::Custom(CustomStructure {
        data: json,
        alarm: extract_alarm(value),
        timestamp: extract_timestamp(value),
    }))
}

/// Extract alarm fields from a PVA structure.
pub fn extract_alarm(value: &PvaValue) -> Alarm {
    match value.field("alarm") {
        Some(a) => Alarm {
            severity: AlarmSeverity::from(a.field_i32("severity").unwrap_or(0) as i16),
            status: AlarmStatus::from(a.field_i32("status").unwrap_or(0) as i16),
            message: a.field_string("message").unwrap_or("").to_string(),
        },
        None => Alarm::default(),
    }
}

/// Extract timestamp fields from a PVA structure.
pub fn extract_timestamp(value: &PvaValue) -> TimeStamp {
    value
        .field("timeStamp")
        .map(|ts| extract_timestamp_from(ts))
        .unwrap_or_default()
}

/// Extract timestamp from a timestamp sub-structure directly.
fn extract_timestamp_from(ts: &PvaValue) -> TimeStamp {
    TimeStamp {
        seconds: ts
            .field("secondsPastEpoch")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        nanoseconds: ts.field_i32("nanoseconds").unwrap_or(0) as u32,
        user_tag: ts.field_i32("userTag").unwrap_or(0),
    }
}

/// Extract a string array from a PvaValue::ScalarArray of Strings.
fn extract_string_array(val: Option<&PvaValue>) -> Vec<String> {
    match val {
        Some(PvaValue::ScalarArray(arr)) => arr
            .iter()
            .filter_map(|s| {
                if let PvaScalar::String(s) = s {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect(),
        _ => vec![],
    }
}

/// Convert a PvaValue::ScalarArray into an aura-core ArrayValue.
fn extract_array_value(val: &PvaValue) -> Option<ArrayValue> {
    let arr = val.as_scalar_array()?;
    if arr.is_empty() {
        return Some(ArrayValue::DoubleArray(vec![]));
    }

    // Determine type from first element, collect in one pass.
    Some(match &arr[0] {
        PvaScalar::Boolean(_) => ArrayValue::BooleanArray(
            arr.iter()
                .map(|s| matches!(s, PvaScalar::Boolean(true)))
                .collect(),
        ),
        PvaScalar::Byte(_) => ArrayValue::ByteArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::Byte(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::UByte(_) => ArrayValue::UByteArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::UByte(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::Short(_) => ArrayValue::ShortArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::Short(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::UShort(_) => ArrayValue::UShortArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::UShort(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::Int(_) => ArrayValue::IntArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::Int(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::UInt(_) => ArrayValue::UIntArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::UInt(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::Long(_) => ArrayValue::LongArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::Long(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::ULong(_) => ArrayValue::ULongArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::ULong(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::Float(_) => ArrayValue::FloatArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::Float(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::Double(_) => ArrayValue::DoubleArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::Double(v) = s {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        PvaScalar::String(_) => ArrayValue::StringArray(
            arr.iter()
                .filter_map(|s| {
                    if let PvaScalar::String(v) = s {
                        Some(v.clone())
                    } else {
                        None
                    }
                })
                .collect(),
        ),
    })
}

/// Extract NTNDArray dimensions from a PvaValue array of structures.
fn extract_dimensions(val: Option<&PvaValue>) -> Vec<Dimension> {
    match val {
        Some(PvaValue::ScalarArray(_)) => vec![], // wrong type
        Some(PvaValue::Structure(fields)) => {
            fields
                .iter()
                .filter_map(|(_, dim)| {
                    Some(Dimension {
                        size: dim.field_i32("size")?,
                        offset: dim.field_i32("offset").unwrap_or(0),
                        full_size: dim.field_i32("fullSize").unwrap_or(0),
                        binning: dim.field_i32("binning").unwrap_or(1),
                        reverse: dim
                            .field("reverse")
                            .and_then(|v| v.as_scalar())
                            .map(|s| matches!(s, PvaScalar::Boolean(true)))
                            .unwrap_or(false),
                    })
                })
                .collect()
        }
        _ => vec![],
    }
}

/// Extract NTNDArray attributes.
fn extract_nd_attributes(val: Option<&PvaValue>) -> Vec<NdAttribute> {
    match val {
        Some(PvaValue::Structure(attrs)) => attrs
            .iter()
            .filter_map(|(_, attr)| {
                let name = attr.field_string("name")?.to_string();
                let value = attr.field("value")?.as_scalar()?.clone();
                Some(NdAttribute {
                    name,
                    value,
                    source: attr.field_string("source").unwrap_or("").to_string(),
                    source_type: Default::default(),
                    description: attr.field_string("description").unwrap_or("").to_string(),
                })
            })
            .collect(),
        _ => vec![],
    }
}

/// Convert a PvaValue tree to a serde_json::Value (for Custom type).
fn pva_value_to_json(val: &PvaValue) -> serde_json::Value {
    match val {
        PvaValue::Scalar(s) => match s {
            PvaScalar::Boolean(v) => serde_json::Value::Bool(*v),
            PvaScalar::String(v)  => serde_json::Value::String(v.clone()),
            _ => match s {
                PvaScalar::Float(_) | PvaScalar::Double(_) =>
                    s.as_f64().map(|f| serde_json::json!(f)).unwrap_or(serde_json::Value::Null),
                _ =>
                    s.as_i64().map(|i| serde_json::json!(i)).unwrap_or(serde_json::Value::Null),
            },
        },
        PvaValue::ScalarArray(arr) => serde_json::Value::Array(
            arr.iter()
                .map(|s| match s {
                    PvaScalar::String(v) => serde_json::Value::String(v.clone()),
                    PvaScalar::Boolean(v) => serde_json::Value::Bool(*v),
                    _ => s.as_i64()
                        .map(|i| serde_json::json!(i))
                        .or_else(|| s.as_f64().map(|f| serde_json::json!(f)))
                        .unwrap_or(serde_json::Value::Null),
                })
                .collect(),
        ),
        PvaValue::Structure(fields) => {
            let map: serde_json::Map<String, serde_json::Value> = fields
                .iter()
                .map(|(k, v)| (k.to_string(), pva_value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        PvaValue::Union(name, val) => {
            let mut map = serde_json::Map::new();
            map.insert(name.to_string(), pva_value_to_json(val));
            serde_json::Value::Object(map)
        }
        PvaValue::Null => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::field_desc::FieldDesc;
    use aura_core::pva::scalars::ScalarValue;

    fn desc(type_id: &str) -> FieldDesc {
        FieldDesc::structure(type_id, vec![])
    }

    fn sv(v: f64) -> PvaScalar {
        PvaScalar::Double(v)
    }

    fn si(v: i32) -> PvaScalar {
        PvaScalar::Int(v)
    }

    fn ss(v: &str) -> PvaScalar {
        PvaScalar::String(v.into())
    }

    fn sl(v: i64) -> PvaScalar {
        PvaScalar::Long(v)
    }

    fn alarm_fields(severity: i32) -> (std::sync::Arc<str>, PvaValue) {
        (
            "alarm".into(),
            PvaValue::Structure(vec![
                ("severity".into(), PvaValue::Scalar(si(severity))),
                ("status".into(), PvaValue::Scalar(si(0))),
                ("message".into(), PvaValue::Scalar(ss(""))),
            ]),
        )
    }

    fn ts_fields(secs: i64, ns: i32) -> (std::sync::Arc<str>, PvaValue) {
        (
            "timeStamp".into(),
            PvaValue::Structure(vec![
                ("secondsPastEpoch".into(), PvaValue::Scalar(sl(secs))),
                ("nanoseconds".into(), PvaValue::Scalar(si(ns))),
                ("userTag".into(), PvaValue::Scalar(si(0))),
            ]),
        )
    }

    fn make_nt_scalar(val: PvaScalar, sev: i32) -> PvaValue {
        PvaValue::Structure(vec![
            ("value".into(), PvaValue::Scalar(val)),
            alarm_fields(sev),
            ts_fields(1700000000, 123),
        ])
    }

    #[test]
    fn test_scalar_double() {
        let nt = to_normative(
            &desc("epics:nt/NTScalar:1.0"),
            &make_nt_scalar(sv(4.217), 0),
        )
        .unwrap();
        match &nt {
            NormativeType::NTScalar(s) => assert_eq!(s.value.as_f64(), Some(4.217)),
            _ => panic!(),
        }
    }

    #[test]
    fn test_scalar_int() {
        let nt = to_normative(&desc("epics:nt/NTScalar:1.0"), &make_nt_scalar(si(42), 0)).unwrap();
        match &nt {
            NormativeType::NTScalar(s) => assert_eq!(s.value, ScalarValue::Int(42)),
            _ => panic!(),
        }
    }

    #[test]
    fn test_scalar_string() {
        let nt = to_normative(
            &desc("epics:nt/NTScalar:1.0"),
            &make_nt_scalar(ss("hello"), 0),
        )
        .unwrap();
        match &nt {
            NormativeType::NTScalar(s) => assert_eq!(s.value, ScalarValue::String("hello".into())),
            _ => panic!(),
        }
    }

    #[test]
    fn test_scalar_alarm() {
        let nt = to_normative(&desc("epics:nt/NTScalar:1.0"), &make_nt_scalar(sv(0.0), 2)).unwrap();
        assert_eq!(nt.alarm().severity, AlarmSeverity::Major);
    }

    #[test]
    fn test_scalar_timestamp() {
        let nt = to_normative(&desc("epics:nt/NTScalar:1.0"), &make_nt_scalar(sv(0.0), 0)).unwrap();
        assert_eq!(nt.timestamp().seconds, 1700000000);
        assert_eq!(nt.timestamp().nanoseconds, 123);
    }

    #[test]
    fn test_scalar_no_value() {
        assert!(
            to_normative(&desc("epics:nt/NTScalar:1.0"), &PvaValue::Structure(vec![])).is_none()
        );
    }

    #[test]
    fn test_scalar_null() {
        assert!(to_normative(&desc("epics:nt/NTScalar:1.0"), &PvaValue::Null).is_none());
    }

    #[test]
    fn test_enum() {
        let v = PvaValue::Structure(vec![
            (
                "value".into(),
                PvaValue::Structure(vec![
                    ("index".into(), PvaValue::Scalar(si(1))),
                    (
                        "choices".into(),
                        PvaValue::ScalarArray(vec![ss("OFF"), ss("ON")]),
                    ),
                ]),
            ),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        let nt = to_normative(&desc("epics:nt/NTEnum:1.0"), &v).unwrap();
        match &nt {
            NormativeType::NTEnum(e) => {
                assert_eq!(e.value.index, 1);
                assert_eq!(e.value.choices, vec!["OFF", "ON"]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_enum_no_value() {
        assert!(to_normative(&desc("epics:nt/NTEnum:1.0"), &PvaValue::Structure(vec![])).is_none());
    }

    #[test]
    fn test_enum_empty_choices() {
        let v = PvaValue::Structure(vec![
            (
                "value".into(),
                PvaValue::Structure(vec![("index".into(), PvaValue::Scalar(si(0)))]),
            ),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("epics:nt/NTEnum:1.0"), &v).unwrap() {
            NormativeType::NTEnum(e) => assert!(e.value.choices.is_empty()),
            _ => panic!(),
        }
    }

    #[test]
    fn test_array_double() {
        let v = PvaValue::Structure(vec![
            (
                "value".into(),
                PvaValue::ScalarArray(vec![sv(1.0), sv(2.0), sv(3.0)]),
            ),
            alarm_fields(0),
            ts_fields(1700000000, 0),
        ]);
        let nt = to_normative(&desc("epics:nt/NTScalarArray:1.0"), &v).unwrap();
        match &nt {
            NormativeType::NTScalarArray(a) => {
                assert_eq!(a.value.len(), 3);
                assert!(a.value.is_numeric());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_array_int() {
        let v = PvaValue::Structure(vec![
            ("value".into(), PvaValue::ScalarArray(vec![si(10), si(20)])),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("epics:nt/NTScalarArray:1.0"), &v).unwrap() {
            NormativeType::NTScalarArray(a) => assert_eq!(a.value.len(), 2),
            _ => panic!(),
        }
    }

    #[test]
    fn test_array_ubyte() {
        let v = PvaValue::Structure(vec![
            (
                "value".into(),
                PvaValue::ScalarArray(vec![PvaScalar::UByte(0xFF), PvaScalar::UByte(0x00)]),
            ),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("epics:nt/NTScalarArray:1.0"), &v).unwrap() {
            NormativeType::NTScalarArray(a) => assert_eq!(a.value.len(), 2),
            _ => panic!(),
        }
    }

    #[test]
    fn test_array_string() {
        let v = PvaValue::Structure(vec![
            (
                "value".into(),
                PvaValue::ScalarArray(vec![ss("a"), ss("b")]),
            ),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("epics:nt/NTScalarArray:1.0"), &v).unwrap() {
            NormativeType::NTScalarArray(a) => assert_eq!(a.value.len(), 2),
            _ => panic!(),
        }
    }

    #[test]
    fn test_array_empty() {
        let v = PvaValue::Structure(vec![
            ("value".into(), PvaValue::ScalarArray(vec![])),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("epics:nt/NTScalarArray:1.0"), &v).unwrap() {
            NormativeType::NTScalarArray(a) => assert_eq!(a.value.len(), 0),
            _ => panic!(),
        }
    }

    #[test]
    fn test_array_alarm() {
        let v = PvaValue::Structure(vec![
            ("value".into(), PvaValue::ScalarArray(vec![sv(1.0)])),
            alarm_fields(1),
            ts_fields(0, 0),
        ]);
        assert_eq!(
            to_normative(&desc("epics:nt/NTScalarArray:1.0"), &v)
                .unwrap()
                .alarm()
                .severity,
            AlarmSeverity::Minor
        );
    }

    #[test]
    fn test_array_no_value() {
        assert!(
            to_normative(
                &desc("epics:nt/NTScalarArray:1.0"),
                &PvaValue::Structure(vec![])
            )
            .is_none()
        );
    }

    #[test]
    fn test_table() {
        let v = PvaValue::Structure(vec![
            (
                "labels".into(),
                PvaValue::ScalarArray(vec![ss("x"), ss("y")]),
            ),
            (
                "value".into(),
                PvaValue::Structure(vec![
                    ("x".into(), PvaValue::ScalarArray(vec![sv(1.0), sv(2.0)])),
                    ("y".into(), PvaValue::ScalarArray(vec![sv(3.0), sv(4.0)])),
                ]),
            ),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("epics:nt/NTTable:1.0"), &v).unwrap() {
            NormativeType::NTTable(t) => {
                assert_eq!(t.labels, vec!["x", "y"]);
                assert_eq!(t.column_count(), 2);
                assert_eq!(t.row_count(), 2);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_table_empty() {
        let v = PvaValue::Structure(vec![
            ("labels".into(), PvaValue::ScalarArray(vec![])),
            ("value".into(), PvaValue::Structure(vec![])),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("epics:nt/NTTable:1.0"), &v).unwrap() {
            NormativeType::NTTable(t) => assert_eq!(t.column_count(), 0),
            _ => panic!(),
        }
    }

    #[test]
    fn test_ndarray() {
        let v = PvaValue::Structure(vec![
            (
                "value".into(),
                PvaValue::ScalarArray(vec![PvaScalar::UByte(0), PvaScalar::UByte(255)]),
            ),
            (
                "dimension".into(),
                PvaValue::Structure(vec![(
                    "dim0".into(),
                    PvaValue::Structure(vec![
                        ("size".into(), PvaValue::Scalar(si(2))),
                        ("offset".into(), PvaValue::Scalar(si(0))),
                    ]),
                )]),
            ),
            ("uniqueId".into(), PvaValue::Scalar(si(42))),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("epics:nt/NTNDArray:1.0"), &v).unwrap() {
            NormativeType::NTNDArray(nd) => {
                assert_eq!(nd.value.len(), 2);
                assert_eq!(nd.unique_id, 42);
                assert_eq!(nd.dimension.len(), 1);
                assert_eq!(nd.dimension[0].size, 2);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_ndarray_no_value() {
        assert!(
            to_normative(
                &desc("epics:nt/NTNDArray:1.0"),
                &PvaValue::Structure(vec![])
            )
            .is_none()
        );
    }

    #[test]
    fn test_custom_preserves_data() {
        let v = PvaValue::Structure(vec![
            ("x".into(), PvaValue::Scalar(sv(42.0))),
            alarm_fields(0),
            ts_fields(0, 0),
        ]);
        match to_normative(&desc("vendor:custom/Type:1.0"), &v).unwrap() {
            NormativeType::Custom(c) => {
                assert!(c.data.is_object());
                assert!(c.data["x"].is_number());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_custom_with_alarm() {
        let v = PvaValue::Structure(vec![alarm_fields(2), ts_fields(0, 0)]);
        assert_eq!(
            to_normative(&desc("vendor:x"), &v)
                .unwrap()
                .alarm()
                .severity,
            AlarmSeverity::Major
        );
    }

    #[test]
    fn test_alarm_default() {
        assert_eq!(
            extract_alarm(&PvaValue::Structure(vec![])).severity,
            AlarmSeverity::None
        );
    }

    #[test]
    fn test_alarm_major() {
        let v = PvaValue::Structure(vec![alarm_fields(2)]);
        assert_eq!(extract_alarm(&v).severity, AlarmSeverity::Major);
    }

    #[test]
    fn test_ts_default() {
        assert_eq!(
            extract_timestamp(&PvaValue::Structure(vec![])),
            TimeStamp::default()
        );
    }

    #[test]
    fn test_ts_values() {
        let v = PvaValue::Structure(vec![ts_fields(1234567890, 500)]);
        let ts = extract_timestamp(&v);
        assert_eq!(ts.seconds, 1234567890);
        assert_eq!(ts.nanoseconds, 500);
    }

    #[test]
    fn test_arr_bool() {
        let a = extract_array_value(&PvaValue::ScalarArray(vec![
            PvaScalar::Boolean(true),
            PvaScalar::Boolean(false),
        ]))
        .unwrap();
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn test_arr_byte() {
        let a = extract_array_value(&PvaValue::ScalarArray(vec![PvaScalar::Byte(-1)])).unwrap();
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn test_arr_short() {
        let a = extract_array_value(&PvaValue::ScalarArray(vec![PvaScalar::Short(1)])).unwrap();
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn test_arr_ushort() {
        let a = extract_array_value(&PvaValue::ScalarArray(vec![PvaScalar::UShort(1)])).unwrap();
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn test_arr_uint() {
        let a = extract_array_value(&PvaValue::ScalarArray(vec![PvaScalar::UInt(1)])).unwrap();
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn test_arr_long() {
        let a = extract_array_value(&PvaValue::ScalarArray(vec![PvaScalar::Long(1)])).unwrap();
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn test_arr_ulong() {
        let a = extract_array_value(&PvaValue::ScalarArray(vec![PvaScalar::ULong(1)])).unwrap();
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn test_arr_float() {
        let a = extract_array_value(&PvaValue::ScalarArray(vec![PvaScalar::Float(1.0)])).unwrap();
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn test_arr_not_array() {
        assert!(extract_array_value(&PvaValue::Null).is_none());
    }

    #[test]
    fn test_json_scalar() {
        assert_eq!(
            pva_value_to_json(&PvaValue::Scalar(sv(3.14))),
            serde_json::json!(3.14)
        );
    }

    #[test]
    fn test_json_string() {
        assert_eq!(
            pva_value_to_json(&PvaValue::Scalar(ss("hi"))),
            serde_json::json!("hi")
        );
    }

    #[test]
    fn test_json_bool() {
        assert_eq!(
            pva_value_to_json(&PvaValue::Scalar(PvaScalar::Boolean(true))),
            serde_json::json!(true)
        );
    }

    #[test]
    fn test_json_null() {
        assert_eq!(pva_value_to_json(&PvaValue::Null), serde_json::Value::Null);
    }

    #[test]
    fn test_json_array() {
        let j = pva_value_to_json(&PvaValue::ScalarArray(vec![sv(1.0), sv(2.0)]));
        assert!(j.is_array());
        assert_eq!(j.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_json_struct() {
        let j = pva_value_to_json(&PvaValue::Structure(vec![(
            "x".into(),
            PvaValue::Scalar(si(42)),
        )]));
        assert!(j.is_object());
        assert_eq!(j["x"], serde_json::json!(42));
    }
}