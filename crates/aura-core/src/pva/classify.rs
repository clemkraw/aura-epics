//! PV data type classification — routes data to the correct hypertable.
//!
//! Every PV monitor update must be stored in the appropriate TimescaleDB
//! table based on its Normative Type. This module provides the routing
//! logic: [`PvDataType::from_nt`] inspects a [`NormativeType`] and
//! returns the storage classification.
//!
//! Storage mapping:
//! | PvDataType    | Target table         | Format              |
//! |---------------|----------------------|---------------------|
//! | Scalar        | `samples`            | (time, pv_id, f64)  |
//! | String        | `samples_string`     | (time, pv_id, text)  |
//! | Array         | `samples_array`      | (time, pv_id, f64[]) |
//! | Matrix        | `samples_array`      | + dim metadata       |
//! | Histogram     | `samples_histogram`  | ranges + counts      |
//! | Continuum     | `samples_continuum`  | base + traces        |
//! | NameValue     | `samples_namevalue`  | names + values       |
//! | Table         | `samples_table`      | JSON                 |
//! | Image         | `samples_image`      | blob / reference     |
//! | MultiChannel  | `samples_multi`      | per-channel values   |
//! | Aggregate     | `samples`            | (time, pv_id, f64)   |
//! | Union         | depends on runtime   | resolved at ingest   |
//! | Custom        | `samples_custom`     | JSON                 |

use serde::{Deserialize, Serialize};
use std::fmt;

use super::normative::NormativeType;
use super::scalars::ScalarValue;

/// Determines which storage table receives the data.
///
/// This enum is the bridge between the PVAccess type system and the
/// TimescaleDB schema. The `aura-store` writer uses this to dispatch
/// each sample to the correct hypertable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PvDataType {
    /// Scalar numeric (double, int, etc.) or enum → `samples`
    Scalar,
    /// String scalar → `samples_string`
    String,
    /// Numeric array / waveform → `samples_array`
    Array,
    /// Matrix (2D) → `samples_array` + dimension metadata
    Matrix,
    /// Histogram (bins + counts) → `samples_histogram`
    Histogram,
    /// Continuum (multi-trace) → `samples_continuum`
    Continuum,
    /// Name-value pairs → `samples_namevalue`
    NameValue,
    /// Columnar table → `samples_table` (JSON)
    Table,
    /// Image / NDArray → `samples_image`
    Image,
    /// Multi-channel snapshot → `samples_multi`
    MultiChannel,
    /// Pre-computed statistics → `samples` (value = aggregate mean)
    Aggregate,
    /// Union → resolved at runtime, depends on inner type
    Union,
    /// Non-standard structure → `samples_custom` (JSON)
    Custom,
}

impl PvDataType {
    /// All 13 data type classifications.
    pub const ALL: [Self; 13] = [
        Self::Scalar, Self::String, Self::Array, Self::Matrix,
        Self::Histogram, Self::Continuum, Self::NameValue, Self::Table,
        Self::Image, Self::MultiChannel, Self::Aggregate,
        Self::Union, Self::Custom,
    ];

    /// Classify a Normative Type into its storage destination.
    ///
    /// This is the core routing decision. Called once per PV at
    /// subscription time and cached in the PV metadata.
    #[inline]
    pub fn from_nt(nt: &NormativeType) -> Self {
        match nt {
            NormativeType::NTScalar(s) => {
                if matches!(s.value, ScalarValue::String(_)) {
                    Self::String
                } else {
                    Self::Scalar
                }
            }
            NormativeType::NTEnum(_)         => Self::Scalar,
            NormativeType::NTScalarArray(_)  => Self::Array,
            NormativeType::NTMatrix(_)       => Self::Matrix,
            NormativeType::NTHistogram(_)    => Self::Histogram,
            NormativeType::NTContinuum(_)    => Self::Continuum,
            NormativeType::NTNameValue(_)    => Self::NameValue,
            NormativeType::NTTable(_)        => Self::Table,
            NormativeType::NTNDArray(_)      => Self::Image,
            NormativeType::NTMultiChannel(_) => Self::MultiChannel,
            NormativeType::NTAggregate(_)    => Self::Aggregate,
            NormativeType::NTUnion(_)        => Self::Union,
            NormativeType::Custom(_)         => Self::Custom,
        }
    }

    /// The target TimescaleDB table name for this data type.
    pub const fn table_name(&self) -> &'static str {
        match self {
            Self::Scalar      => "samples",
            Self::String      => "samples_string",
            Self::Array       => "samples_array",
            Self::Matrix      => "samples_array",
            Self::Histogram   => "samples_histogram",
            Self::Continuum   => "samples_continuum",
            Self::NameValue   => "samples_namevalue",
            Self::Table       => "samples_table",
            Self::Image       => "samples_image",
            Self::MultiChannel => "samples_multi",
            Self::Aggregate   => "samples",
            Self::Union       => "samples_custom",
            Self::Custom      => "samples_custom",
        }
    }

    /// Whether this type stores a single f64 value per sample
    /// (can use the epsilon-deadband filter directly).
    pub const fn is_scalar_filterable(&self) -> bool {
        matches!(self, Self::Scalar | Self::Aggregate)
    }

    /// Whether this type is stored as JSON (complex structures).
    pub const fn is_json_stored(&self) -> bool {
        matches!(self, Self::Table | Self::Custom | Self::Union)
    }

    /// Whether this type contains array/bulk data.
    pub const fn is_bulk_data(&self) -> bool {
        matches!(self, Self::Array | Self::Matrix | Self::Image
            | Self::Histogram | Self::Continuum)
    }
}

impl fmt::Display for PvDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Scalar       => "scalar",
            Self::String       => "string",
            Self::Array        => "array",
            Self::Matrix       => "matrix",
            Self::Histogram    => "histogram",
            Self::Continuum    => "continuum",
            Self::NameValue    => "namevalue",
            Self::Table        => "table",
            Self::Image        => "image",
            Self::MultiChannel => "multichannel",
            Self::Aggregate    => "aggregate",
            Self::Union        => "union",
            Self::Custom       => "custom",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::alarm::Alarm;
    use super::super::arrays::ArrayValue;
    use super::super::enums::EnumValue;
    use super::super::ndarray::{Codec, Dimension};
    use super::super::normative::*;
    use super::super::table::HistogramValue;
    use super::super::time::TimeStamp;
    use super::super::union::UnionValue;

    fn ts() -> TimeStamp { TimeStamp::new(0, 0) }

    // ── Helper: build one NT per variant ─────────────────────────────

    fn all_nt_with_expected() -> Vec<(NormativeType, PvDataType, &'static str)> {
        // (normative_type, expected_classification, description)
        vec![
            (
                NormativeType::NTScalar(NTScalar {
                    value: ScalarValue::Double(4.2), alarm: Alarm::default(),
                    timestamp: ts(), display: None, control: None, value_alarm: None,
                }),
                PvDataType::Scalar, "NTScalar(Double)"
            ),
            (
                NormativeType::NTScalar(NTScalar {
                    value: ScalarValue::Int(-1), alarm: Alarm::default(),
                    timestamp: ts(), display: None, control: None, value_alarm: None,
                }),
                PvDataType::Scalar, "NTScalar(Int)"
            ),
            (
                NormativeType::NTScalar(NTScalar {
                    value: ScalarValue::Boolean(true), alarm: Alarm::default(),
                    timestamp: ts(), display: None, control: None, value_alarm: None,
                }),
                PvDataType::Scalar, "NTScalar(Boolean)"
            ),
            (
                NormativeType::NTScalar(NTScalar {
                    value: ScalarValue::String("hello".into()), alarm: Alarm::default(),
                    timestamp: ts(), display: None, control: None, value_alarm: None,
                }),
                PvDataType::String, "NTScalar(String)"
            ),
            (
                NormativeType::NTEnum(NTEnum {
                    value: EnumValue { index: 0, choices: vec!["Off".into(), "On".into()] },
                    alarm: Alarm::default(), timestamp: ts(),
                }),
                PvDataType::Scalar, "NTEnum"
            ),
            (
                NormativeType::NTScalarArray(NTScalarArray {
                    value: ArrayValue::DoubleArray(vec![1.0, 2.0]),
                    alarm: Alarm::default(), timestamp: ts(),
                    display: None, control: None, value_alarm: None,
                }),
                PvDataType::Array, "NTScalarArray"
            ),
            (
                NormativeType::NTMatrix(NTMatrix {
                    value: vec![1.0, 2.0, 3.0, 4.0], dim: vec![2, 2],
                    descriptor: String::new(), alarm: Alarm::default(),
                    timestamp: ts(), display: None,
                }),
                PvDataType::Matrix, "NTMatrix"
            ),
            (
                NormativeType::NTHistogram(NTHistogram {
                    ranges: vec![0.0, 1.0, 2.0],
                    value: HistogramValue::Int(vec![10, 20]),
                    descriptor: String::new(), alarm: Alarm::default(),
                    timestamp: ts(),
                }),
                PvDataType::Histogram, "NTHistogram"
            ),
            (
                NormativeType::NTContinuum(NTContinuum {
                    base: vec![0.0, 1.0], value: vec![1.0, 2.0],
                    units: vec!["s".into(), "V".into()],
                    descriptor: String::new(), alarm: Alarm::default(),
                    timestamp: ts(),
                }),
                PvDataType::Continuum, "NTContinuum"
            ),
            (
                NormativeType::NTNameValue(NTNameValue {
                    name: vec!["gain".into()],
                    value: ArrayValue::DoubleArray(vec![1.5]),
                    descriptor: String::new(), alarm: Alarm::default(),
                    timestamp: ts(),
                }),
                PvDataType::NameValue, "NTNameValue"
            ),
            (
                NormativeType::NTTable(NTTable {
                    labels: vec!["x".into()],
                    columns: vec![super::super::table::TableColumn {
                        name: "x".into(),
                        values: ArrayValue::DoubleArray(vec![1.0]),
                    }],
                    alarm: Alarm::default(), timestamp: ts(),
                }),
                PvDataType::Table, "NTTable"
            ),
            (
                NormativeType::NTNDArray(NTNDArray {
                    value: ArrayValue::UByteArray(vec![0; 100]),
                    codec: Codec::default(),
                    compressed_size: 0, uncompressed_size: 100,
                    dimension: vec![Dimension { size: 10, offset: 0, full_size: 10, binning: 1, reverse: false }],
                    unique_id: 1, data_timestamp: None,
                    alarm: Alarm::default(), timestamp: ts(),
                    attribute: vec![],
                }),
                PvDataType::Image, "NTNDArray"
            ),
            (
                NormativeType::NTMultiChannel(NTMultiChannel {
                    values: vec![ScalarValue::Double(1.0)],
                    channel_name: vec!["PV:A".into()],
                    is_connected: vec![true],
                    severity: vec![], status: vec![], message: vec![],
                    seconds_past_epoch: vec![], nanoseconds: vec![],
                    alarm: Alarm::default(), timestamp: ts(),
                }),
                PvDataType::MultiChannel, "NTMultiChannel"
            ),
            (
                NormativeType::NTAggregate(NTAggregate {
                    value: 4.2, n: 100, dispersion: 0.01,
                    first: 4.1, last: 4.3, max: 4.5, min: 3.9,
                    alarm: Alarm::default(), timestamp: ts(),
                }),
                PvDataType::Aggregate, "NTAggregate"
            ),
            (
                NormativeType::NTUnion(NTUnion {
                    value: UnionValue::Scalar(ScalarValue::Double(0.0)),
                    descriptor: String::new(), alarm: Alarm::default(),
                    timestamp: ts(),
                }),
                PvDataType::Union, "NTUnion"
            ),
            (
                NormativeType::Custom(CustomStructure {
                    data: serde_json::json!({"x": 1}),
                    alarm: Alarm::default(), timestamp: ts(),
                }),
                PvDataType::Custom, "Custom"
            ),
        ]
    }

    // ── from_nt: exhaustive classification ───────────────────────────

    #[test]
    fn test_from_nt_all_variants() {
        for (nt, expected, desc) in all_nt_with_expected() {
            assert_eq!(PvDataType::from_nt(&nt), expected,
                       "classification failed for {}", desc);
        }
    }

    #[test]
    fn test_from_nt_all_scalar_subtypes() {
        // Every non-String ScalarValue must classify as Scalar
        let numeric_values = vec![
            ScalarValue::Boolean(false),
            ScalarValue::Byte(0),
            ScalarValue::UByte(0),
            ScalarValue::Short(0),
            ScalarValue::UShort(0),
            ScalarValue::Int(0),
            ScalarValue::UInt(0),
            ScalarValue::Long(0),
            ScalarValue::ULong(0),
            ScalarValue::Float(0.0),
            ScalarValue::Double(0.0),
        ];
        for val in numeric_values {
            let nt = NormativeType::NTScalar(NTScalar {
                value: val.clone(), alarm: Alarm::default(), timestamp: ts(),
                display: None, control: None, value_alarm: None,
            });
            assert_eq!(PvDataType::from_nt(&nt), PvDataType::Scalar,
                       "numeric ScalarValue {:?} should classify as Scalar", val);
        }
    }

    // ── table_name ───────────────────────────────────────────────────

    #[test]
    fn test_table_name_all() {
        let expected: Vec<(PvDataType, &str)> = vec![
            (PvDataType::Scalar,       "samples"),
            (PvDataType::String,       "samples_string"),
            (PvDataType::Array,        "samples_array"),
            (PvDataType::Matrix,       "samples_array"),
            (PvDataType::Histogram,    "samples_histogram"),
            (PvDataType::Continuum,    "samples_continuum"),
            (PvDataType::NameValue,    "samples_namevalue"),
            (PvDataType::Table,        "samples_table"),
            (PvDataType::Image,        "samples_image"),
            (PvDataType::MultiChannel, "samples_multi"),
            (PvDataType::Aggregate,    "samples"),
            (PvDataType::Union,        "samples_custom"),
            (PvDataType::Custom,       "samples_custom"),
        ];
        for (dt, name) in expected {
            assert_eq!(dt.table_name(), name, "table_name wrong for {:?}", dt);
        }
    }

    #[test]
    fn test_scalar_and_aggregate_share_table() {
        assert_eq!(PvDataType::Scalar.table_name(), PvDataType::Aggregate.table_name());
    }

    #[test]
    fn test_union_and_custom_share_table() {
        assert_eq!(PvDataType::Union.table_name(), PvDataType::Custom.table_name());
    }

    // ── is_scalar_filterable ─────────────────────────────────────────

    #[test]
    fn test_is_scalar_filterable() {
        for dt in PvDataType::ALL {
            let expected = matches!(dt, PvDataType::Scalar | PvDataType::Aggregate);
            assert_eq!(dt.is_scalar_filterable(), expected,
                       "is_scalar_filterable wrong for {:?}", dt);
        }
    }

    // ── is_json_stored ───────────────────────────────────────────────

    #[test]
    fn test_is_json_stored() {
        for dt in PvDataType::ALL {
            let expected = matches!(dt, PvDataType::Table | PvDataType::Custom | PvDataType::Union);
            assert_eq!(dt.is_json_stored(), expected,
                       "is_json_stored wrong for {:?}", dt);
        }
    }

    // ── is_bulk_data ─────────────────────────────────────────────────

    #[test]
    fn test_is_bulk_data() {
        for dt in PvDataType::ALL {
            let expected = matches!(dt,
                PvDataType::Array | PvDataType::Matrix | PvDataType::Image
                | PvDataType::Histogram | PvDataType::Continuum);
            assert_eq!(dt.is_bulk_data(), expected,
                       "is_bulk_data wrong for {:?}", dt);
        }
    }

    // ── Display ──────────────────────────────────────────────────────

    #[test]
    fn test_display_all() {
        let expected = [
            "scalar", "string", "array", "matrix", "histogram",
            "continuum", "namevalue", "table", "image",
            "multichannel", "aggregate", "union", "custom",
        ];
        for (dt, exp) in PvDataType::ALL.iter().zip(expected.iter()) {
            assert_eq!(dt.to_string(), *exp);
        }
    }

    // ── ALL constant ─────────────────────────────────────────────────

    #[test]
    fn test_all_count() {
        assert_eq!(PvDataType::ALL.len(), 13);
    }

    #[test]
    fn test_all_unique() {
        use std::collections::HashSet;
        let set: HashSet<PvDataType> = PvDataType::ALL.iter().copied().collect();
        assert_eq!(set.len(), 13);
    }

    // ── Serde ────────────────────────────────────────────────────────

    #[test]
    fn test_serde_roundtrip_all() {
        for dt in PvDataType::ALL {
            let json = serde_json::to_string(&dt).unwrap();
            let back: PvDataType = serde_json::from_str(&json).unwrap();
            assert_eq!(dt, back, "serde roundtrip failed for {:?}", dt);
        }
    }

    #[test]
    fn test_serde_json_format() {
        assert_eq!(serde_json::to_string(&PvDataType::Scalar).unwrap(), r#""Scalar""#);
        assert_eq!(serde_json::to_string(&PvDataType::Image).unwrap(), r#""Image""#);
    }

    // ── Copy / Hash / Debug ──────────────────────────────────────────

    #[test]
    fn test_copy() {
        let a = PvDataType::Scalar;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_hash_as_map_key() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        for dt in PvDataType::ALL {
            map.insert(dt, dt.table_name());
        }
        assert_eq!(map.len(), 13);
        assert_eq!(map[&PvDataType::Scalar], "samples");
    }

    #[test]
    fn test_debug_not_empty() {
        for dt in PvDataType::ALL {
            assert!(!format!("{:?}", dt).is_empty());
        }
    }

    // ── Cross-cutting: from_nt → table_name consistency ──────────────

    #[test]
    fn test_from_nt_to_table_name_consistency() {
        for (nt, expected_dt, desc) in all_nt_with_expected() {
            let dt = PvDataType::from_nt(&nt);
            assert_eq!(dt, expected_dt, "classification for {}", desc);
            // table_name must not be empty
            assert!(!dt.table_name().is_empty(), "empty table for {}", desc);
            // table_name must start with "samples"
            assert!(dt.table_name().starts_with("samples"),
                    "table '{}' doesn't start with 'samples' for {}", dt.table_name(), desc);
        }
    }
}