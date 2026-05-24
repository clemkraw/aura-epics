//! PVAccess scalar types: the 12 primitive value types.
//!
//! `ScalarType` is the **single source of truth** for scalar type tags
//! across the entire AURA pipeline.
//!
//! The `#[repr(u8)]` values match the PVA wire protocol specification.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Scalar type tag matching the PVA wire protocol encoding.
///
/// `#[repr(u8)]` values are the PVA specification byte codes:
/// Boolean=0, Byte=1, Short=2, Int=3, Long=4, UByte=5, UShort=6, UInt=7, ULong=8, Float=9, Double=10, String=11.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum ScalarType {
    Boolean = 0,
    Byte = 1,
    Short = 2,
    Int = 3,
    Long = 4,
    UByte = 5,
    UShort = 6,
    UInt = 7,
    ULong = 8,
    Float = 9,
    Double = 10,
    String = 11,
}

impl ScalarType {
    /// All 12 scalar types in PVA specification order.
    pub const ALL: [Self; 12] = [
        Self::Boolean,
        Self::Byte,
        Self::UByte,
        Self::Short,
        Self::UShort,
        Self::Int,
        Self::UInt,
        Self::Long,
        Self::ULong,
        Self::Float,
        Self::Double,
        Self::String,
    ];

    /// Decode from PVA wire byte. Returns `None` for invalid codes.
    pub fn from_wire(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Boolean),
            1 => Some(Self::Byte),
            2 => Some(Self::Short),
            3 => Some(Self::Int),
            4 => Some(Self::Long),
            5 => Some(Self::UByte),
            6 => Some(Self::UShort),
            7 => Some(Self::UInt),
            8 => Some(Self::ULong),
            9 => Some(Self::Float),
            10 => Some(Self::Double),
            11 => Some(Self::String),
            _ => None,
        }
    }

    /// Encode to PVA wire byte.
    #[inline]
    pub const fn to_wire(self) -> u8 {
        self as u8
    }

    /// Size in bytes of the wire representation (0 for String = variable).
    pub const fn wire_size(&self) -> usize {
        match self {
            Self::Boolean | Self::Byte | Self::UByte => 1,
            Self::Short | Self::UShort => 2,
            Self::Int | Self::UInt | Self::Float => 4,
            Self::Long | Self::ULong | Self::Double => 8,
            Self::String => 0,
        }
    }

    /// Whether this type can be losslessly converted to f64.
    pub const fn is_f64_exact(&self) -> bool {
        matches!(
            self,
            Self::Boolean
                | Self::Byte
                | Self::UByte
                | Self::Short
                | Self::UShort
                | Self::Int
                | Self::UInt
                | Self::Float
                | Self::Double
        )
    }

    /// Whether this is a numeric type (not Boolean, not String).
    pub const fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_floating()
    }

    /// Whether this is an integer type (signed or unsigned).
    pub const fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::Byte
                | Self::Short
                | Self::Int
                | Self::Long
                | Self::UByte
                | Self::UShort
                | Self::UInt
                | Self::ULong
        )
    }

    /// Whether this is a floating-point type.
    pub const fn is_floating(&self) -> bool {
        matches!(self, Self::Float | Self::Double)
    }

    /// Whether this is an unsigned integer type.
    pub const fn is_unsigned(&self) -> bool {
        matches!(self, Self::UByte | Self::UShort | Self::UInt | Self::ULong)
    }

    /// Short name for display/logging.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Byte => "byte",
            Self::Short => "short",
            Self::Int => "int",
            Self::Long => "long",
            Self::UByte => "ubyte",
            Self::UShort => "ushort",
            Self::UInt => "uint",
            Self::ULong => "ulong",
            Self::Float => "float",
            Self::Double => "double",
            Self::String => "string",
        }
    }
}

impl fmt::Display for ScalarType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A PVAccess scalar value — the `value` field of NTScalar.
///
/// Serialized as `{"type":"Double","v":4.217}` for Redis transport.
/// Used across the entire pipeline: wire → ingest → Redis → store → DB.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "v")]
pub enum ScalarValue {
    Boolean(bool),
    Byte(i8),
    UByte(u8),
    Short(i16),
    UShort(u16),
    Int(i32),
    UInt(u32),
    Long(i64),
    ULong(u64),
    Float(f32),
    Double(f64),
    String(String),
}

impl ScalarValue {
    pub const TRUE: Self = Self::Boolean(true);
    pub const FALSE: Self = Self::Boolean(false);
    pub const ZERO_INT: Self = Self::Int(0);
    pub const ZERO_DOUBLE: Self = Self::Double(0.0);

    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Boolean(v) => Some(if *v { 1.0 } else { 0.0 }),
            Self::Byte(v) => Some(*v as f64),
            Self::UByte(v) => Some(*v as f64),
            Self::Short(v) => Some(*v as f64),
            Self::UShort(v) => Some(*v as f64),
            Self::Int(v) => Some(*v as f64),
            Self::UInt(v) => Some(*v as f64),
            Self::Long(v) => Some(*v as f64),
            Self::ULong(v) => Some(*v as f64),
            Self::Float(v) => Some(*v as f64),
            Self::Double(v) => Some(*v),
            Self::String(_) => None,
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Boolean(v) => Some(if *v { 1 } else { 0 }),
            Self::Byte(v) => Some(*v as i64),
            Self::UByte(v) => Some(*v as i64),
            Self::Short(v) => Some(*v as i64),
            Self::UShort(v) => Some(*v as i64),
            Self::Int(v) => Some(*v as i64),
            Self::UInt(v) => Some(*v as i64),
            Self::Long(v) => Some(*v),
            Self::ULong(v) => Some(*v as i64),
            Self::Float(v) => Some(*v as i64),
            Self::Double(v) => Some(*v as i64),
            Self::String(_) => None,
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Option<i32> {
        self.as_i64().map(|v| v as i32)
    }

    #[inline]
    pub fn is_numeric(&self) -> bool {
        !matches!(self, Self::String(_))
    }

    pub fn as_string(&self) -> String {
        match self {
            Self::Boolean(v) => v.to_string(),
            Self::Byte(v) => v.to_string(),
            Self::UByte(v) => v.to_string(),
            Self::Short(v) => v.to_string(),
            Self::UShort(v) => v.to_string(),
            Self::Int(v) => v.to_string(),
            Self::UInt(v) => v.to_string(),
            Self::Long(v) => v.to_string(),
            Self::ULong(v) => v.to_string(),
            Self::Float(v) => v.to_string(),
            Self::Double(v) => v.to_string(),
            Self::String(v) => v.clone(),
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn type_tag(&self) -> ScalarType {
        match self {
            Self::Boolean(_) => ScalarType::Boolean,
            Self::Byte(_) => ScalarType::Byte,
            Self::UByte(_) => ScalarType::UByte,
            Self::Short(_) => ScalarType::Short,
            Self::UShort(_) => ScalarType::UShort,
            Self::Int(_) => ScalarType::Int,
            Self::UInt(_) => ScalarType::UInt,
            Self::Long(_) => ScalarType::Long,
            Self::ULong(_) => ScalarType::ULong,
            Self::Float(_) => ScalarType::Float,
            Self::Double(_) => ScalarType::Double,
            Self::String(_) => ScalarType::String,
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean(v) => write!(f, "{v}"),
            Self::Byte(v) => write!(f, "{v}"),
            Self::UByte(v) => write!(f, "{v}"),
            Self::Short(v) => write!(f, "{v}"),
            Self::UShort(v) => write!(f, "{v}"),
            Self::Int(v) => write!(f, "{v}"),
            Self::UInt(v) => write!(f, "{v}"),
            Self::Long(v) => write!(f, "{v}"),
            Self::ULong(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Double(v) => write!(f, "{v}"),
            Self::String(v) => write!(f, "{v}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_variants() -> Vec<(ScalarValue, ScalarType, &'static str, Option<f64>)> {
        vec![
            (
                ScalarValue::Boolean(true),
                ScalarType::Boolean,
                "true",
                Some(1.0),
            ),
            (
                ScalarValue::Byte(-128),
                ScalarType::Byte,
                "-128",
                Some(-128.0),
            ),
            (
                ScalarValue::UByte(255),
                ScalarType::UByte,
                "255",
                Some(255.0),
            ),
            (
                ScalarValue::Short(i16::MIN),
                ScalarType::Short,
                "-32768",
                Some(-32768.0),
            ),
            (
                ScalarValue::UShort(u16::MAX),
                ScalarType::UShort,
                "65535",
                Some(65535.0),
            ),
            (
                ScalarValue::Int(i32::MIN),
                ScalarType::Int,
                "-2147483648",
                Some(i32::MIN as f64),
            ),
            (
                ScalarValue::UInt(u32::MAX),
                ScalarType::UInt,
                "4294967295",
                Some(u32::MAX as f64),
            ),
            (
                ScalarValue::Long(i64::MIN),
                ScalarType::Long,
                "-9223372036854775808",
                Some(i64::MIN as f64),
            ),
            (
                ScalarValue::ULong(u64::MAX),
                ScalarType::ULong,
                "18446744073709551615",
                Some(u64::MAX as f64),
            ),
            (
                ScalarValue::Float(3.14),
                ScalarType::Float,
                "3.14",
                Some(3.14f32 as f64),
            ),
            (
                ScalarValue::Double(2.71828),
                ScalarType::Double,
                "2.71828",
                Some(2.71828),
            ),
            (
                ScalarValue::String("aura".into()),
                ScalarType::String,
                "aura",
                None,
            ),
        ]
    }

    // ── ScalarType: wire encoding ────────────────────────────────

    #[test]
    fn test_wire_roundtrip() {
        for st in ScalarType::ALL {
            assert_eq!(ScalarType::from_wire(st.to_wire()), Some(st), "{st:?}");
        }
    }
    #[test]
    fn test_wire_values() {
        assert_eq!(ScalarType::Boolean.to_wire(), 0);
        assert_eq!(ScalarType::Byte.to_wire(), 1);
        assert_eq!(ScalarType::Short.to_wire(), 2);
        assert_eq!(ScalarType::Int.to_wire(), 3);
        assert_eq!(ScalarType::Long.to_wire(), 4);
        assert_eq!(ScalarType::UByte.to_wire(), 5);
        assert_eq!(ScalarType::UShort.to_wire(), 6);
        assert_eq!(ScalarType::UInt.to_wire(), 7);
        assert_eq!(ScalarType::ULong.to_wire(), 8);
        assert_eq!(ScalarType::Float.to_wire(), 9);
        assert_eq!(ScalarType::Double.to_wire(), 10);
        assert_eq!(ScalarType::String.to_wire(), 11);
    }
    #[test]
    fn test_from_wire_invalid() {
        assert!(ScalarType::from_wire(12).is_none());
        assert!(ScalarType::from_wire(255).is_none());
    }

    // ── ScalarType: classification ───────────────────────────────

    #[test]
    fn test_is_integer() {
        assert!(ScalarType::Int.is_integer());
        assert!(!ScalarType::Double.is_integer());
        assert!(!ScalarType::Boolean.is_integer());
        assert!(!ScalarType::String.is_integer());
    }
    #[test]
    fn test_is_floating() {
        assert!(ScalarType::Float.is_floating());
        assert!(ScalarType::Double.is_floating());
        assert!(!ScalarType::Int.is_floating());
    }
    #[test]
    fn test_is_unsigned() {
        assert!(ScalarType::UByte.is_unsigned());
        assert!(ScalarType::UInt.is_unsigned());
        assert!(!ScalarType::Int.is_unsigned());
    }
    #[test]
    fn test_is_numeric() {
        assert!(ScalarType::Int.is_numeric());
        assert!(ScalarType::Double.is_numeric());
        assert!(!ScalarType::Boolean.is_numeric());
        assert!(!ScalarType::String.is_numeric());
    }
    #[test]
    fn test_is_f64_exact() {
        assert!(ScalarType::Int.is_f64_exact());
        assert!(!ScalarType::Long.is_f64_exact());
        assert!(!ScalarType::String.is_f64_exact());
    }

    // ── ScalarType: wire_size / name / display ───────────────────

    #[test]
    fn test_wire_size() {
        assert_eq!(ScalarType::Boolean.wire_size(), 1);
        assert_eq!(ScalarType::Short.wire_size(), 2);
        assert_eq!(ScalarType::Int.wire_size(), 4);
        assert_eq!(ScalarType::Double.wire_size(), 8);
        assert_eq!(ScalarType::String.wire_size(), 0);
    }
    #[test]
    fn test_name() {
        assert_eq!(ScalarType::Double.name(), "double");
        assert_eq!(ScalarType::UByte.name(), "ubyte");
    }
    #[test]
    fn test_display() {
        let e = [
            "boolean", "byte", "ubyte", "short", "ushort", "int", "uint", "long", "ulong", "float",
            "double", "string",
        ];
        for (st, exp) in ScalarType::ALL.iter().zip(e.iter()) {
            assert_eq!(st.to_string(), *exp);
        }
    }
    #[test]
    fn test_all_count() {
        assert_eq!(ScalarType::ALL.len(), 12);
    }
    #[test]
    fn test_all_unique() {
        use std::collections::HashSet;
        assert_eq!(ScalarType::ALL.iter().collect::<HashSet<_>>().len(), 12);
    }
    #[test]
    fn test_copy() {
        let a = ScalarType::Double;
        let b = a;
        assert_eq!(a, b);
    }
    #[test]
    fn test_hash() {
        use std::collections::HashMap;
        let mut m = HashMap::new();
        for st in ScalarType::ALL {
            m.insert(st, st.wire_size());
        }
        assert_eq!(m[&ScalarType::Double], 8);
    }

    // ── ScalarValue: as_f64, type_tag, Display ───────────────────

    #[test]
    fn test_as_f64_all() {
        for (v, _, _, e) in all_variants() {
            assert_eq!(v.as_f64(), e, "{v:?}");
        }
    }
    #[test]
    fn test_type_tag_all() {
        for (v, t, _, _) in all_variants() {
            assert_eq!(v.type_tag(), t, "{v:?}");
        }
    }
    #[test]
    fn test_display_all() {
        for (v, _, s, _) in all_variants() {
            assert_eq!(v.to_string(), s, "{v:?}");
        }
    }

    // ── ScalarValue: as_i64 / as_i32 / as_str ────────────────────

    #[test]
    fn test_i64() {
        assert_eq!(ScalarValue::Int(42).as_i64(), Some(42));
        assert_eq!(ScalarValue::String("x".into()).as_i64(), None);
    }
    #[test]
    fn test_i32() {
        assert_eq!(ScalarValue::Short(7).as_i32(), Some(7));
    }
    #[test]
    fn test_as_str() {
        assert_eq!(ScalarValue::String("hi".into()).as_str(), Some("hi"));
        assert!(ScalarValue::Int(0).as_str().is_none());
    }

    // ── ScalarValue: edge cases ──────────────────────────────────

    #[test]
    fn test_bool_false() {
        assert_eq!(ScalarValue::Boolean(false).as_f64(), Some(0.0));
    }
    #[test]
    fn test_float_nan() {
        assert!(ScalarValue::Float(f32::NAN).as_f64().unwrap().is_nan());
    }
    #[test]
    fn test_float_inf() {
        assert_eq!(
            ScalarValue::Float(f32::INFINITY).as_f64(),
            Some(f64::INFINITY)
        );
    }
    #[test]
    fn test_string_empty() {
        assert!(!ScalarValue::String(String::new()).is_numeric());
    }
    #[test]
    fn test_cross_type_ne() {
        assert_ne!(ScalarValue::Int(1), ScalarValue::UInt(1));
        assert_ne!(ScalarValue::Float(1.0), ScalarValue::Double(1.0));
    }
    #[test]
    fn test_constants() {
        assert_eq!(ScalarValue::TRUE.as_f64(), Some(1.0));
        assert_eq!(ScalarValue::ZERO_INT.as_f64(), Some(0.0));
    }
    #[test]
    fn test_clone() {
        for (v, _, _, _) in all_variants() {
            assert_eq!(v.clone(), v);
        }
    }

    // ── Serde ────────────────────────────────────────────────────

    #[test]
    fn test_serde_roundtrip() {
        for (v, _, _, _) in all_variants() {
            let json = serde_json::to_string(&v).unwrap();
            let back: ScalarValue = serde_json::from_str(&json).unwrap();
            if let Some(f) = v.as_f64() {
                if f.is_nan() {
                    continue;
                }
            }
            assert_eq!(v, back, "{v:?}");
        }
    }
    #[test]
    fn test_serde_format() {
        assert_eq!(
            serde_json::to_string(&ScalarValue::Double(4.217)).unwrap(),
            r#"{"type":"Double","v":4.217}"#
        );
        assert_eq!(
            serde_json::to_string(&ScalarValue::Int(-42)).unwrap(),
            r#"{"type":"Int","v":-42}"#
        );
    }
    #[test]
    fn test_scalar_type_serde() {
        for st in ScalarType::ALL {
            let j = serde_json::to_string(&st).unwrap();
            assert_eq!(serde_json::from_str::<ScalarType>(&j).unwrap(), st);
        }
    }
    #[test]
    fn test_scalar_type_lowercase() {
        assert_eq!(
            serde_json::to_string(&ScalarType::UByte).unwrap(),
            r#""ubyte""#
        );
    }
}
