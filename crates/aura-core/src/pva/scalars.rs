//! PVAccess scalar types: the 12 primitive value types.
//!
//! These are the atomic building blocks of all PVA data structures.
//! Every field in a Normative Type ultimately resolves to one of these.
//!
//! Reference: EPICS PVAccess Normative Types Specification, Section 3.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Scalar type tag (meta-information, no value).
///
/// Used for introspection, schema description, and storage routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScalarType {
    Boolean,
    Byte,
    UByte,
    Short,
    UShort,
    Int,
    UInt,
    Long,
    ULong,
    Float,
    Double,
    String,
}

impl ScalarType {
    /// All 12 scalar types in PVA specification order.
    pub const ALL: [Self; 12] = [
        Self::Boolean, Self::Byte, Self::UByte, Self::Short, Self::UShort,
        Self::Int, Self::UInt, Self::Long, Self::ULong,
        Self::Float, Self::Double, Self::String,
    ];

    /// Size in bytes of the wire representation (0 for String = variable).
    pub const fn wire_size(&self) -> usize {
        match self {
            Self::Boolean | Self::Byte | Self::UByte => 1,
            Self::Short | Self::UShort => 2,
            Self::Int | Self::UInt | Self::Float => 4,
            Self::Long | Self::ULong | Self::Double => 8,
            Self::String => 0, // variable length
        }
    }

    /// Whether this type can be losslessly converted to f64.
    /// False for Long, ULong (values > 2^53 lose precision), and String.
    pub const fn is_f64_exact(&self) -> bool {
        matches!(
            self,
            Self::Boolean | Self::Byte | Self::UByte
            | Self::Short | Self::UShort | Self::Int | Self::UInt
            | Self::Float | Self::Double
        )
    }

    /// Whether this is a numeric type (everything except String).
    pub const fn is_numeric(&self) -> bool {
        !matches!(self, Self::String)
    }
}

impl fmt::Display for ScalarType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Boolean => "boolean",
            Self::Byte => "byte",
            Self::UByte => "ubyte",
            Self::Short => "short",
            Self::UShort => "ushort",
            Self::Int => "int",
            Self::UInt => "uint",
            Self::Long => "long",
            Self::ULong => "ulong",
            Self::Float => "float",
            Self::Double => "double",
            Self::String => "string",
        })
    }
}

/// A PVAccess scalar value — the `value` field of NTScalar.
///
/// Tagged enum carrying one of the 12 PVA primitive types.
/// Serialized as `{"type":"Double","v":4.217}` for Redis transport.
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
}

impl ScalarValue {
    /// Convert to f64 for archiving. Returns `None` for String.
    ///
    /// **Precision note:** `Long` and `ULong` values exceeding 2^53
    /// will lose precision when converted to f64.
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

    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.type_tag().is_numeric()
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean(v) => write!(f, "{}", v),
            Self::Byte(v) => write!(f, "{}", v),
            Self::UByte(v) => write!(f, "{}", v),
            Self::Short(v) => write!(f, "{}", v),
            Self::UShort(v) => write!(f, "{}", v),
            Self::Int(v) => write!(f, "{}", v),
            Self::UInt(v) => write!(f, "{}", v),
            Self::Long(v) => write!(f, "{}", v),
            Self::ULong(v) => write!(f, "{}", v),
            Self::Float(v) => write!(f, "{}", v),
            Self::Double(v) => write!(f, "{}", v),
            Self::String(v) => write!(f, "{}", v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test data: one representative of each variant ─────────────────

    /// Returns (value, type_tag, display_string, expected_f64) for all 12 types.
    fn all_variants() -> Vec<(ScalarValue, ScalarType, &'static str, Option<f64>)> {
        vec![
            (ScalarValue::Boolean(true),        ScalarType::Boolean, "true",      Some(1.0)),
            (ScalarValue::Byte(-128),           ScalarType::Byte,    "-128",      Some(-128.0)),
            (ScalarValue::UByte(255),           ScalarType::UByte,   "255",       Some(255.0)),
            (ScalarValue::Short(i16::MIN),      ScalarType::Short,   "-32768",    Some(-32768.0)),
            (ScalarValue::UShort(u16::MAX),     ScalarType::UShort,  "65535",     Some(65535.0)),
            (ScalarValue::Int(i32::MIN),        ScalarType::Int,     "-2147483648", Some(i32::MIN as f64)),
            (ScalarValue::UInt(u32::MAX),       ScalarType::UInt,    "4294967295", Some(u32::MAX as f64)),
            (ScalarValue::Long(i64::MIN),       ScalarType::Long,    "-9223372036854775808", Some(i64::MIN as f64)),
            (ScalarValue::ULong(u64::MAX),      ScalarType::ULong,   "18446744073709551615", Some(u64::MAX as f64)),
            (ScalarValue::Float(3.14),          ScalarType::Float,   "3.14",      Some(3.14f32 as f64)),
            (ScalarValue::Double(2.71828),      ScalarType::Double,  "2.71828",   Some(2.71828)),
            (ScalarValue::String("aura".into()),ScalarType::String,  "aura",      None),
        ]
    }

    // ── Exhaustive: as_f64, type_tag, Display ────────────────────────

    #[test]
    fn test_as_f64_all_variants() {
        for (val, _, _, expected) in all_variants() {
            assert_eq!(val.as_f64(), expected, "as_f64 failed for {:?}", val);
        }
    }

    #[test]
    fn test_type_tag_all_variants() {
        for (val, expected_tag, _, _) in all_variants() {
            assert_eq!(val.type_tag(), expected_tag, "type_tag failed for {:?}", val);
        }
    }

    #[test]
    fn test_display_all_variants() {
        for (val, _, expected_str, _) in all_variants() {
            assert_eq!(val.to_string(), expected_str, "Display failed for {:?}", val);
        }
    }

    // ── Boolean edge cases ───────────────────────────────────────────

    #[test]
    fn test_boolean_false() {
        let v = ScalarValue::Boolean(false);
        assert_eq!(v.as_f64(), Some(0.0));
        assert_eq!(v.to_string(), "false");
        assert!(v.is_numeric());
    }

    // ── Boundary values ──────────────────────────────────────────────

    #[test]
    fn test_boundary_byte() {
        assert_eq!(ScalarValue::Byte(i8::MIN).as_f64(), Some(-128.0));
        assert_eq!(ScalarValue::Byte(i8::MAX).as_f64(), Some(127.0));
        assert_eq!(ScalarValue::Byte(0).as_f64(), Some(0.0));
    }

    #[test]
    fn test_boundary_ubyte() {
        assert_eq!(ScalarValue::UByte(0).as_f64(), Some(0.0));
        assert_eq!(ScalarValue::UByte(u8::MAX).as_f64(), Some(255.0));
    }

    #[test]
    fn test_boundary_short() {
        assert_eq!(ScalarValue::Short(i16::MIN).as_f64(), Some(i16::MIN as f64));
        assert_eq!(ScalarValue::Short(i16::MAX).as_f64(), Some(i16::MAX as f64));
    }

    #[test]
    fn test_boundary_ushort() {
        assert_eq!(ScalarValue::UShort(0).as_f64(), Some(0.0));
        assert_eq!(ScalarValue::UShort(u16::MAX).as_f64(), Some(u16::MAX as f64));
    }

    #[test]
    fn test_boundary_int() {
        assert_eq!(ScalarValue::Int(i32::MIN).as_f64(), Some(i32::MIN as f64));
        assert_eq!(ScalarValue::Int(i32::MAX).as_f64(), Some(i32::MAX as f64));
    }

    #[test]
    fn test_boundary_uint() {
        assert_eq!(ScalarValue::UInt(0).as_f64(), Some(0.0));
        assert_eq!(ScalarValue::UInt(u32::MAX).as_f64(), Some(u32::MAX as f64));
    }

    #[test]
    fn test_boundary_long() {
        assert_eq!(ScalarValue::Long(i64::MIN).as_f64(), Some(i64::MIN as f64));
        assert_eq!(ScalarValue::Long(i64::MAX).as_f64(), Some(i64::MAX as f64));
        assert_eq!(ScalarValue::Long(0).as_f64(), Some(0.0));
    }

    #[test]
    fn test_boundary_ulong() {
        assert_eq!(ScalarValue::ULong(0).as_f64(), Some(0.0));
        assert_eq!(ScalarValue::ULong(u64::MAX).as_f64(), Some(u64::MAX as f64));
    }

    // ── Float special values ─────────────────────────────────────────

    #[test]
    fn test_float_special_values() {
        // NaN
        let nan = ScalarValue::Float(f32::NAN);
        assert!(nan.as_f64().unwrap().is_nan());

        // Infinity
        let inf = ScalarValue::Float(f32::INFINITY);
        assert_eq!(inf.as_f64(), Some(f64::INFINITY));

        // Negative infinity
        let ninf = ScalarValue::Float(f32::NEG_INFINITY);
        assert_eq!(ninf.as_f64(), Some(f64::NEG_INFINITY));

        // Negative zero
        let nz = ScalarValue::Float(-0.0f32);
        assert_eq!(nz.as_f64(), Some(0.0));
    }

    #[test]
    fn test_double_special_values() {
        assert!(ScalarValue::Double(f64::NAN).as_f64().unwrap().is_nan());
        assert_eq!(ScalarValue::Double(f64::INFINITY).as_f64(), Some(f64::INFINITY));
        assert_eq!(ScalarValue::Double(f64::NEG_INFINITY).as_f64(), Some(f64::NEG_INFINITY));
    }

    // ── String edge cases ────────────────────────────────────────────

    #[test]
    fn test_string_empty() {
        let v = ScalarValue::String(String::new());
        assert_eq!(v.as_f64(), None);
        assert_eq!(v.to_string(), "");
        assert!(!v.is_numeric());
    }

    #[test]
    fn test_string_unicode() {
        let v = ScalarValue::String("température — 4.2 K 🌡️".into());
        assert_eq!(v.as_f64(), None);
        assert!(v.to_string().contains("température"));
    }

    #[test]
    fn test_string_numeric_content_not_parsed() {
        // A String containing "42.0" must NOT return Some(42.0)
        let v = ScalarValue::String("42.0".into());
        assert_eq!(v.as_f64(), None);
    }

    // ── is_numeric ───────────────────────────────────────────────────

    #[test]
    fn test_is_numeric() {
        for (val, tag, _, _) in all_variants() {
            let expected = tag != ScalarType::String;
            assert_eq!(val.is_numeric(), expected, "is_numeric failed for {:?}", val);
        }
    }

    // ── Cross-type inequality ────────────────────────────────────────

    #[test]
    fn test_different_types_not_equal() {
        // Same numeric value but different type → not equal
        assert_ne!(ScalarValue::Int(1), ScalarValue::UInt(1));
        assert_ne!(ScalarValue::Int(0), ScalarValue::Double(0.0));
        assert_ne!(ScalarValue::Float(1.0), ScalarValue::Double(1.0));
        assert_ne!(ScalarValue::Byte(1), ScalarValue::UByte(1));
        assert_ne!(ScalarValue::Short(1), ScalarValue::Int(1));
        assert_ne!(ScalarValue::Long(1), ScalarValue::ULong(1));
        assert_ne!(ScalarValue::Boolean(true), ScalarValue::Int(1));
    }

    // ── Sentinel constants ───────────────────────────────────────────

    #[test]
    fn test_sentinel_constants() {
        assert_eq!(ScalarValue::TRUE.as_f64(), Some(1.0));
        assert_eq!(ScalarValue::FALSE.as_f64(), Some(0.0));
        assert_eq!(ScalarValue::ZERO_INT.as_f64(), Some(0.0));
        assert_eq!(ScalarValue::ZERO_DOUBLE.as_f64(), Some(0.0));
    }

    // ── Clone / PartialEq / Debug ────────────────────────────────────

    #[test]
    fn test_clone_equals_original() {
        for (val, _, _, _) in all_variants() {
            let cloned = val.clone();
            assert_eq!(val, cloned);
        }
    }

    #[test]
    fn test_debug_not_empty() {
        for (val, _, _, _) in all_variants() {
            assert!(!format!("{:?}", val).is_empty());
        }
    }

    // ── Serde: ScalarValue ───────────────────────────────────────────

    #[test]
    fn test_serde_roundtrip_all_variants() {
        for (val, _, _, _) in all_variants() {
            let json = serde_json::to_string(&val).unwrap();
            let back: ScalarValue = serde_json::from_str(&json).unwrap();
            // NaN != NaN, so skip direct comparison for NaN
            if let Some(f) = val.as_f64() {
                if f.is_nan() { continue; }
            }
            assert_eq!(val, back, "serde roundtrip failed for {:?}", val);
        }
    }

    #[test]
    fn test_serde_json_format_contract() {
        // Lock the wire format: {"type":"<Variant>","v":<value>}
        assert_eq!(
            serde_json::to_string(&ScalarValue::Double(4.217)).unwrap(),
            r#"{"type":"Double","v":4.217}"#
        );
        assert_eq!(
            serde_json::to_string(&ScalarValue::Int(-42)).unwrap(),
            r#"{"type":"Int","v":-42}"#
        );
        assert_eq!(
            serde_json::to_string(&ScalarValue::Boolean(true)).unwrap(),
            r#"{"type":"Boolean","v":true}"#
        );
        assert_eq!(
            serde_json::to_string(&ScalarValue::String("pv".into())).unwrap(),
            r#"{"type":"String","v":"pv"}"#
        );
    }

    // ── Serde: ScalarType ────────────────────────────────────────────

    #[test]
    fn test_scalar_type_serde_roundtrip() {
        for st in ScalarType::ALL {
            let json = serde_json::to_string(&st).unwrap();
            let back: ScalarType = serde_json::from_str(&json).unwrap();
            assert_eq!(st, back);
        }
    }

    #[test]
    fn test_scalar_type_serde_lowercase() {
        // Verify rename_all = "lowercase" works
        assert_eq!(serde_json::to_string(&ScalarType::UByte).unwrap(), r#""ubyte""#);
        assert_eq!(serde_json::to_string(&ScalarType::Double).unwrap(), r#""double""#);
    }

    // ── ScalarType: Display ──────────────────────────────────────────

    #[test]
    fn test_scalar_type_display_all() {
        let expected = [
            "boolean", "byte", "ubyte", "short", "ushort",
            "int", "uint", "long", "ulong", "float", "double", "string",
        ];
        for (st, exp) in ScalarType::ALL.iter().zip(expected.iter()) {
            assert_eq!(st.to_string(), *exp);
        }
    }

    // ── ScalarType: wire_size ────────────────────────────────────────

    #[test]
    fn test_wire_size() {
        assert_eq!(ScalarType::Boolean.wire_size(), 1);
        assert_eq!(ScalarType::Byte.wire_size(), 1);
        assert_eq!(ScalarType::UByte.wire_size(), 1);
        assert_eq!(ScalarType::Short.wire_size(), 2);
        assert_eq!(ScalarType::UShort.wire_size(), 2);
        assert_eq!(ScalarType::Int.wire_size(), 4);
        assert_eq!(ScalarType::UInt.wire_size(), 4);
        assert_eq!(ScalarType::Float.wire_size(), 4);
        assert_eq!(ScalarType::Long.wire_size(), 8);
        assert_eq!(ScalarType::ULong.wire_size(), 8);
        assert_eq!(ScalarType::Double.wire_size(), 8);
        assert_eq!(ScalarType::String.wire_size(), 0);
    }

    // ── ScalarType: is_f64_exact ─────────────────────────────────────

    #[test]
    fn test_is_f64_exact() {
        // All types <= 32 bits are exact in f64
        assert!(ScalarType::Boolean.is_f64_exact());
        assert!(ScalarType::Byte.is_f64_exact());
        assert!(ScalarType::Int.is_f64_exact());
        assert!(ScalarType::UInt.is_f64_exact());
        assert!(ScalarType::Float.is_f64_exact());
        assert!(ScalarType::Double.is_f64_exact());
        // Long/ULong can exceed 2^53 → not exact
        assert!(!ScalarType::Long.is_f64_exact());
        assert!(!ScalarType::ULong.is_f64_exact());
        // String is not numeric
        assert!(!ScalarType::String.is_f64_exact());
    }

    // ── ScalarType: is_numeric ───────────────────────────────────────

    #[test]
    fn test_scalar_type_is_numeric() {
        for st in ScalarType::ALL {
            let expected = st != ScalarType::String;
            assert_eq!(st.is_numeric(), expected, "is_numeric for {:?}", st);
        }
    }

    // ── ScalarType: ALL constant ─────────────────────────────────────

    #[test]
    fn test_scalar_type_all_count() {
        assert_eq!(ScalarType::ALL.len(), 12);
    }

    #[test]
    fn test_scalar_type_all_unique() {
        use std::collections::HashSet;
        let set: HashSet<ScalarType> = ScalarType::ALL.iter().copied().collect();
        assert_eq!(set.len(), 12);
    }

    // ── ScalarType: Copy / Hash ──────────────────────────────────────

    #[test]
    fn test_scalar_type_copy() {
        let a = ScalarType::Double;
        let b = a; // Copy, not move
        assert_eq!(a, b);
    }

    #[test]
    fn test_scalar_type_hash() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        for st in ScalarType::ALL {
            map.insert(st, st.wire_size());
        }
        assert_eq!(map.len(), 12);
        assert_eq!(map[&ScalarType::Double], 8);
    }
}