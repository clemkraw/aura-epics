//! PVA union type — runtime polymorphic value.
//!
//! NTUnion allows a PV to change its value type at runtime.
//! The inner value can be a scalar, an array, or an arbitrary
//! structure (stored as JSON).
//!
//! Reference: EPICS PVAccess Normative Types Specification, Section 5.13

use serde::{Deserialize, Serialize};
use std::fmt;

use super::arrays::ArrayValue;
use super::scalars::{ScalarType, ScalarValue};

/// Value held by an NTUnion — runtime-typed.
///
/// Serialized as `{"type":"Scalar","v":{"type":"Double","v":4.2}}` for Redis transport.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "v")]
pub enum UnionValue {
    /// A single scalar value.
    Scalar(ScalarValue),
    /// A typed array.
    Array(ArrayValue),
    /// An arbitrary structure, stored as raw JSON.
    Structure(serde_json::Value),
}

impl UnionValue {
    /// Try to extract as f64.
    /// Returns `Some` only for numeric `Scalar` variants.
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Scalar(s) => s.as_f64(),
            _ => None,
        }
    }

    /// Try to extract as a string representation.
    pub fn as_string(&self) -> String {
        match self {
            Self::Scalar(s) => s.to_string(),
            Self::Array(a) => a.to_string(),
            Self::Structure(v) => v.to_string(),
        }
    }

    /// The variant tag name.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::Scalar(_) => "Scalar",
            Self::Array(_) => "Array",
            Self::Structure(_) => "Structure",
        }
    }

    #[inline]
    pub fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(_))
    }

    #[inline]
    pub fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    #[inline]
    pub fn is_structure(&self) -> bool {
        matches!(self, Self::Structure(_))
    }

    /// The scalar type tag, if this is a Scalar variant.
    pub fn scalar_type(&self) -> Option<ScalarType> {
        match self {
            Self::Scalar(s) => Some(s.type_tag()),
            _ => None,
        }
    }

    /// The array element type, if this is an Array variant.
    pub fn array_element_type(&self) -> Option<ScalarType> {
        match self {
            Self::Array(a) => Some(a.element_type()),
            _ => None,
        }
    }
}

impl fmt::Display for UnionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(s) => write!(f, "union({})", s),
            Self::Array(a) => write!(f, "union({})", a),
            Self::Structure(v) => write!(f, "union(struct:{}B)", v.to_string().len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Construction helpers ─────────────────────────────────────────

    fn scalar_double() -> UnionValue {
        UnionValue::Scalar(ScalarValue::Double(4.217))
    }

    fn scalar_string() -> UnionValue {
        UnionValue::Scalar(ScalarValue::String("hello".into()))
    }

    fn array_double() -> UnionValue {
        UnionValue::Array(ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]))
    }

    fn structure() -> UnionValue {
        UnionValue::Structure(serde_json::json!({"x": 1, "y": "two"}))
    }

    // ── as_f64 ───────────────────────────────────────────────────────

    #[test]
    fn test_as_f64_scalar_numeric() {
        assert_eq!(scalar_double().as_f64(), Some(4.217));
    }

    #[test]
    fn test_as_f64_scalar_int() {
        let u = UnionValue::Scalar(ScalarValue::Int(42));
        assert_eq!(u.as_f64(), Some(42.0));
    }

    #[test]
    fn test_as_f64_scalar_boolean() {
        let u = UnionValue::Scalar(ScalarValue::Boolean(true));
        assert_eq!(u.as_f64(), Some(1.0));
    }

    #[test]
    fn test_as_f64_scalar_string_returns_none() {
        assert_eq!(scalar_string().as_f64(), None);
    }

    #[test]
    fn test_as_f64_array_returns_none() {
        assert_eq!(array_double().as_f64(), None);
    }

    #[test]
    fn test_as_f64_structure_returns_none() {
        assert_eq!(structure().as_f64(), None);
    }

    // ── as_string ────────────────────────────────────────────────────

    #[test]
    fn test_as_string_scalar() {
        assert_eq!(scalar_double().as_string(), "4.217");
    }

    #[test]
    fn test_as_string_scalar_string() {
        assert_eq!(scalar_string().as_string(), "hello");
    }

    #[test]
    fn test_as_string_array() {
        let s = array_double().as_string();
        assert_eq!(s, "double[3]");
    }

    #[test]
    fn test_as_string_structure() {
        let s = structure().as_string();
        assert!(s.contains("x"));
        assert!(s.contains("two"));
    }

    // ── variant checks ───────────────────────────────────────────────

    #[test]
    fn test_is_scalar() {
        assert!(scalar_double().is_scalar());
        assert!(!array_double().is_scalar());
        assert!(!structure().is_scalar());
    }

    #[test]
    fn test_is_array() {
        assert!(!scalar_double().is_array());
        assert!(array_double().is_array());
        assert!(!structure().is_array());
    }

    #[test]
    fn test_is_structure() {
        assert!(!scalar_double().is_structure());
        assert!(!array_double().is_structure());
        assert!(structure().is_structure());
    }

    // ── variant_name ─────────────────────────────────────────────────

    #[test]
    fn test_variant_name() {
        assert_eq!(scalar_double().variant_name(), "Scalar");
        assert_eq!(array_double().variant_name(), "Array");
        assert_eq!(structure().variant_name(), "Structure");
    }

    // ── type introspection ───────────────────────────────────────────

    #[test]
    fn test_scalar_type() {
        assert_eq!(scalar_double().scalar_type(), Some(ScalarType::Double));
        assert_eq!(scalar_string().scalar_type(), Some(ScalarType::String));
        assert_eq!(array_double().scalar_type(), None);
        assert_eq!(structure().scalar_type(), None);
    }

    #[test]
    fn test_array_element_type() {
        assert_eq!(array_double().array_element_type(), Some(ScalarType::Double));
        assert_eq!(scalar_double().array_element_type(), None);
        assert_eq!(structure().array_element_type(), None);
    }

    #[test]
    fn test_array_element_type_int() {
        let u = UnionValue::Array(ArrayValue::IntArray(vec![1, 2]));
        assert_eq!(u.array_element_type(), Some(ScalarType::Int));
    }

    // ── Display ──────────────────────────────────────────────────────

    #[test]
    fn test_display_scalar() {
        assert_eq!(scalar_double().to_string(), "union(4.217)");
    }

    #[test]
    fn test_display_array() {
        assert_eq!(array_double().to_string(), "union(double[3])");
    }

    #[test]
    fn test_display_structure() {
        let s = structure().to_string();
        assert!(s.starts_with("union(struct:"));
        assert!(s.ends_with("B)"));
    }

    // ── Clone / PartialEq ────────────────────────────────────────────

    #[test]
    fn test_clone_eq() {
        let a = scalar_double();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_ne_different_variants() {
        assert_ne!(scalar_double(), array_double());
        assert_ne!(scalar_double(), structure());
        assert_ne!(array_double(), structure());
    }

    #[test]
    fn test_ne_same_variant_different_value() {
        let a = UnionValue::Scalar(ScalarValue::Double(1.0));
        let b = UnionValue::Scalar(ScalarValue::Double(2.0));
        assert_ne!(a, b);
    }

    // ── Serde ────────────────────────────────────────────────────────

    #[test]
    fn test_serde_roundtrip_scalar() {
        let u = scalar_double();
        let json = serde_json::to_string(&u).unwrap();
        let back: UnionValue = serde_json::from_str(&json).unwrap();
        assert_eq!(u, back);
    }

    #[test]
    fn test_serde_roundtrip_array() {
        let u = array_double();
        let json = serde_json::to_string(&u).unwrap();
        let back: UnionValue = serde_json::from_str(&json).unwrap();
        assert_eq!(u, back);
    }

    #[test]
    fn test_serde_roundtrip_structure() {
        let u = structure();
        let json = serde_json::to_string(&u).unwrap();
        let back: UnionValue = serde_json::from_str(&json).unwrap();
        assert_eq!(u, back);
    }

    #[test]
    fn test_serde_json_format() {
        let u = UnionValue::Scalar(ScalarValue::Int(42));
        let json = serde_json::to_string(&u).unwrap();
        assert!(json.contains(r#""type":"Scalar""#));
        assert!(json.contains(r#""type":"Int""#));
        assert!(json.contains(r#""v":42"#));
    }

    // ── Debug ────────────────────────────────────────────────────────

    #[test]
    fn test_debug() {
        assert!(!format!("{:?}", scalar_double()).is_empty());
        assert!(!format!("{:?}", array_double()).is_empty());
        assert!(!format!("{:?}", structure()).is_empty());
    }
}