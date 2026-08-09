//! PVAccess array types: typed arrays for each of the 12 scalar types.
//!
//! These represent the `value` field of NTScalarArray, the pixel data
//! of NTNDArray, and array columns in NTTable.
//!
//! Reference: EPICS PVAccess Normative Types Specification, Section 3.

use serde::{Deserialize, Serialize};
use std::fmt;

use super::scalars::ScalarType;

/// A PVAccess typed array value.
///
/// Serialized as `{"type":"DoubleArray","v":[1.0,2.0]}` for Redis transport.
/// Each variant wraps a `Vec<T>` of the corresponding scalar type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "v")]
pub enum ArrayValue {
    BooleanArray(Vec<bool>),
    ByteArray(Vec<i8>),
    UByteArray(Vec<u8>),
    ShortArray(Vec<i16>),
    UShortArray(Vec<u16>),
    IntArray(Vec<i32>),
    UIntArray(Vec<u32>),
    LongArray(Vec<i64>),
    ULongArray(Vec<u64>),
    FloatArray(Vec<f32>),
    DoubleArray(Vec<f64>),
    StringArray(Vec<String>),
}

impl ArrayValue {
    /// Convert all elements to f64. Returns `None` for `StringArray`.
    ///
    /// **Precision note:** `LongArray` and `ULongArray` values exceeding
    /// 2^53 will lose precision. See [`super::scalars::ScalarValue::as_f64`].
    #[inline]
    pub fn as_f64_vec(&self) -> Option<Vec<f64>> {
        match self {
            Self::BooleanArray(v) => Some(v.iter().map(|&b| if b { 1.0 } else { 0.0 }).collect()),
            Self::ByteArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::UByteArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::ShortArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::UShortArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::IntArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::UIntArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::LongArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::ULongArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::FloatArray(v) => Some(v.iter().map(|&x| x as f64).collect()),
            Self::DoubleArray(v) => Some(v.clone()),
            Self::StringArray(_) => None,
        }
    }

    /// Number of elements in the array.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::BooleanArray(v) => v.len(),
            Self::ByteArray(v) => v.len(),
            Self::UByteArray(v) => v.len(),
            Self::ShortArray(v) => v.len(),
            Self::UShortArray(v) => v.len(),
            Self::IntArray(v) => v.len(),
            Self::UIntArray(v) => v.len(),
            Self::LongArray(v) => v.len(),
            Self::ULongArray(v) => v.len(),
            Self::FloatArray(v) => v.len(),
            Self::DoubleArray(v) => v.len(),
            Self::StringArray(v) => v.len(),
        }
    }

    /// Whether the array contains zero elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The scalar type of the array elements.
    #[inline]
    pub fn element_type(&self) -> ScalarType {
        match self {
            Self::BooleanArray(_) => ScalarType::Boolean,
            Self::ByteArray(_) => ScalarType::Byte,
            Self::UByteArray(_) => ScalarType::UByte,
            Self::ShortArray(_) => ScalarType::Short,
            Self::UShortArray(_) => ScalarType::UShort,
            Self::IntArray(_) => ScalarType::Int,
            Self::UIntArray(_) => ScalarType::UInt,
            Self::LongArray(_) => ScalarType::Long,
            Self::ULongArray(_) => ScalarType::ULong,
            Self::FloatArray(_) => ScalarType::Float,
            Self::DoubleArray(_) => ScalarType::Double,
            Self::StringArray(_) => ScalarType::String,
        }
    }

    /// Whether the array elements are numeric (not String).
    #[inline]
    pub fn is_numeric(&self) -> bool {
        !matches!(self, Self::StringArray(_))
    }

    /// Total size in bytes on the wire (element_size × len).
    /// Returns 0 for StringArray (variable-length encoding).
    pub fn wire_size(&self) -> usize {
        self.element_type().wire_size() * self.len()
    }

    /// Compute the L2 norm (Euclidean distance) between two arrays.
    /// Used by the epsilon filter to compare waveforms.
    /// Returns `None` if types differ, either is a StringArray, or lengths differ.
    pub fn l2_distance(&self, other: &Self) -> Option<f64> {
        let a = self.as_f64_vec()?;
        let b = other.as_f64_vec()?;
        if a.len() != b.len() {
            return None;
        }
        let sum_sq: f64 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum();
        Some(sum_sq.sqrt())
    }
}

impl fmt::Display for ArrayValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.element_type(), self.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test data ────────────────────────────────────────────────────

    /// One representative of each array variant with known values.
    fn all_variants() -> Vec<(ArrayValue, ScalarType, usize, bool)> {
        // (value, element_type, len, is_numeric)
        vec![
            (
                ArrayValue::BooleanArray(vec![true, false]),
                ScalarType::Boolean,
                2,
                true,
            ),
            (
                ArrayValue::ByteArray(vec![-1, 0, 1]),
                ScalarType::Byte,
                3,
                true,
            ),
            (
                ArrayValue::UByteArray(vec![0, 128, 255]),
                ScalarType::UByte,
                3,
                true,
            ),
            (
                ArrayValue::ShortArray(vec![-100, 0, 100]),
                ScalarType::Short,
                3,
                true,
            ),
            (
                ArrayValue::UShortArray(vec![0, 1000, 65535]),
                ScalarType::UShort,
                3,
                true,
            ),
            (
                ArrayValue::IntArray(vec![-1, 0, 1]),
                ScalarType::Int,
                3,
                true,
            ),
            (
                ArrayValue::UIntArray(vec![0, 42, u32::MAX]),
                ScalarType::UInt,
                3,
                true,
            ),
            (
                ArrayValue::LongArray(vec![i64::MIN, 0, i64::MAX]),
                ScalarType::Long,
                3,
                true,
            ),
            (
                ArrayValue::ULongArray(vec![0, 1, u64::MAX]),
                ScalarType::ULong,
                3,
                true,
            ),
            (
                ArrayValue::FloatArray(vec![1.0, 2.5, -3.0]),
                ScalarType::Float,
                3,
                true,
            ),
            (
                ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]),
                ScalarType::Double,
                3,
                true,
            ),
            (
                ArrayValue::StringArray(vec!["a".into(), "b".into()]),
                ScalarType::String,
                2,
                false,
            ),
        ]
    }

    // ── element_type (exhaustive) ────────────────────────────────────

    #[test]
    fn test_element_type_all_variants() {
        for (arr, expected_type, _, _) in all_variants() {
            assert_eq!(
                arr.element_type(),
                expected_type,
                "element_type mismatch for {:?}",
                arr.element_type()
            );
        }
    }

    // ── len / is_empty ───────────────────────────────────────────────

    #[test]
    fn test_len_all_variants() {
        for (arr, _, expected_len, _) in all_variants() {
            assert_eq!(arr.len(), expected_len, "len mismatch for {}", arr);
        }
    }

    #[test]
    fn test_is_empty_non_empty() {
        for (arr, _, _, _) in all_variants() {
            assert!(!arr.is_empty(), "{} should not be empty", arr);
        }
    }

    #[test]
    fn test_is_empty_each_variant() {
        let empties: Vec<ArrayValue> = vec![
            ArrayValue::BooleanArray(vec![]),
            ArrayValue::ByteArray(vec![]),
            ArrayValue::UByteArray(vec![]),
            ArrayValue::ShortArray(vec![]),
            ArrayValue::UShortArray(vec![]),
            ArrayValue::IntArray(vec![]),
            ArrayValue::UIntArray(vec![]),
            ArrayValue::LongArray(vec![]),
            ArrayValue::ULongArray(vec![]),
            ArrayValue::FloatArray(vec![]),
            ArrayValue::DoubleArray(vec![]),
            ArrayValue::StringArray(vec![]),
        ];
        for arr in &empties {
            assert!(arr.is_empty(), "{} should be empty", arr);
            assert_eq!(arr.len(), 0);
        }
    }

    // ── is_numeric ───────────────────────────────────────────────────

    #[test]
    fn test_is_numeric_all_variants() {
        for (arr, _, _, expected_numeric) in all_variants() {
            assert_eq!(
                arr.is_numeric(),
                expected_numeric,
                "is_numeric mismatch for {}",
                arr
            );
        }
    }

    // ── as_f64_vec ───────────────────────────────────────────────────

    #[test]
    fn test_as_f64_vec_numeric_variants() {
        // All numeric variants must return Some
        for (arr, _, _, is_numeric) in all_variants() {
            if is_numeric {
                assert!(
                    arr.as_f64_vec().is_some(),
                    "as_f64_vec should be Some for {}",
                    arr
                );
                assert_eq!(arr.as_f64_vec().unwrap().len(), arr.len());
            }
        }
    }

    #[test]
    fn test_as_f64_vec_string_returns_none() {
        let arr = ArrayValue::StringArray(vec!["42".into()]);
        assert!(arr.as_f64_vec().is_none());
    }

    #[test]
    fn test_as_f64_vec_values_correct() {
        assert_eq!(
            ArrayValue::IntArray(vec![1, 2, 3]).as_f64_vec(),
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            ArrayValue::BooleanArray(vec![true, false, true]).as_f64_vec(),
            Some(vec![1.0, 0.0, 1.0])
        );
        assert_eq!(
            ArrayValue::DoubleArray(vec![3.14]).as_f64_vec(),
            Some(vec![3.14])
        );
    }

    #[test]
    fn test_as_f64_vec_empty_numeric() {
        let arr = ArrayValue::DoubleArray(vec![]);
        assert_eq!(arr.as_f64_vec(), Some(vec![]));
    }

    #[test]
    fn test_as_f64_vec_empty_string() {
        let arr = ArrayValue::StringArray(vec![]);
        assert!(arr.as_f64_vec().is_none());
    }

    #[test]
    fn test_as_f64_vec_boundary_values() {
        let arr = ArrayValue::LongArray(vec![i64::MIN, 0, i64::MAX]);
        let result = arr.as_f64_vec().unwrap();
        assert_eq!(result[0], i64::MIN as f64);
        assert_eq!(result[1], 0.0);
        assert_eq!(result[2], i64::MAX as f64);
    }

    #[test]
    fn test_as_f64_vec_float_special() {
        let arr = ArrayValue::FloatArray(vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY]);
        let result = arr.as_f64_vec().unwrap();
        assert!(result[0].is_nan());
        assert_eq!(result[1], f64::INFINITY);
        assert_eq!(result[2], f64::NEG_INFINITY);
    }

    // ── wire_size ────────────────────────────────────────────────────

    #[test]
    fn test_wire_size() {
        assert_eq!(ArrayValue::DoubleArray(vec![1.0, 2.0]).wire_size(), 16); // 2 × 8
        assert_eq!(ArrayValue::UByteArray(vec![0; 1024]).wire_size(), 1024); // 1024 × 1
        assert_eq!(ArrayValue::IntArray(vec![0; 10]).wire_size(), 40); // 10 × 4
        assert_eq!(ArrayValue::ShortArray(vec![0; 5]).wire_size(), 10); // 5 × 2
        assert_eq!(ArrayValue::StringArray(vec!["a".into()]).wire_size(), 0); // variable
    }

    #[test]
    fn test_wire_size_empty() {
        assert_eq!(ArrayValue::DoubleArray(vec![]).wire_size(), 0);
    }

    // ── l2_distance ──────────────────────────────────────────────────

    #[test]
    fn test_l2_distance_identical() {
        let a = ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]);
        let b = ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]);
        assert_eq!(a.l2_distance(&b), Some(0.0));
    }

    #[test]
    fn test_l2_distance_known_value() {
        let a = ArrayValue::DoubleArray(vec![0.0, 0.0]);
        let b = ArrayValue::DoubleArray(vec![3.0, 4.0]);
        let d = a.l2_distance(&b).unwrap();
        assert!((d - 5.0).abs() < 1e-10); // 3-4-5 triangle
    }

    #[test]
    fn test_l2_distance_different_lengths() {
        let a = ArrayValue::DoubleArray(vec![1.0, 2.0]);
        let b = ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]);
        assert!(a.l2_distance(&b).is_none());
    }

    #[test]
    fn test_l2_distance_string_returns_none() {
        let a = ArrayValue::StringArray(vec!["a".into()]);
        let b = ArrayValue::StringArray(vec!["b".into()]);
        assert!(a.l2_distance(&b).is_none());
    }

    #[test]
    fn test_l2_distance_cross_type() {
        let a = ArrayValue::IntArray(vec![0, 0]);
        let b = ArrayValue::DoubleArray(vec![3.0, 4.0]);
        // Both convert to f64 vec, so this works
        let d = a.l2_distance(&b).unwrap();
        assert!((d - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_l2_distance_empty() {
        let a = ArrayValue::DoubleArray(vec![]);
        let b = ArrayValue::DoubleArray(vec![]);
        assert_eq!(a.l2_distance(&b), Some(0.0));
    }

    // ── Display ──────────────────────────────────────────────────────

    #[test]
    fn test_display_all_variants() {
        for (arr, expected_type, expected_len, _) in all_variants() {
            let display = arr.to_string();
            let expected = format!("{}[{}]", expected_type, expected_len);
            assert_eq!(
                display,
                expected,
                "Display mismatch for {:?}",
                arr.element_type()
            );
        }
    }

    #[test]
    fn test_display_empty() {
        assert_eq!(ArrayValue::DoubleArray(vec![]).to_string(), "double[0]");
    }

    // ── Clone / PartialEq / Debug ────────────────────────────────────

    #[test]
    fn test_clone_all_variants() {
        for (arr, _, _, _) in all_variants() {
            let cloned = arr.clone();
            assert_eq!(arr, cloned);
        }
    }

    #[test]
    fn test_partial_eq_different_values() {
        let a = ArrayValue::IntArray(vec![1, 2, 3]);
        let b = ArrayValue::IntArray(vec![1, 2, 4]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_partial_eq_different_types() {
        let a = ArrayValue::IntArray(vec![1, 2, 3]);
        let b = ArrayValue::UIntArray(vec![1, 2, 3]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_debug_not_empty() {
        for (arr, _, _, _) in all_variants() {
            assert!(!format!("{:?}", arr).is_empty());
        }
    }

    // ── Serde ────────────────────────────────────────────────────────

    #[test]
    fn test_serde_roundtrip_all_variants() {
        for (arr, _, _, _) in all_variants() {
            let json = serde_json::to_string(&arr).unwrap();
            let back: ArrayValue = serde_json::from_str(&json).unwrap();
            assert_eq!(arr, back, "serde roundtrip failed for {}", arr);
        }
    }

    #[test]
    fn test_serde_json_format_contract() {
        let arr = ArrayValue::DoubleArray(vec![1.0, 2.0]);
        let json = serde_json::to_string(&arr).unwrap();
        assert_eq!(json, r#"{"type":"DoubleArray","v":[1.0,2.0]}"#);
    }

    #[test]
    fn test_serde_empty_array() {
        let arr = ArrayValue::IntArray(vec![]);
        let json = serde_json::to_string(&arr).unwrap();
        let back: ArrayValue = serde_json::from_str(&json).unwrap();
        assert_eq!(arr, back);
        assert_eq!(json, r#"{"type":"IntArray","v":[]}"#);
    }

    // ── Large array (performance sanity) ─────────────────────────────

    #[test]
    fn test_large_array() {
        let n = 100_000;
        let arr = ArrayValue::DoubleArray((0..n).map(|i| i as f64).collect());
        assert_eq!(arr.len(), n);
        assert_eq!(arr.wire_size(), n * 8);
        let f64s = arr.as_f64_vec().unwrap();
        assert_eq!(f64s.len(), n);
        assert_eq!(f64s[0], 0.0);
        assert_eq!(f64s[n - 1], (n - 1) as f64);
    }
}
