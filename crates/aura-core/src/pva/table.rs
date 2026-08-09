//! Table column and histogram value types.
//!
//! - [`TableColumn`] — a named, typed column used in NTTable.
//! - [`HistogramValue`] — bin counts for NTHistogram (short/int/long).
//!
//! Reference: EPICS PVAccess Normative Types Specification, Sections 5.7 & 5.11

use serde::{Deserialize, Serialize};
use std::fmt;

use super::arrays::ArrayValue;
use super::scalars::ScalarType;

/// A named, typed column in an NTTable.
///
/// NTTable stores data in columnar format: each column is an `ArrayValue`
/// with a label. All columns in a table must have the same length (number of rows).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableColumn {
    /// Column name / label (e.g., "x", "beam_current", "status").
    pub name: String,
    /// Column data — a typed array of uniform elements.
    pub values: ArrayValue,
}

impl TableColumn {
    pub fn new(name: impl Into<String>, values: ArrayValue) -> Self {
        Self {
            name: name.into(),
            values,
        }
    }

    /// Number of rows in this column.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline]
    pub fn element_type(&self) -> ScalarType {
        self.values.element_type()
    }

    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.values.is_numeric()
    }
}

impl fmt::Display for TableColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.values)
    }
}

/// Histogram bin counts — frequency data for NTHistogram.
///
/// The PVAccess spec allows short[], int[], or long[] for bin counts.
/// AURA normalizes to i64 via [`as_i64_vec`] for uniform processing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "v")]
pub enum HistogramValue {
    Short(Vec<i16>),
    Int(Vec<i32>),
    Long(Vec<i64>),
}

impl HistogramValue {
    /// Number of bins.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Short(v) => v.len(),
            Self::Int(v) => v.len(),
            Self::Long(v) => v.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert all bin counts to i64 for uniform processing.
    pub fn as_i64_vec(&self) -> Vec<i64> {
        match self {
            Self::Short(v) => v.iter().map(|&x| x as i64).collect(),
            Self::Int(v) => v.iter().map(|&x| x as i64).collect(),
            Self::Long(v) => v.clone(),
        }
    }

    /// Convert all bin counts to f64 (for plotting/analysis).
    pub fn as_f64_vec(&self) -> Vec<f64> {
        match self {
            Self::Short(v) => v.iter().map(|&x| x as f64).collect(),
            Self::Int(v) => v.iter().map(|&x| x as f64).collect(),
            Self::Long(v) => v.iter().map(|&x| x as f64).collect(),
        }
    }

    /// Total count across all bins.
    pub fn total(&self) -> i64 {
        match self {
            Self::Short(v) => v.iter().map(|&x| x as i64).sum(),
            Self::Int(v) => v.iter().map(|&x| x as i64).sum(),
            Self::Long(v) => v.iter().sum(),
        }
    }

    /// Maximum bin count.
    pub fn max(&self) -> Option<i64> {
        match self {
            Self::Short(v) => v.iter().max().map(|&x| x as i64),
            Self::Int(v) => v.iter().max().map(|&x| x as i64),
            Self::Long(v) => v.iter().max().copied(),
        }
    }

    /// The integer element type tag.
    pub fn element_type(&self) -> &'static str {
        match self {
            Self::Short(_) => "short",
            Self::Int(_) => "int",
            Self::Long(_) => "long",
        }
    }
}

impl fmt::Display for HistogramValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.element_type(), self.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Table column ─────────────────────────────────────────────────

    #[test]
    fn test_column_new() {
        let col = TableColumn::new("x", ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]));
        assert_eq!(col.name, "x");
        assert_eq!(col.len(), 3);
        assert!(!col.is_empty());
    }

    #[test]
    fn test_column_empty() {
        let col = TableColumn::new("empty", ArrayValue::IntArray(vec![]));
        assert!(col.is_empty());
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_column_element_type() {
        let col = TableColumn::new("temp", ArrayValue::DoubleArray(vec![4.2]));
        assert_eq!(col.element_type(), ScalarType::Double);
        assert!(col.is_numeric());
    }

    #[test]
    fn test_column_string_not_numeric() {
        let col = TableColumn::new("names", ArrayValue::StringArray(vec!["a".into()]));
        assert_eq!(col.element_type(), ScalarType::String);
        assert!(!col.is_numeric());
    }

    #[test]
    fn test_column_display() {
        let col = TableColumn::new("current", ArrayValue::DoubleArray(vec![1.0, 2.0]));
        assert_eq!(col.to_string(), "current:double[2]");
    }

    #[test]
    fn test_column_clone_eq() {
        let a = TableColumn::new("x", ArrayValue::IntArray(vec![1, 2]));
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_column_ne() {
        let a = TableColumn::new("x", ArrayValue::IntArray(vec![1]));
        let b = TableColumn::new("y", ArrayValue::IntArray(vec![1]));
        assert_ne!(a, b); // different name
    }

    #[test]
    fn test_column_ne_values() {
        let a = TableColumn::new("x", ArrayValue::IntArray(vec![1]));
        let b = TableColumn::new("x", ArrayValue::IntArray(vec![2]));
        assert_ne!(a, b); // different values
    }

    #[test]
    fn test_column_serde_roundtrip() {
        let col = TableColumn::new("pressure", ArrayValue::DoubleArray(vec![1.013, 0.5]));
        let json = serde_json::to_string(&col).unwrap();
        let back: TableColumn = serde_json::from_str(&json).unwrap();
        assert_eq!(col, back);
    }

    #[test]
    fn test_column_debug() {
        let col = TableColumn::new("x", ArrayValue::IntArray(vec![1]));
        assert!(!format!("{:?}", col).is_empty());
    }

    // ── Construction & len ───────────────────────────────────────────

    #[test]
    fn test_histogram_short() {
        let h = HistogramValue::Short(vec![10, 20, 30]);
        assert_eq!(h.len(), 3);
        assert!(!h.is_empty());
        assert_eq!(h.element_type(), "short");
    }

    #[test]
    fn test_histogram_int() {
        let h = HistogramValue::Int(vec![100, 200]);
        assert_eq!(h.len(), 2);
        assert_eq!(h.element_type(), "int");
    }

    #[test]
    fn test_histogram_long() {
        let h = HistogramValue::Long(vec![1_000_000_000]);
        assert_eq!(h.len(), 1);
        assert_eq!(h.element_type(), "long");
    }

    #[test]
    fn test_histogram_empty() {
        let h = HistogramValue::Int(vec![]);
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
    }

    // ── as_i64_vec ───────────────────────────────────────────────────

    #[test]
    fn test_as_i64_vec_short() {
        let h = HistogramValue::Short(vec![1, 2, 3]);
        assert_eq!(h.as_i64_vec(), vec![1i64, 2, 3]);
    }

    #[test]
    fn test_as_i64_vec_int() {
        let h = HistogramValue::Int(vec![100, -50]);
        assert_eq!(h.as_i64_vec(), vec![100i64, -50]);
    }

    #[test]
    fn test_as_i64_vec_long() {
        let h = HistogramValue::Long(vec![i64::MAX, i64::MIN]);
        assert_eq!(h.as_i64_vec(), vec![i64::MAX, i64::MIN]);
    }

    #[test]
    fn test_as_i64_vec_empty() {
        let h = HistogramValue::Short(vec![]);
        assert_eq!(h.as_i64_vec(), Vec::<i64>::new());
    }

    // ── as_f64_vec ───────────────────────────────────────────────────

    #[test]
    fn test_as_f64_vec_short() {
        let h = HistogramValue::Short(vec![1, 2]);
        assert_eq!(h.as_f64_vec(), vec![1.0, 2.0]);
    }

    #[test]
    fn test_as_f64_vec_int() {
        let h = HistogramValue::Int(vec![42]);
        assert_eq!(h.as_f64_vec(), vec![42.0]);
    }

    #[test]
    fn test_as_f64_vec_long() {
        let h = HistogramValue::Long(vec![100]);
        assert_eq!(h.as_f64_vec(), vec![100.0]);
    }

    // ── total ────────────────────────────────────────────────────────

    #[test]
    fn test_total() {
        let h = HistogramValue::Int(vec![10, 20, 30]);
        assert_eq!(h.total(), 60);
    }

    #[test]
    fn test_total_empty() {
        let h = HistogramValue::Int(vec![]);
        assert_eq!(h.total(), 0);
    }

    #[test]
    fn test_total_with_negatives() {
        let h = HistogramValue::Short(vec![10, -5, 20]);
        assert_eq!(h.total(), 25);
    }

    // ── max ──────────────────────────────────────────────────────────

    #[test]
    fn test_max() {
        let h = HistogramValue::Int(vec![5, 42, 3]);
        assert_eq!(h.max(), Some(42));
    }

    #[test]
    fn test_max_empty() {
        let h = HistogramValue::Int(vec![]);
        assert_eq!(h.max(), None);
    }

    #[test]
    fn test_max_single() {
        let h = HistogramValue::Long(vec![7]);
        assert_eq!(h.max(), Some(7));
    }

    // ── Display ──────────────────────────────────────────────────────

    #[test]
    fn test_histogram_display_short() {
        let h = HistogramValue::Short(vec![1, 2, 3]);
        assert_eq!(h.to_string(), "short[3]");
    }

    #[test]
    fn test_histogram_display_int() {
        let h = HistogramValue::Int(vec![1, 2]);
        assert_eq!(h.to_string(), "int[2]");
    }

    #[test]
    fn test_histogram_display_long() {
        let h = HistogramValue::Long(vec![]);
        assert_eq!(h.to_string(), "long[0]");
    }

    // ── Clone / PartialEq ────────────────────────────────────────────

    #[test]
    fn test_histogram_clone_eq() {
        let a = HistogramValue::Int(vec![1, 2, 3]);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_histogram_ne_values() {
        let a = HistogramValue::Int(vec![1, 2, 3]);
        let b = HistogramValue::Int(vec![1, 2, 4]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_histogram_ne_types() {
        let a = HistogramValue::Short(vec![1, 2]);
        let b = HistogramValue::Int(vec![1, 2]);
        assert_ne!(a, b);
    }

    // ── Serde ────────────────────────────────────────────────────────

    #[test]
    fn test_histogram_serde_roundtrip_short() {
        let h = HistogramValue::Short(vec![10, 20, 30]);
        let json = serde_json::to_string(&h).unwrap();
        let back: HistogramValue = serde_json::from_str(&json).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn test_histogram_serde_roundtrip_int() {
        let h = HistogramValue::Int(vec![100, 200]);
        let json = serde_json::to_string(&h).unwrap();
        let back: HistogramValue = serde_json::from_str(&json).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn test_histogram_serde_roundtrip_long() {
        let h = HistogramValue::Long(vec![i64::MAX]);
        let json = serde_json::to_string(&h).unwrap();
        let back: HistogramValue = serde_json::from_str(&json).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn test_histogram_serde_json_format() {
        let h = HistogramValue::Int(vec![5, 10]);
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json, r#"{"type":"Int","v":[5,10]}"#);
    }

    #[test]
    fn test_column_serde_json_format() {
        let col = TableColumn::new("x", ArrayValue::IntArray(vec![1]));
        let json = serde_json::to_string(&col).unwrap();
        assert!(json.contains(r#""name":"x""#));
    }

    // ── Debug ────────────────────────────────────────────────────────

    #[test]
    fn test_debug_all() {
        assert!(!format!("{:?}", HistogramValue::Short(vec![])).is_empty());
        assert!(!format!("{:?}", HistogramValue::Int(vec![])).is_empty());
        assert!(!format!("{:?}", HistogramValue::Long(vec![])).is_empty());
        assert!(!format!("{:?}", TableColumn::new("x", ArrayValue::IntArray(vec![]))).is_empty());
    }
}
