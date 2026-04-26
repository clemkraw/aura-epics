//! EPICS PVA enum type — index + string choices.
//!
//! Enums in EPICS are integers with associated string labels.
//! Published by bi, bo, mbbi, mbbo records. The index is the
//! current value; the choices array maps indices to labels.
//!
//! Reference: EPICS PVAccess Normative Types Specification, Section 5.3

use serde::{Deserialize, Serialize};
use std::fmt;

/// Enumerated value: an integer index into a list of string labels.
///
/// Example: a valve state PV with choices `["Open", "Closed", "Fault"]`
/// and index `1` means the valve is currently "Closed".
///
/// Stored in TimescaleDB as the numeric index (f64) in the `samples`
/// table. The choices are stored once in `pv_metadata.enum_choices`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EnumValue {
    /// Current value as an integer index into `choices`.
    pub index: i32,
    /// All possible labels, ordered by index.
    pub choices: Vec<String>,
}

impl EnumValue {
    pub fn new(index: i32, choices: Vec<String>) -> Self {
        Self { index, choices }
    }

    pub fn from_strs(index: i32, choices: &[&str]) -> Self {
        Self {
            index,
            choices: choices.iter().map(|&s| s.to_string()).collect(),
        }
    }

    /// Label for the current index, or `None` if out of range.
    #[inline]
    pub fn current_label(&self) -> Option<&str> {
        if self.index < 0 {
            return None;
        }
        self.choices.get(self.index as usize).map(|s| s.as_str())
    }

    /// Convert to f64 (the index as a float).
    /// This is what gets stored in the `samples` hypertable.
    #[inline]
    pub fn as_f64(&self) -> f64 {
        self.index as f64
    }

    #[inline]
    pub fn choice_count(&self) -> usize {
        self.choices.len()
    }

    /// Whether the current index is valid (within bounds).
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.index >= 0 && (self.index as usize) < self.choices.len()
    }

    /// Look up the label for any index (not just the current one).
    pub fn label_for(&self, index: i32) -> Option<&str> {
        if index < 0 {
            return None;
        }
        self.choices.get(index as usize).map(|s| s.as_str())
    }

    /// Find the index for a given label (reverse lookup).
    /// Returns `None` if the label is not in the choices.
    pub fn index_of(&self, label: &str) -> Option<i32> {
        self.choices.iter()
            .position(|s| s == label)
            .map(|i| i as i32)
    }
}

impl fmt::Display for EnumValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.current_label() {
            Some(label) => write!(f, "{}({})", label, self.index),
            None => write!(f, "<invalid>({})", self.index),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────

    fn valve_enum() -> EnumValue {
        EnumValue::from_strs(1, &["Open", "Closed", "Fault"])
    }

    fn binary_enum() -> EnumValue {
        EnumValue::from_strs(0, &["Off", "On"])
    }

    // ── Construction ─────────────────────────────────────────────────

    #[test]
    fn test_new() {
        let e = EnumValue::new(2, vec!["A".into(), "B".into(), "C".into()]);
        assert_eq!(e.index, 2);
        assert_eq!(e.choices.len(), 3);
    }

    #[test]
    fn test_from_strs() {
        let e = EnumValue::from_strs(0, &["Off", "On"]);
        assert_eq!(e.index, 0);
        assert_eq!(e.choices, vec!["Off", "On"]);
    }

    // ── current_label ────────────────────────────────────────────────

    #[test]
    fn test_current_label_valid() {
        assert_eq!(valve_enum().current_label(), Some("Closed"));
    }

    #[test]
    fn test_current_label_first() {
        assert_eq!(binary_enum().current_label(), Some("Off"));
    }

    #[test]
    fn test_current_label_last() {
        let e = EnumValue::from_strs(2, &["A", "B", "C"]);
        assert_eq!(e.current_label(), Some("C"));
    }

    #[test]
    fn test_current_label_out_of_range() {
        let e = EnumValue::from_strs(99, &["A"]);
        assert_eq!(e.current_label(), None);
    }

    #[test]
    fn test_current_label_negative_index() {
        let e = EnumValue::from_strs(-1, &["A", "B"]);
        assert_eq!(e.current_label(), None);
    }

    #[test]
    fn test_current_label_empty_choices() {
        let e = EnumValue::new(0, vec![]);
        assert_eq!(e.current_label(), None);
    }

    // ── as_f64 ───────────────────────────────────────────────────────

    #[test]
    fn test_as_f64() {
        assert_eq!(valve_enum().as_f64(), 1.0);
        assert_eq!(binary_enum().as_f64(), 0.0);
    }

    #[test]
    fn test_as_f64_negative() {
        let e = EnumValue::from_strs(-1, &["A"]);
        assert_eq!(e.as_f64(), -1.0);
    }

    // ── choice_count ─────────────────────────────────────────────────

    #[test]
    fn test_choice_count() {
        assert_eq!(valve_enum().choice_count(), 3);
        assert_eq!(binary_enum().choice_count(), 2);
        assert_eq!(EnumValue::new(0, vec![]).choice_count(), 0);
    }

    // ── is_valid ─────────────────────────────────────────────────────

    #[test]
    fn test_is_valid() {
        assert!(valve_enum().is_valid());       // index 1, 3 choices
        assert!(binary_enum().is_valid());      // index 0, 2 choices
    }

    #[test]
    fn test_is_valid_out_of_range() {
        let e = EnumValue::from_strs(5, &["A", "B"]);
        assert!(!e.is_valid());
    }

    #[test]
    fn test_is_valid_negative() {
        let e = EnumValue::from_strs(-1, &["A"]);
        assert!(!e.is_valid());
    }

    #[test]
    fn test_is_valid_empty_choices() {
        let e = EnumValue::new(0, vec![]);
        assert!(!e.is_valid());
    }

    // ── label_for ────────────────────────────────────────────────────

    #[test]
    fn test_label_for_valid() {
        let e = valve_enum();
        assert_eq!(e.label_for(0), Some("Open"));
        assert_eq!(e.label_for(1), Some("Closed"));
        assert_eq!(e.label_for(2), Some("Fault"));
    }

    #[test]
    fn test_label_for_out_of_range() {
        assert_eq!(valve_enum().label_for(10), None);
    }

    #[test]
    fn test_label_for_negative() {
        assert_eq!(valve_enum().label_for(-1), None);
    }

    // ── index_of ─────────────────────────────────────────────────────

    #[test]
    fn test_index_of_found() {
        let e = valve_enum();
        assert_eq!(e.index_of("Open"), Some(0));
        assert_eq!(e.index_of("Closed"), Some(1));
        assert_eq!(e.index_of("Fault"), Some(2));
    }

    #[test]
    fn test_index_of_not_found() {
        assert_eq!(valve_enum().index_of("Unknown"), None);
    }

    #[test]
    fn test_index_of_empty() {
        let e = EnumValue::new(0, vec![]);
        assert_eq!(e.index_of("A"), None);
    }

    #[test]
    fn test_index_of_case_sensitive() {
        assert_eq!(valve_enum().index_of("open"), None); // "Open" != "open"
    }

    // ── Display ──────────────────────────────────────────────────────

    #[test]
    fn test_display_valid() {
        assert_eq!(valve_enum().to_string(), "Closed(1)");
    }

    #[test]
    fn test_display_invalid() {
        let e = EnumValue::from_strs(99, &["A"]);
        assert_eq!(e.to_string(), "<invalid>(99)");
    }

    #[test]
    fn test_display_negative() {
        let e = EnumValue::from_strs(-1, &["A"]);
        assert_eq!(e.to_string(), "<invalid>(-1)");
    }

    // ── Clone / PartialEq / Hash ─────────────────────────────────────

    #[test]
    fn test_clone_eq() {
        let a = valve_enum();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_ne_index() {
        let a = EnumValue::from_strs(0, &["A", "B"]);
        let b = EnumValue::from_strs(1, &["A", "B"]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_ne_choices() {
        let a = EnumValue::from_strs(0, &["A", "B"]);
        let b = EnumValue::from_strs(0, &["X", "Y"]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(valve_enum());
        set.insert(valve_enum()); // duplicate
        set.insert(binary_enum());
        assert_eq!(set.len(), 2);
    }

    // ── Serde ────────────────────────────────────────────────────────

    #[test]
    fn test_serde_roundtrip() {
        let e = valve_enum();
        let json = serde_json::to_string(&e).unwrap();
        let back: EnumValue = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn test_serde_json_format() {
        let e = EnumValue::from_strs(1, &["Off", "On"]);
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains(r#""index":1"#));
        assert!(json.contains(r#""choices":["Off","On"]"#));
    }

    #[test]
    fn test_serde_empty_choices() {
        let e = EnumValue::new(0, vec![]);
        let json = serde_json::to_string(&e).unwrap();
        let back: EnumValue = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    // ── Debug ────────────────────────────────────────────────────────

    #[test]
    fn test_debug() {
        let debug = format!("{:?}", valve_enum());
        assert!(debug.contains("index"));
        assert!(debug.contains("Closed"));
    }
}