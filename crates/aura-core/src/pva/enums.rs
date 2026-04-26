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