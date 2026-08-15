//! Display, control, and value alarm metadata sub-structures.
//!
//! These are optional fields embedded in NTScalar, NTScalarArray,
//! and NTMatrix. They describe how to present, constrain, and alarm
//! on the PV's value.
//!
//! - [`Display`] — units, precision, display range (LOPR/HOPR)
//! - [`Control`] — safe drive limits (DRVL/DRVH)
//! - [`ValueAlarm`] — alarm thresholds (LOLO/LOW/HIGH/HIHI)
//!
//! Reference: EPICS PVAccess Normative Types Specification, Section 4.

use serde::{Deserialize, Serialize};
use std::fmt;

use super::alarm::AlarmSeverity;

/// Display metadata — how to present the value to a human.
///
/// Maps to the PVAccess `display_t` structure.
/// Used by Grafana dashboards to autoconfigure axis labels,
/// units, and decimal precision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Display {
    /// Lower display limit (LOPR). Grafana Y-axis minimum.
    #[serde(default)]
    pub limit_low: f64,
    /// Upper display limit (HOPR). Grafana Y-axis maximum.
    #[serde(default)]
    pub limit_high: f64,
    /// Human-readable description (DESC field).
    #[serde(default)]
    pub description: String,
    /// Engineering units (EGU): "K", "A", "mbar", "mm/s".
    #[serde(default)]
    pub units: String,
    /// Number of decimal places for display (PREC).
    #[serde(default)]
    pub precision: i32,
    /// Numeric display format hint.
    #[serde(default)]
    pub form: DisplayForm,
}

impl Display {
    pub fn new(units: impl Into<String>, precision: i32) -> Self {
        Self {
            units: units.into(),
            precision,
            ..Self::default()
        }
    }

    /// Whether display limits define a valid range (low < high).
    pub fn has_valid_range(&self) -> bool {
        self.limit_low < self.limit_high
    }

    /// The display range span. Returns 0.0 if limits are invalid.
    pub fn range(&self) -> f64 {
        if self.has_valid_range() {
            self.limit_high - self.limit_low
        } else {
            0.0
        }
    }
}

impl Default for Display {
    fn default() -> Self {
        Self {
            limit_low: 0.0,
            limit_high: 0.0,
            description: String::new(),
            units: String::new(),
            precision: 0,
            form: DisplayForm::Default,
        }
    }
}

impl fmt::Display for Display {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.units.is_empty() {
            write!(f, "prec={}", self.precision)
        } else {
            write!(f, "{} (prec={})", self.units, self.precision)
        }
    }
}

/// Display format hint — how to render the numeric value.
///
/// Maps to the PVAccess `enum_t form` field inside `display_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum DisplayForm {
    #[default]
    Default,
    String,
    Binary,
    Decimal,
    Hex,
    Exponential,
    Engineering,
}

impl DisplayForm {
    /// All 7 display form variants.
    pub const ALL: [Self; 7] = [
        Self::Default,
        Self::String,
        Self::Binary,
        Self::Decimal,
        Self::Hex,
        Self::Exponential,
        Self::Engineering,
    ];
}

impl fmt::Display for DisplayForm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Default => "default",
            Self::String => "string",
            Self::Binary => "binary",
            Self::Decimal => "decimal",
            Self::Hex => "hex",
            Self::Exponential => "exponential",
            Self::Engineering => "engineering",
        })
    }
}

/// Control limits — safe operating range for output PVs.
///
/// Maps to the PVAccess `control_t` structure.
/// These are the DRVL/DRVH fields: the IOC will clamp any
/// caput value to [limit_low, limit_high].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Control {
    /// Lower drive limit (DRVL).
    #[serde(default)]
    pub limit_low: f64,
    /// Upper drive limit (DRVH).
    #[serde(default)]
    pub limit_high: f64,
    /// Minimum step size for incremental changes.
    #[serde(default)]
    pub min_step: f64,
}

impl Control {
    pub fn new(limit_low: f64, limit_high: f64) -> Self {
        Self {
            limit_low,
            limit_high,
            min_step: 0.0,
        }
    }

    /// Whether control limits define a valid range (low < high).
    pub fn has_valid_range(&self) -> bool {
        self.limit_low < self.limit_high
    }

    pub fn range(&self) -> f64 {
        if self.has_valid_range() {
            self.limit_high - self.limit_low
        } else {
            0.0
        }
    }

    /// Whether a value is within the control limits.
    pub fn is_in_range(&self, value: f64) -> bool {
        if !self.has_valid_range() {
            return true; // no valid range = no constraint
        }
        value >= self.limit_low && value <= self.limit_high
    }
}

impl Default for Control {
    fn default() -> Self {
        Self {
            limit_low: 0.0,
            limit_high: 0.0,
            min_step: 0.0,
        }
    }
}

impl fmt::Display for Control {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {}]", self.limit_low, self.limit_high)
    }
}

/// Value-based alarm thresholds.
///
/// Maps to the PVAccess `valueAlarm_t` structure.
/// Defines four thresholds (LOLO < LOW < HIGH < HIHI) and the
/// alarm severity to raise when each is crossed.
///
/// ```text
///   LOLO          LOW                HIGH         HIHI
///    |--- Major ---|--- Minor ---| OK |--- Minor ---|--- Major ---|
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueAlarm {
    /// Whether value-based alarms are enabled.
    #[serde(default = "default_true")]
    pub active: bool,
    /// LOLO threshold — below this triggers low alarm.
    #[serde(default)]
    pub low_alarm_limit: f64,
    /// LOW threshold — below this triggers low warning.
    #[serde(default)]
    pub low_warning_limit: f64,
    /// HIGH threshold — above this triggers high warning.
    #[serde(default)]
    pub high_warning_limit: f64,
    /// HIHI threshold — above this triggers high alarm.
    #[serde(default)]
    pub high_alarm_limit: f64,
    /// Severity when value < LOLO.
    #[serde(default)]
    pub low_alarm_severity: AlarmSeverity,
    /// Severity when LOW < value < LOLO.
    #[serde(default)]
    pub low_warning_severity: AlarmSeverity,
    /// Severity when HIGH < value < HIHI.
    #[serde(default)]
    pub high_warning_severity: AlarmSeverity,
    /// Severity when value > HIHI.
    #[serde(default)]
    pub high_alarm_severity: AlarmSeverity,
    /// Hysteresis to prevent alarm oscillation at thresholds (HYST).
    #[serde(default)]
    pub hysteresis: f64,
}

impl ValueAlarm {
    /// Create a symmetric alarm configuration.
    ///
    /// `warning` = LOW/HIGH threshold distance from center.
    /// `alarm` = LOLO/HIHI threshold distance from center.
    pub fn symmetric(center: f64, warning: f64, alarm: f64) -> Self {
        Self {
            active: true,
            low_alarm_limit: center - alarm,
            low_warning_limit: center - warning,
            high_warning_limit: center + warning,
            high_alarm_limit: center + alarm,
            low_alarm_severity: AlarmSeverity::Major,
            low_warning_severity: AlarmSeverity::Minor,
            high_warning_severity: AlarmSeverity::Minor,
            high_alarm_severity: AlarmSeverity::Major,
            hysteresis: 0.0,
        }
    }

    /// Evaluate which alarm severity a value should produce.
    /// Returns `AlarmSeverity::None` if within normal range or inactive.
    pub fn evaluate(&self, value: f64) -> AlarmSeverity {
        if !self.active {
            return AlarmSeverity::None;
        }
        if value <= self.low_alarm_limit {
            self.low_alarm_severity
        } else if value <= self.low_warning_limit {
            self.low_warning_severity
        } else if value >= self.high_alarm_limit {
            self.high_alarm_severity
        } else if value >= self.high_warning_limit {
            self.high_warning_severity
        } else {
            AlarmSeverity::None
        }
    }

    /// Whether the thresholds are ordered correctly:
    /// LOLO ≤ LOW ≤ HIGH ≤ HIHI.
    pub fn is_valid(&self) -> bool {
        self.low_alarm_limit <= self.low_warning_limit
            && self.low_warning_limit <= self.high_warning_limit
            && self.high_warning_limit <= self.high_alarm_limit
    }
}

impl Default for ValueAlarm {
    fn default() -> Self {
        Self {
            active: true,
            low_alarm_limit: 0.0,
            low_warning_limit: 0.0,
            high_warning_limit: 0.0,
            high_alarm_limit: 0.0,
            low_alarm_severity: AlarmSeverity::None,
            low_warning_severity: AlarmSeverity::None,
            high_warning_severity: AlarmSeverity::None,
            high_alarm_severity: AlarmSeverity::None,
            hysteresis: 0.0,
        }
    }
}

impl fmt::Display for ValueAlarm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LOLO={} LOW={} HIGH={} HIHI={}",
            self.low_alarm_limit,
            self.low_warning_limit,
            self.high_warning_limit,
            self.high_alarm_limit
        )
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_default() {
        let d = Display::default();
        assert_eq!(d.limit_low, 0.0);
        assert_eq!(d.limit_high, 0.0);
        assert_eq!(d.description, "");
        assert_eq!(d.units, "");
        assert_eq!(d.precision, 0);
        assert_eq!(d.form, DisplayForm::Default);
        assert!(!d.has_valid_range());
        assert_eq!(d.range(), 0.0);
    }

    #[test]
    fn test_display_new() {
        let d = Display::new("K", 3);
        assert_eq!(d.units, "K");
        assert_eq!(d.precision, 3);
        assert_eq!(d.limit_low, 0.0); // rest defaults
    }

    #[test]
    fn test_display_valid_range() {
        let d = Display {
            limit_low: 0.0,
            limit_high: 100.0,
            ..Default::default()
        };
        assert!(d.has_valid_range());
        assert_eq!(d.range(), 100.0);
    }

    #[test]
    fn test_display_invalid_range_equal() {
        let d = Display {
            limit_low: 5.0,
            limit_high: 5.0,
            ..Default::default()
        };
        assert!(!d.has_valid_range());
        assert_eq!(d.range(), 0.0);
    }

    #[test]
    fn test_display_invalid_range_inverted() {
        let d = Display {
            limit_low: 100.0,
            limit_high: 0.0,
            ..Default::default()
        };
        assert!(!d.has_valid_range());
        assert_eq!(d.range(), 0.0);
    }

    #[test]
    fn test_display_fmt_with_units() {
        let d = Display::new("mbar", 2);
        assert_eq!(d.to_string(), "mbar (prec=2)");
    }

    #[test]
    fn test_display_fmt_without_units() {
        let d = Display::default();
        assert_eq!(d.to_string(), "prec=0");
    }

    #[test]
    fn test_display_clone_eq() {
        let a = Display::new("A", 4);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_display_ne() {
        let a = Display::new("A", 4);
        let b = Display::new("V", 4);
        assert_ne!(a, b);
    }

    #[test]
    fn test_display_serde_roundtrip() {
        let d = Display {
            limit_low: -10.0,
            limit_high: 50.0,
            description: "Cryo temp".into(),
            units: "K".into(),
            precision: 3,
            form: DisplayForm::Exponential,
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: Display = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_display_serde_defaults() {
        // Minimal JSON with all fields defaulted
        let json = "{}";
        let d: Display = serde_json::from_str(json).unwrap();
        assert_eq!(d, Display::default());
    }

    #[test]
    fn test_display_form_default() {
        assert_eq!(DisplayForm::default(), DisplayForm::Default);
    }

    #[test]
    fn test_display_form_all_count() {
        assert_eq!(DisplayForm::ALL.len(), 7);
    }

    #[test]
    fn test_display_form_all_unique() {
        use std::collections::HashSet;
        let set: HashSet<DisplayForm> = DisplayForm::ALL.iter().copied().collect();
        assert_eq!(set.len(), 7);
    }

    #[test]
    fn test_display_form_display_all() {
        let expected = [
            "default",
            "string",
            "binary",
            "decimal",
            "hex",
            "exponential",
            "engineering",
        ];
        for (form, exp) in DisplayForm::ALL.iter().zip(expected.iter()) {
            assert_eq!(form.to_string(), *exp);
        }
    }

    #[test]
    fn test_display_form_serde_roundtrip() {
        for form in DisplayForm::ALL {
            let json = serde_json::to_string(&form).unwrap();
            let back: DisplayForm = serde_json::from_str(&json).unwrap();
            assert_eq!(form, back);
        }
    }

    #[test]
    fn test_display_form_copy() {
        let a = DisplayForm::Hex;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_control_default() {
        let c = Control::default();
        assert_eq!(c.limit_low, 0.0);
        assert_eq!(c.limit_high, 0.0);
        assert_eq!(c.min_step, 0.0);
        assert!(!c.has_valid_range());
    }

    #[test]
    fn test_control_new() {
        let c = Control::new(0.0, 100.0);
        assert!(c.has_valid_range());
        assert_eq!(c.range(), 100.0);
        assert_eq!(c.min_step, 0.0);
    }

    #[test]
    fn test_control_is_in_range() {
        let c = Control::new(0.0, 10.0);
        assert!(c.is_in_range(0.0)); // at lower bound
        assert!(c.is_in_range(5.0)); // middle
        assert!(c.is_in_range(10.0)); // at upper bound
        assert!(!c.is_in_range(-0.1)); // below
        assert!(!c.is_in_range(10.1)); // above
    }

    #[test]
    fn test_control_is_in_range_no_valid_range() {
        let c = Control::default(); // 0.0, 0.0 → not valid
        // With no valid range, everything is "in range" (no constraint)
        assert!(c.is_in_range(999.0));
        assert!(c.is_in_range(-999.0));
    }

    #[test]
    fn test_control_range_invalid() {
        let c = Control::new(100.0, 0.0); // inverted
        assert!(!c.has_valid_range());
        assert_eq!(c.range(), 0.0);
    }

    #[test]
    fn test_control_display() {
        let c = Control::new(-5.0, 50.0);
        assert_eq!(c.to_string(), "[-5, 50]");
    }

    #[test]
    fn test_control_clone_eq() {
        let a = Control::new(0.0, 100.0);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_control_ne() {
        assert_ne!(Control::new(0.0, 10.0), Control::new(0.0, 20.0));
    }

    #[test]
    fn test_control_serde_roundtrip() {
        let c = Control {
            limit_low: -10.0,
            limit_high: 50.0,
            min_step: 0.01,
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: Control = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn test_control_serde_defaults() {
        let c: Control = serde_json::from_str("{}").unwrap();
        assert_eq!(c, Control::default());
    }

    #[test]
    fn test_value_alarm_default() {
        let va = ValueAlarm::default();
        assert!(va.active);
        assert_eq!(va.low_alarm_limit, 0.0);
        assert_eq!(va.high_alarm_limit, 0.0);
        assert_eq!(va.hysteresis, 0.0);
        assert_eq!(va.low_alarm_severity, AlarmSeverity::None);
    }

    #[test]
    fn test_value_alarm_symmetric() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        assert_eq!(va.low_alarm_limit, 5.0); // 10 - 5
        assert_eq!(va.low_warning_limit, 8.0); // 10 - 2
        assert_eq!(va.high_warning_limit, 12.0); // 10 + 2
        assert_eq!(va.high_alarm_limit, 15.0); // 10 + 5
        assert!(va.is_valid());
        assert!(va.active);
        assert_eq!(va.low_alarm_severity, AlarmSeverity::Major);
        assert_eq!(va.low_warning_severity, AlarmSeverity::Minor);
        assert_eq!(va.high_warning_severity, AlarmSeverity::Minor);
        assert_eq!(va.high_alarm_severity, AlarmSeverity::Major);
    }

    #[test]
    fn test_value_alarm_is_valid() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        assert!(va.is_valid()); // 5 ≤ 8 ≤ 12 ≤ 15

        let mut bad = va.clone();
        bad.low_warning_limit = 20.0; // LOW > HIGH → invalid
        assert!(!bad.is_valid());
    }

    #[test]
    fn test_value_alarm_evaluate_normal() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        // Normal range: 8 < value < 12
        assert_eq!(va.evaluate(10.0), AlarmSeverity::None);
        assert_eq!(va.evaluate(9.0), AlarmSeverity::None);
        assert_eq!(va.evaluate(11.0), AlarmSeverity::None);
    }

    #[test]
    fn test_value_alarm_evaluate_low_warning() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        // LOW zone: 5 < value ≤ 8
        assert_eq!(va.evaluate(8.0), AlarmSeverity::Minor);
        assert_eq!(va.evaluate(6.0), AlarmSeverity::Minor);
    }

    #[test]
    fn test_value_alarm_evaluate_low_alarm() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        // LOLO zone: value ≤ 5
        assert_eq!(va.evaluate(5.0), AlarmSeverity::Major);
        assert_eq!(va.evaluate(0.0), AlarmSeverity::Major);
        assert_eq!(va.evaluate(-100.0), AlarmSeverity::Major);
    }

    #[test]
    fn test_value_alarm_evaluate_high_warning() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        // HIGH zone: 12 ≤ value < 15
        assert_eq!(va.evaluate(12.0), AlarmSeverity::Minor);
        assert_eq!(va.evaluate(14.0), AlarmSeverity::Minor);
    }

    #[test]
    fn test_value_alarm_evaluate_high_alarm() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        // HIHI zone: value ≥ 15
        assert_eq!(va.evaluate(15.0), AlarmSeverity::Major);
        assert_eq!(va.evaluate(100.0), AlarmSeverity::Major);
    }

    #[test]
    fn test_value_alarm_evaluate_inactive() {
        let mut va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        va.active = false;
        // Inactive → always None regardless of value
        assert_eq!(va.evaluate(0.0), AlarmSeverity::None);
        assert_eq!(va.evaluate(100.0), AlarmSeverity::None);
    }

    #[test]
    fn test_value_alarm_display() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        assert_eq!(va.to_string(), "LOLO=5 LOW=8 HIGH=12 HIHI=15");
    }

    #[test]
    fn test_value_alarm_clone_eq() {
        let a = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_value_alarm_ne() {
        let a = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        let b = ValueAlarm::symmetric(20.0, 2.0, 5.0);
        assert_ne!(a, b);
    }

    #[test]
    fn test_value_alarm_serde_roundtrip() {
        let va = ValueAlarm::symmetric(10.0, 2.0, 5.0);
        let json = serde_json::to_string(&va).unwrap();
        let back: ValueAlarm = serde_json::from_str(&json).unwrap();
        assert_eq!(va, back);
    }

    #[test]
    fn test_value_alarm_serde_defaults() {
        let va: ValueAlarm = serde_json::from_str("{}").unwrap();
        assert!(va.active); // default_true
        assert_eq!(va.low_alarm_limit, 0.0);
    }

    #[test]
    fn test_value_alarm_serde_active_false() {
        let json = r#"{"active":false}"#;
        let va: ValueAlarm = serde_json::from_str(json).unwrap();
        assert!(!va.active);
    }

    #[test]
    fn test_debug_all_types() {
        assert!(!format!("{:?}", Display::default()).is_empty());
        assert!(!format!("{:?}", DisplayForm::Default).is_empty());
        assert!(!format!("{:?}", Control::default()).is_empty());
        assert!(!format!("{:?}", ValueAlarm::default()).is_empty());
    }
}
