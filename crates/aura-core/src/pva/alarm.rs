//! EPICS alarm types — embedded in every Normative Type.
//!
//! Every PVAccess Normative Type carries an `alarm_t` sub-structure
//! describing the current alarm state of the PV. AURA preserves this
//! information with every archived sample.
//!
//! Reference: EPICS PVAccess Normative Types Specification, Section 4.1

use serde::{Deserialize, Serialize};

/// EPICS alarm state — severity + status + optional message.
///
/// This struct mirrors the PVAccess `alarm_t` structure exactly.
/// It is embedded in every Normative Type (NTScalar, NTEnum, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Alarm {
    /// Current alarm severity level.
    pub severity: AlarmSeverity,
    /// Source/cause of the alarm.
    pub status: AlarmStatus,
    /// Human-readable alarm message (may be empty).
    #[serde(default)]
    pub message: String,
}

impl Alarm {
    /// Create an alarm with all fields.
    pub fn new(severity: AlarmSeverity, status: AlarmStatus, message: impl Into<String>) -> Self {
        Self {
            severity,
            status,
            message: message.into(),
        }
    }

    /// Whether this alarm is in an active (non-None) state.
    pub fn is_active(&self) -> bool {
        self.severity != AlarmSeverity::None
    }

    /// Whether the alarm severity is INVALID (data quality issue).
    pub fn is_invalid(&self) -> bool {
        self.severity == AlarmSeverity::Invalid
    }
}

impl Default for Alarm {
    fn default() -> Self {
        Self {
            severity: AlarmSeverity::None,
            status: AlarmStatus::None,
            message: String::new(),
        }
    }
}

impl std::fmt::Display for Alarm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "{}/{}", self.severity, self.status)
        } else {
            write!(f, "{}/{}: {}", self.severity, self.status, self.message)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// ALARM SEVERITY
// ═══════════════════════════════════════════════════════════════════════

/// Alarm severity levels as defined by EPICS.
///
/// Ordered by increasing severity: None < Minor < Major < Invalid.
/// `Undefined` is used for unknown/unmapped values.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
#[repr(i16)]
pub enum AlarmSeverity {
    /// No alarm — value is within normal operating range.
    #[default]
    None = 0,
    /// Minor alarm — value is approaching a limit (WARNING).
    Minor = 1,
    /// Major alarm — value has exceeded a critical limit (ALARM).
    Major = 2,
    /// Invalid — data quality is compromised (sensor failure, comm error).
    Invalid = 3,
    /// Undefined — unknown severity (unmapped integer value).
    Undefined = 4,
}

impl AlarmSeverity {
    /// All defined severity levels in order of increasing severity.
    pub const ALL: [Self; 5] = [
        Self::None,
        Self::Minor,
        Self::Major,
        Self::Invalid,
        Self::Undefined,
    ];
}

impl From<i16> for AlarmSeverity {
    fn from(v: i16) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Minor,
            2 => Self::Major,
            3 => Self::Invalid,
            _ => Self::Undefined,
        }
    }
}

impl From<AlarmSeverity> for i16 {
    fn from(v: AlarmSeverity) -> Self {
        v as i16
    }
}

impl std::fmt::Display for AlarmSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::None => "NO_ALARM",
            Self::Minor => "MINOR",
            Self::Major => "MAJOR",
            Self::Invalid => "INVALID",
            Self::Undefined => "UNDEFINED",
        };
        write!(f, "{}", s)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// ALARM STATUS
// ═══════════════════════════════════════════════════════════════════════

/// Alarm status codes — identifies the source/cause of an alarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(i16)]
pub enum AlarmStatus {
    /// No alarm condition.
    #[default]
    None = 0,
    /// Alarm raised by the device/hardware.
    Device = 1,
    /// Alarm raised by the device driver.
    Driver = 2,
    /// Alarm raised by the record processing logic.
    Record = 3,
    /// Alarm raised by the database layer.
    Db = 4,
    /// Alarm raised by a configuration error.
    Conf = 5,
    /// Undefined — unknown status (unmapped integer value).
    Undefined = 6,
    /// Alarm raised by a client (operator intervention).
    Client = 7,
}

impl AlarmStatus {
    /// All defined status codes.
    pub const ALL: [Self; 8] = [
        Self::None,
        Self::Device,
        Self::Driver,
        Self::Record,
        Self::Db,
        Self::Conf,
        Self::Undefined,
        Self::Client,
    ];
}

impl From<i16> for AlarmStatus {
    fn from(v: i16) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Device,
            2 => Self::Driver,
            3 => Self::Record,
            4 => Self::Db,
            5 => Self::Conf,
            7 => Self::Client,
            _ => Self::Undefined,
        }
    }
}

impl From<AlarmStatus> for i16 {
    fn from(v: AlarmStatus) -> Self {
        v as i16
    }
}

impl std::fmt::Display for AlarmStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::None => "NO_STATUS",
            Self::Device => "DEVICE",
            Self::Driver => "DRIVER",
            Self::Record => "RECORD",
            Self::Db => "DB",
            Self::Conf => "CONF",
            Self::Undefined => "UNDEFINED",
            Self::Client => "CLIENT",
        };
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Alarm struct ─────────────────────────────────────────────────

    #[test]
    fn test_alarm_default() {
        let a = Alarm::default();
        assert_eq!(a.severity, AlarmSeverity::None);
        assert_eq!(a.status, AlarmStatus::None);
        assert_eq!(a.message, "");
        assert!(!a.is_active());
        assert!(!a.is_invalid());
    }

    #[test]
    fn test_alarm_new() {
        let a = Alarm::new(AlarmSeverity::Major, AlarmStatus::Device, "overtemp");
        assert_eq!(a.severity, AlarmSeverity::Major);
        assert_eq!(a.status, AlarmStatus::Device);
        assert_eq!(a.message, "overtemp");
        assert!(a.is_active());
        assert!(!a.is_invalid());
    }

    #[test]
    fn test_alarm_invalid() {
        let a = Alarm::new(AlarmSeverity::Invalid, AlarmStatus::Driver, "comm lost");
        assert!(a.is_active());
        assert!(a.is_invalid());
    }

    #[test]
    fn test_alarm_display_without_message() {
        let a = Alarm::default();
        assert_eq!(a.to_string(), "NO_ALARM/NO_STATUS");
    }

    #[test]
    fn test_alarm_display_with_message() {
        let a = Alarm::new(AlarmSeverity::Minor, AlarmStatus::Record, "high");
        assert_eq!(a.to_string(), "MINOR/RECORD: high");
    }

    #[test]
    fn test_alarm_clone_eq() {
        let a = Alarm::new(AlarmSeverity::Major, AlarmStatus::Db, "x");
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_alarm_ne() {
        let a = Alarm::new(AlarmSeverity::Major, AlarmStatus::Db, "x");
        let b = Alarm::new(AlarmSeverity::Minor, AlarmStatus::Db, "x");
        assert_ne!(a, b);
    }

    #[test]
    fn test_alarm_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Alarm::default());
        set.insert(Alarm::default());
        assert_eq!(set.len(), 1);
        set.insert(Alarm::new(AlarmSeverity::Major, AlarmStatus::Device, "x"));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_alarm_serialization_roundtrip() {
        let a = Alarm::new(AlarmSeverity::Minor, AlarmStatus::Client, "test");
        let json = serde_json::to_string(&a).unwrap();
        let b: Alarm = serde_json::from_str(&json).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_alarm_deserialize_missing_message() {
        let json = r#"{"severity":"None","status":"None"}"#;
        let a: Alarm = serde_json::from_str(json).unwrap();
        assert_eq!(a.message, "");
    }

    // ── AlarmSeverity ────────────────────────────────────────────────

    #[test]
    fn test_severity_from_i16_all_values() {
        assert_eq!(AlarmSeverity::from(0), AlarmSeverity::None);
        assert_eq!(AlarmSeverity::from(1), AlarmSeverity::Minor);
        assert_eq!(AlarmSeverity::from(2), AlarmSeverity::Major);
        assert_eq!(AlarmSeverity::from(3), AlarmSeverity::Invalid);
        assert_eq!(AlarmSeverity::from(4), AlarmSeverity::Undefined);
    }

    #[test]
    fn test_severity_from_i16_unknown_maps_to_undefined() {
        for v in [5, 10, -1, 99, i16::MAX, i16::MIN] {
            assert_eq!(
                AlarmSeverity::from(v),
                AlarmSeverity::Undefined,
                "value {} should map to Undefined",
                v
            );
        }
    }

    #[test]
    fn test_severity_to_i16_roundtrip() {
        for sev in AlarmSeverity::ALL {
            let raw = i16::from(sev);
            let back = AlarmSeverity::from(raw);
            assert_eq!(sev, back, "roundtrip failed for {:?} (raw={})", sev, raw);
        }
    }

    #[test]
    fn test_severity_default() {
        assert_eq!(AlarmSeverity::default(), AlarmSeverity::None);
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(AlarmSeverity::None.to_string(), "NO_ALARM");
        assert_eq!(AlarmSeverity::Minor.to_string(), "MINOR");
        assert_eq!(AlarmSeverity::Major.to_string(), "MAJOR");
        assert_eq!(AlarmSeverity::Invalid.to_string(), "INVALID");
        assert_eq!(AlarmSeverity::Undefined.to_string(), "UNDEFINED");
    }

    #[test]
    fn test_severity_ordering() {
        assert!(AlarmSeverity::None < AlarmSeverity::Minor);
        assert!(AlarmSeverity::Minor < AlarmSeverity::Major);
        assert!(AlarmSeverity::Major < AlarmSeverity::Invalid);
        assert!(AlarmSeverity::Invalid < AlarmSeverity::Undefined);
    }

    #[test]
    fn test_severity_all_count() {
        assert_eq!(AlarmSeverity::ALL.len(), 5);
    }

    #[test]
    fn test_severity_copy() {
        let a = AlarmSeverity::Major;
        let b = a; // Copy, not move
        assert_eq!(a, b);
    }

    #[test]
    fn test_severity_hash() {
        use std::collections::HashSet;
        let set: HashSet<AlarmSeverity> = AlarmSeverity::ALL.iter().copied().collect();
        assert_eq!(set.len(), 5);
    }

    #[test]
    fn test_severity_serialization_roundtrip() {
        for sev in AlarmSeverity::ALL {
            let json = serde_json::to_string(&sev).unwrap();
            let back: AlarmSeverity = serde_json::from_str(&json).unwrap();
            assert_eq!(sev, back);
        }
    }

    // ── AlarmStatus ──────────────────────────────────────────────────

    #[test]
    fn test_status_from_i16_all_values() {
        assert_eq!(AlarmStatus::from(0), AlarmStatus::None);
        assert_eq!(AlarmStatus::from(1), AlarmStatus::Device);
        assert_eq!(AlarmStatus::from(2), AlarmStatus::Driver);
        assert_eq!(AlarmStatus::from(3), AlarmStatus::Record);
        assert_eq!(AlarmStatus::from(4), AlarmStatus::Db);
        assert_eq!(AlarmStatus::from(5), AlarmStatus::Conf);
        assert_eq!(AlarmStatus::from(6), AlarmStatus::Undefined);
        assert_eq!(AlarmStatus::from(7), AlarmStatus::Client);
    }

    #[test]
    fn test_status_from_i16_unknown_maps_to_undefined() {
        for v in [8, 10, -1, 99, i16::MAX, i16::MIN] {
            assert_eq!(
                AlarmStatus::from(v),
                AlarmStatus::Undefined,
                "value {} should map to Undefined",
                v
            );
        }
    }

    #[test]
    fn test_status_to_i16_roundtrip() {
        for st in AlarmStatus::ALL {
            let raw = i16::from(st);
            let back = AlarmStatus::from(raw);
            assert_eq!(st, back, "roundtrip failed for {:?} (raw={})", st, raw);
        }
    }

    #[test]
    fn test_status_default() {
        assert_eq!(AlarmStatus::default(), AlarmStatus::None);
    }

    #[test]
    fn test_status_display() {
        assert_eq!(AlarmStatus::None.to_string(), "NO_STATUS");
        assert_eq!(AlarmStatus::Device.to_string(), "DEVICE");
        assert_eq!(AlarmStatus::Driver.to_string(), "DRIVER");
        assert_eq!(AlarmStatus::Record.to_string(), "RECORD");
        assert_eq!(AlarmStatus::Db.to_string(), "DB");
        assert_eq!(AlarmStatus::Conf.to_string(), "CONF");
        assert_eq!(AlarmStatus::Undefined.to_string(), "UNDEFINED");
        assert_eq!(AlarmStatus::Client.to_string(), "CLIENT");
    }

    #[test]
    fn test_status_all_count() {
        assert_eq!(AlarmStatus::ALL.len(), 8);
    }

    #[test]
    fn test_status_copy() {
        let a = AlarmStatus::Client;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_status_hash() {
        use std::collections::HashSet;
        let set: HashSet<AlarmStatus> = AlarmStatus::ALL.iter().copied().collect();
        assert_eq!(set.len(), 8);
    }

    #[test]
    fn test_status_serialization_roundtrip() {
        for st in AlarmStatus::ALL {
            let json = serde_json::to_string(&st).unwrap();
            let back: AlarmStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(st, back);
        }
    }

    // ── Debug trait ──────────────────────────────────────────────────

    #[test]
    fn test_debug_output() {
        let a = Alarm::new(AlarmSeverity::Major, AlarmStatus::Device, "hot");
        let debug = format!("{:?}", a);
        assert!(debug.contains("Major"));
        assert!(debug.contains("Device"));
        assert!(debug.contains("hot"));
    }
}
