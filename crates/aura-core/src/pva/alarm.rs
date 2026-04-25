//! EPICS alarm types — embedded in every Normative Type.

use serde::{Deserialize, Serialize};

/// EPICS alarm state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Alarm {
    pub severity: AlarmSeverity,
    pub status: AlarmStatus,
    #[serde(default)]
    pub message: String,
}

impl Default for Alarm {
    fn default() -> Self {
        Self { severity: AlarmSeverity::None, status: AlarmStatus::None, message: String::new() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i16)]
pub enum AlarmSeverity {
    None = 0,
    Minor = 1,
    Major = 2,
    Invalid = 3,
    Undefined = 4,
}

impl From<i16> for AlarmSeverity {
    fn from(v: i16) -> Self {
        match v { 0 => Self::None, 1 => Self::Minor, 2 => Self::Major, 3 => Self::Invalid, _ => Self::Undefined }
    }
}

impl From<AlarmSeverity> for i16 {
    fn from(v: AlarmSeverity) -> Self { v as i16 }
}

impl Default for AlarmSeverity {
    fn default() -> Self { Self::None }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i16)]
pub enum AlarmStatus {
    None = 0,
    Device = 1,
    Driver = 2,
    Record = 3,
    Db = 4,
    Conf = 5,
    Undefined = 6,
    Client = 7,
}

impl From<i16> for AlarmStatus {
    fn from(v: i16) -> Self {
        match v { 0 => Self::None, 1 => Self::Device, 2 => Self::Driver, 3 => Self::Record,
            4 => Self::Db, 5 => Self::Conf, 7 => Self::Client, _ => Self::Undefined }
    }
}

impl From<AlarmStatus> for i16 {
    fn from(v: AlarmStatus) -> Self { v as i16 }
}

impl Default for AlarmStatus {
    fn default() -> Self { Self::None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_conversion() {
        assert_eq!(AlarmSeverity::from(0), AlarmSeverity::None);
        assert_eq!(AlarmSeverity::from(2), AlarmSeverity::Major);
        assert_eq!(AlarmSeverity::from(99), AlarmSeverity::Undefined);
        assert_eq!(i16::from(AlarmSeverity::Major), 2);
    }

    #[test]
    fn test_default_alarm() {
        let a = Alarm::default();
        assert_eq!(a.severity, AlarmSeverity::None);
        assert_eq!(a.message, "");
    }
}