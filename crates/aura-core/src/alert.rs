//! Alert severity levels — shared across all AURA crates.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Alert severity level.
///
/// Ordered from least to most severe: Info < Warning < Critical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertLevel {
    Info,
    Warning,
    Critical,
}

impl AlertLevel {
    pub const ALL: [Self; 3] = [Self::Info, Self::Warning, Self::Critical];

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }

    /// Numeric severity (higher = more severe).
    pub const fn severity(&self) -> u8 {
        match self {
            Self::Info => 0,
            Self::Warning => 1,
            Self::Critical => 2,
        }
    }

    pub const fn is_critical(&self) -> bool {
        matches!(self, Self::Critical)
    }
    pub const fn is_warning(&self) -> bool {
        matches!(self, Self::Warning)
    }
    pub const fn is_info(&self) -> bool {
        matches!(self, Self::Info)
    }
}

impl fmt::Display for AlertLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AlertLevel {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "info" => Ok(Self::Info),
            "warning" => Ok(Self::Warning),
            "critical" => Ok(Self::Critical),
            other => Err(format!("unknown alert level: {other:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_str() {
        assert_eq!(AlertLevel::Info.as_str(), "info");
        assert_eq!(AlertLevel::Warning.as_str(), "warning");
        assert_eq!(AlertLevel::Critical.as_str(), "critical");
    }

    #[test]
    fn test_severity_order() {
        assert!(AlertLevel::Info.severity() < AlertLevel::Warning.severity());
        assert!(AlertLevel::Warning.severity() < AlertLevel::Critical.severity());
    }

    #[test]
    fn test_ord() {
        assert!(AlertLevel::Info < AlertLevel::Warning);
        assert!(AlertLevel::Warning < AlertLevel::Critical);
    }

    #[test]
    fn test_display() {
        assert_eq!(AlertLevel::Critical.to_string(), "critical");
    }

    #[test]
    fn test_from_str() {
        assert_eq!("info".parse::<AlertLevel>().unwrap(), AlertLevel::Info);
        assert_eq!(
            "warning".parse::<AlertLevel>().unwrap(),
            AlertLevel::Warning
        );
        assert_eq!(
            "critical".parse::<AlertLevel>().unwrap(),
            AlertLevel::Critical
        );
        assert!("unknown".parse::<AlertLevel>().is_err());
    }

    #[test]
    fn test_serde_roundtrip() {
        for level in AlertLevel::ALL {
            let json = serde_json::to_string(&level).unwrap();
            let back: AlertLevel = serde_json::from_str(&json).unwrap();
            assert_eq!(level, back);
        }
    }

    #[test]
    fn test_serde_lowercase() {
        assert_eq!(
            serde_json::to_string(&AlertLevel::Critical).unwrap(),
            r#""critical""#
        );
    }

    #[test]
    fn test_all() {
        assert_eq!(AlertLevel::ALL.len(), 3);
    }

    #[test]
    fn test_is_methods() {
        assert!(AlertLevel::Info.is_info());
        assert!(AlertLevel::Warning.is_warning());
        assert!(AlertLevel::Critical.is_critical());
        assert!(!AlertLevel::Info.is_critical());
    }

    #[test]
    fn test_copy() {
        let a = AlertLevel::Warning;
        let b = a;
        assert_eq!(a, b);
    }
    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let set: HashSet<AlertLevel> = AlertLevel::ALL.iter().copied().collect();
        assert_eq!(set.len(), 3);
    }
}
