//! PV configuration, status, and IOC metadata types.
//!
//! - [`PvConfig`] - mirrors the `pv_config` table in TimescaleDB.
//!   Source of truth for which PVs to archive and with what parameters.
//! - [`PvStatus`] - runtime state of a PV (connected/disconnected/etc.)
//! - [`IocState`] - liveness state for IOCs (TCP connection based)
//! - [`IocInfo`] - metadata about a discovered IOC
//!
//! Configuration is polled every 30 seconds by `aura-discover`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration for a single Process Variable.
///
/// Each row in the `pv_config` table maps to one instance of this struct.
/// Polled by `aura-discover`, used by `aura-ingest` for heartbeat config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PvConfig {
    pub pv_name: String,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]
    pub unit: Option<String>,

    /// Heartbeat interval in seconds. 0.0 = use global default from aura.toml.
    #[serde(default = "default_heartbeat")]
    pub heartbeat_s: f64,

    /// Expected IOC address (from ioc_config). Used for connection routing.
    #[serde(default)]
    pub expected_ioc: Option<String>,

    /// Whether this PV is actively archived. `false` = paused.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,

    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

fn default_heartbeat() -> f64 {
    0.0
}
fn default_enabled() -> bool {
    true
}

impl PvConfig {
    pub fn new(pv_name: impl Into<String>) -> Self {
        Self {
            pv_name: pv_name.into(),
            description: None,
            unit: None,
            heartbeat_s: default_heartbeat(),
            expected_ioc: None,
            enabled: true,
            created_at: None,
            updated_at: None,
        }
    }

    pub fn with_heartbeat(mut self, heartbeat_s: f64) -> Self {
        self.heartbeat_s = heartbeat_s;
        self
    }

    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn with_expected_ioc(mut self, addr: impl Into<String>) -> Self {
        self.expected_ioc = Some(addr.into());
        self
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

impl fmt::Display for PvConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pv_name)?;
        if let Some(u) = &self.unit {
            write!(f, " [{u}]")?;
        }
        if !self.enabled {
            write!(f, " (disabled)")?;
        }
        Ok(())
    }
}

/// Runtime status of a PV in the AURA system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PvStatus {
    Connected,
    Disconnected,
    Unconfigured,
    Disabled,
}

impl PvStatus {
    pub const ALL: [Self; 4] = [
        Self::Connected,
        Self::Disconnected,
        Self::Unconfigured,
        Self::Disabled,
    ];

    #[inline]
    pub fn is_active(&self) -> bool {
        *self == Self::Connected
    }
    #[inline]
    pub fn needs_attention(&self) -> bool {
        matches!(self, Self::Disconnected | Self::Unconfigured)
    }
}

impl fmt::Display for PvStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Connected => "CONNECTED",
            Self::Disconnected => "DISCONNECTED",
            Self::Unconfigured => "UNCONFIGURED",
            Self::Disabled => "DISABLED",
        })
    }
}

/// IOC liveness state (TCP connection based, no UDP beacons).
///
/// ```text
///   ONLINE ──(TCP lost)──► SUSPECT ──(reconnect timeout)──► OFFLINE
///     ▲                       │                                │
///     └──(TCP reconnected)────┴────(TCP reconnected)───────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IocState {
    Online,
    Suspect,
    Offline,
}

impl IocState {
    pub const ALL: [Self; 3] = [Self::Online, Self::Suspect, Self::Offline];
    #[inline]
    pub fn is_reachable(&self) -> bool {
        *self != Self::Offline
    }
    #[inline]
    pub fn is_healthy(&self) -> bool {
        *self == Self::Online
    }
}

impl fmt::Display for IocState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Online => "ONLINE",
            Self::Suspect => "SUSPECT",
            Self::Offline => "OFFLINE",
        })
    }
}

/// Metadata about a discovered IOC.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IocInfo {
    pub address: String,
    pub state: IocState,
    pub pv_count: u32,
    pub last_seen: Option<DateTime<Utc>>,
    pub first_seen: Option<DateTime<Utc>>,
}

impl IocInfo {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            state: IocState::Online,
            pv_count: 0,
            last_seen: None,
            first_seen: None,
        }
    }
}

impl fmt::Display for IocInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}, {} PVs)",
            self.address, self.state, self.pv_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pvconfig_new() {
        let pv = PvConfig::new("CRYO:TEMP");
        assert_eq!(pv.pv_name, "CRYO:TEMP");
        assert_eq!(pv.heartbeat_s, 0.0);
        assert!(pv.enabled);
    }

    #[test]
    fn test_pvconfig_builders() {
        let pv = PvConfig::new("MAG:I")
            .with_heartbeat(30.0)
            .with_unit("A")
            .with_description("Dipole")
            .with_expected_ioc("10.0.1.5:5075");
        assert_eq!(pv.heartbeat_s, 30.0);
        assert_eq!(pv.unit.as_deref(), Some("A"));
        assert_eq!(pv.expected_ioc.as_deref(), Some("10.0.1.5:5075"));
    }

    #[test]
    fn test_pvconfig_display() {
        assert_eq!(PvConfig::new("PV").to_string(), "PV");
        assert_eq!(PvConfig::new("PV").with_unit("K").to_string(), "PV [K]");
        assert!(
            PvConfig::new("PV")
                .with_enabled(false)
                .to_string()
                .contains("(disabled)")
        );
    }

    #[test]
    fn test_pvconfig_serde() {
        let pv = PvConfig::new("CRYO:TEMP").with_unit("K");
        assert_eq!(
            pv,
            serde_json::from_str::<PvConfig>(&serde_json::to_string(&pv).unwrap()).unwrap()
        );
    }

    #[test]
    fn test_pvconfig_serde_minimal() {
        let pv: PvConfig = serde_json::from_str(r#"{"pv_name":"TEST:PV"}"#).unwrap();
        assert_eq!(pv.heartbeat_s, 0.0);
        assert!(pv.enabled);
    }

    #[test]
    fn test_pvstatus() {
        assert!(PvStatus::Connected.is_active());
        assert!(PvStatus::Disconnected.needs_attention());
        assert!(!PvStatus::Disabled.needs_attention());
    }

    #[test]
    fn test_iocstate() {
        assert!(IocState::Online.is_healthy());
        assert!(IocState::Suspect.is_reachable());
        assert!(!IocState::Offline.is_reachable());
        assert!(IocState::Online < IocState::Offline);
    }

    #[test]
    fn test_iocinfo() {
        let mut ioc = IocInfo::new("10.0.0.1:5075");
        ioc.pv_count = 42;
        assert!(ioc.to_string().contains("42 PVs"));
    }
}
