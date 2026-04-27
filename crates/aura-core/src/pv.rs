//! PV configuration, status, and IOC metadata types.
//!
//! - [`PvConfig`] — mirrors the `pv_config` table in TimescaleDB.
//!   Source of truth for which PVs to archive and with what parameters.
//! - [`PvStatus`] — runtime state of a PV (connected/disconnected/etc.)
//! - [`IocState`] — liveness state machine for discovered IOCs
//! - [`IocInfo`] — metadata about a discovered IOC
//!
//! Configuration is polled every 30 seconds by `aura-discover`.
//! Changes via the REST API take effect within one poll cycle.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration for a single Process Variable.
///
/// Each row in the `pv_config` table maps to one instance of this struct.
/// Modified via the REST API (`POST/PUT/DELETE /api/v1/pvs`), polled by
/// `aura-discover`, and used by `aura-ingest` to configure the filter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PvConfig {
    /// PV name as it appears on the EPICS network.
    pub pv_name: String,

    /// Human-readable description (e.g., "Cryostat sector 2 temperature").
    #[serde(default)]
    pub description: Option<String>,

    /// Engineering unit (e.g., "K", "A", "mbar").
    #[serde(default)]
    pub unit: Option<String>,

    /// Epsilon-deadband threshold. `None` = auto-calibrate from noise.
    /// Sample stored only if `|value - last_stored| > epsilon`.
    #[serde(default)]
    pub epsilon: Option<f64>,

    /// Heartbeat interval in seconds. Forces a sample even when stable.
    #[serde(default = "default_heartbeat")]
    pub heartbeat_s: f64,

    /// Expected IOC GUID. Alerts if PV is served by a different IOC.
    #[serde(default)]
    pub expected_ioc: Option<String>,

    /// Ingest shard ID assigned by `aura-discover`.
    #[serde(default)]
    pub shard_id: Option<i32>,

    /// Whether this PV is actively archived. `false` = paused.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Row creation timestamp.
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,

    /// Last modification timestamp (used for incremental polling).
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

fn default_heartbeat() -> f64 { 60.0 }
fn default_enabled() -> bool { true }

impl PvConfig {
    /// Create a minimal PV config with all defaults.
    pub fn new(pv_name: impl Into<String>) -> Self {
        Self {
            pv_name: pv_name.into(),
            description: None,
            unit: None,
            epsilon: None,
            heartbeat_s: default_heartbeat(),
            expected_ioc: None,
            shard_id: None,
            enabled: true,
            created_at: None,
            updated_at: None,
        }
    }

    /// Builder: set epsilon.
    pub fn with_epsilon(mut self, epsilon: f64) -> Self {
        self.epsilon = Some(epsilon);
        self
    }

    /// Builder: set heartbeat interval.
    pub fn with_heartbeat(mut self, heartbeat_s: f64) -> Self {
        self.heartbeat_s = heartbeat_s;
        self
    }

    /// Builder: set engineering unit.
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// Builder: set description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Builder: set expected IOC GUID.
    pub fn with_expected_ioc(mut self, guid: impl Into<String>) -> Self {
        self.expected_ioc = Some(guid.into());
        self
    }

    /// Builder: set shard ID.
    pub fn with_shard(mut self, shard_id: i32) -> Self {
        self.shard_id = Some(shard_id);
        self
    }

    /// Builder: set enabled state.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Whether epsilon should be auto-calibrated from signal noise.
    #[inline]
    pub fn is_auto_epsilon(&self) -> bool {
        self.epsilon.is_none()
    }

    /// Effective epsilon: manual value or 0.0 (pre-calibration default).
    #[inline]
    pub fn effective_epsilon(&self) -> f64 {
        self.epsilon.unwrap_or(0.0)
    }

    /// Whether this PV has been assigned to an ingest shard.
    #[inline]
    pub fn is_assigned(&self) -> bool {
        self.shard_id.is_some()
    }

    /// Whether this PV has an expected IOC constraint.
    #[inline]
    pub fn has_expected_ioc(&self) -> bool {
        self.expected_ioc.is_some()
    }
}

impl fmt::Display for PvConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pv_name)?;
        if let Some(u) = &self.unit {
            write!(f, " [{}]", u)?;
        }
        if let Some(e) = self.epsilon {
            write!(f, " ε={}", e)?;
        } else {
            write!(f, " ε=auto")?;
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
    /// Configured and currently receiving data from the IOC.
    Connected,
    /// Configured but not currently receiving data.
    Disconnected,
    /// Detected on the network but not in `pv_config`.
    Unconfigured,
    /// Present in `pv_config` but `enabled = false`.
    Disabled,
}

impl PvStatus {
    /// All possible statuses.
    pub const ALL: [Self; 4] = [
        Self::Connected, Self::Disconnected, Self::Unconfigured, Self::Disabled,
    ];

    /// Whether data is actively flowing for this PV.
    #[inline]
    pub fn is_active(&self) -> bool {
        *self == Self::Connected
    }

    /// Whether this PV requires operator attention.
    #[inline]
    pub fn needs_attention(&self) -> bool {
        matches!(self, Self::Disconnected | Self::Unconfigured)
    }
}

impl fmt::Display for PvStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Connected    => "CONNECTED",
            Self::Disconnected => "DISCONNECTED",
            Self::Unconfigured => "UNCONFIGURED",
            Self::Disabled     => "DISABLED",
        })
    }
}

/// IOC liveness state machine.
///
/// ```text
///   ONLINE ──(no beacon for 45s)──► SUSPECT ──(no beacon for 150s)──► OFFLINE
///     ▲                                │                                  │
///     └────────(beacon received)───────┴──────(beacon received)───────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IocState {
    /// Beacons received within the liveness window.
    Online,
    /// No beacon for `liveness_suspect_s` seconds.
    Suspect,
    /// No beacon for `liveness_offline_s` seconds.
    Offline,
}

impl IocState {
    /// All possible states in degradation order.
    pub const ALL: [Self; 3] = [Self::Online, Self::Suspect, Self::Offline];

    /// Whether the IOC is reachable (Online or Suspect).
    #[inline]
    pub fn is_reachable(&self) -> bool {
        *self != Self::Offline
    }

    /// Whether the IOC is fully healthy.
    #[inline]
    pub fn is_healthy(&self) -> bool {
        *self == Self::Online
    }
}

impl fmt::Display for IocState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Online  => "ONLINE",
            Self::Suspect => "SUSPECT",
            Self::Offline => "OFFLINE",
        })
    }
}

/// Metadata about a discovered IOC.
///
/// Populated by `aura-discover` from PVA beacons and stored in
/// the `ioc_registry` table. Returned by `GET /api/v1/iocs`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IocInfo {
    /// IOC's unique GUID (from PVA beacon).
    pub guid: String,
    /// Network address ("IP:port").
    pub address: String,
    /// Current liveness state.
    pub state: IocState,
    /// Number of PVs served by this IOC.
    pub pv_count: u32,
    /// Assigned ingest shard.
    pub shard_id: Option<i32>,
    /// Last beacon timestamp.
    pub last_seen: Option<DateTime<Utc>>,
    /// First beacon timestamp.
    pub first_seen: Option<DateTime<Utc>>,
}

impl IocInfo {
    /// Create a new IOC info entry.
    pub fn new(guid: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            guid: guid.into(),
            address: address.into(),
            state: IocState::Online,
            pv_count: 0,
            shard_id: None,
            last_seen: None,
            first_seen: None,
        }
    }

    /// Whether this IOC has been assigned to an ingest shard.
    #[inline]
    pub fn is_assigned(&self) -> bool {
        self.shard_id.is_some()
    }
}

impl fmt::Display for IocInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} ({}, {} PVs)", self.guid, self.address, self.state, self.pv_count)
    }
}