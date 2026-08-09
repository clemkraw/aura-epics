//! Every piece of data flowing through AURA's pipeline is a [`PvUpdate`].
//!
//! The update wraps a full PVAccess Normative Type, preserving
//! complete EPICS semantics (alarm, timestamp, metadata) through the entire pipeline.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

use crate::pva::{Alarm, NormativeType, PvDataType};

/// A single monitor update from a Process Variable.
///
/// Atomic unit flowing through AURA's pipeline: session -> bus -> thread -> SharedBuffer -> COPY.
/// The PV name is allocated once at registration and shared via Arc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PvUpdate {
    /// PV name as published by the IOC (e.g., "CRYO:SECT2:TEMP:READ").
    pub pv_name: Arc<str>,
    /// Numeric PV ID from `pv_lookup`. `None` until resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pv_id: Option<i32>,
    /// The full Normative Type payload from the PVA monitor.
    pub data: NormativeType,
}

impl PvUpdate {
    /// Create a new update (pv_id unresolved).
    #[inline]
    pub fn new(pv_name: impl Into<Arc<str>>, data: NormativeType) -> Self {
        Self {
            pv_name: pv_name.into(),
            pv_id: None,
            data,
        }
    }

    /// Create a new update with a pre-resolved pv_id.
    #[inline]
    pub fn with_id(pv_name: impl Into<Arc<str>>, pv_id: i32, data: NormativeType) -> Self {
        Self {
            pv_name: pv_name.into(),
            pv_id: Some(pv_id),
            data,
        }
    }

    /// The timestamp from the PVA data.
    #[inline]
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.data.timestamp().to_datetime()
    }

    /// Try to extract a single f64 value.
    /// Works for NTScalar (numeric), NTEnum, NTAggregate, NTUnion (scalar).
    /// Returns `None` for arrays, tables, images, strings.
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        self.data.as_f64()
    }

    /// Determine which storage table this update belongs to.
    #[inline]
    pub fn data_type(&self) -> PvDataType {
        PvDataType::from_nt(&self.data)
    }

    /// The alarm struct from the PVA data.
    #[inline]
    pub fn alarm(&self) -> &Alarm {
        self.data.alarm()
    }

    /// The alarm severity as i16 (for database storage).
    #[inline]
    pub fn severity(&self) -> i16 {
        self.data.alarm().severity as i16
    }

    /// The alarm status as i16 (for database storage).
    #[inline]
    pub fn status(&self) -> i16 {
        self.data.alarm().status as i16
    }

    /// Whether the pv_id has been resolved.
    #[inline]
    pub fn is_resolved(&self) -> bool {
        self.pv_id.is_some()
    }

    /// The Normative Type name (e.g., "NTScalar", "NTEnum").
    #[inline]
    pub fn type_name(&self) -> &'static str {
        self.data.type_name()
    }
}

impl fmt::Display for PvUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.pv_name, self.data.type_name())?;
        if let Some(v) = self.as_f64() {
            write!(f, "={v}")?;
        }
        if self.data.alarm().is_active() {
            write!(f, " [{}]", self.alarm().severity)?;
        }
        Ok(())
    }
}

/// Reason why a sample was stored (for metrics, debugging, and auditing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StoreReason {
    /// Value changed (default for ScalarDelta fast path).
    ValueChanged,
    /// Heartbeat timer expired (periodic liveness proof).
    Heartbeat,
    /// First sample after subscription or reconnection.
    Initial,
    /// PV disconnected from the network (sentinel record).
    Disconnected,
    /// PV reconnected after a disconnection.
    Reconnected,
    /// Alarm severity changed (transition in/out of alarm).
    AlarmChange,
    /// Array/waveform content changed.
    ArrayChanged,
    /// Table content changed.
    TableChanged,
    /// Image frame received (always stored).
    ImageFrame,
    /// Custom structure changed.
    CustomChanged,
    /// PV silent (IOC alive but PV not publishing). Detected by timeout scan.
    Timeout,
}

impl StoreReason {
    pub const ALL: [Self; 11] = [
        Self::ValueChanged,
        Self::Heartbeat,
        Self::Initial,
        Self::Disconnected,
        Self::Reconnected,
        Self::AlarmChange,
        Self::ArrayChanged,
        Self::TableChanged,
        Self::ImageFrame,
        Self::CustomChanged,
        Self::Timeout,
    ];

    /// Whether this reason indicates a connectivity event (not a value change).
    #[inline]
    pub fn is_connectivity_event(&self) -> bool {
        matches!(
            self,
            Self::Disconnected | Self::Reconnected | Self::Initial | Self::Timeout
        )
    }

    /// Whether this reason indicates a value-driven store.
    #[inline]
    pub fn is_value_change(&self) -> bool {
        matches!(
            self,
            Self::ValueChanged
                | Self::ArrayChanged
                | Self::TableChanged
                | Self::ImageFrame
                | Self::CustomChanged
        )
    }
}

impl fmt::Display for StoreReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ValueChanged => "value_changed",
            Self::Heartbeat => "heartbeat",
            Self::Initial => "initial",
            Self::Disconnected => "disconnected",
            Self::Reconnected => "reconnected",
            Self::AlarmChange => "alarm_change",
            Self::ArrayChanged => "array_changed",
            Self::TableChanged => "table_changed",
            Self::ImageFrame => "image_frame",
            Self::CustomChanged => "custom_changed",
            Self::Timeout => "timeout",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pva::*;

    fn ts() -> TimeStamp {
        TimeStamp::new(1713520000, 0)
    }

    fn scalar_update(value: f64) -> PvUpdate {
        PvUpdate::new(
            "CRYO:TEMP",
            NormativeType::NTScalar(NTScalar {
                value: ScalarValue::Double(value),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        )
    }

    fn alarm_update(sev: AlarmSeverity) -> PvUpdate {
        PvUpdate::new(
            "MAG:I",
            NormativeType::NTScalar(NTScalar {
                value: ScalarValue::Double(802.3),
                alarm: Alarm::new(sev, AlarmStatus::Device, "test"),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        )
    }

    #[test]
    fn test_new() {
        let u = scalar_update(4.217);
        assert_eq!(&*u.pv_name, "CRYO:TEMP");
        assert!(!u.is_resolved());
    }

    #[test]
    fn test_with_id() {
        let u = PvUpdate::with_id(
            "PV",
            42,
            NormativeType::NTScalar(NTScalar {
                value: ScalarValue::Int(0),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        assert_eq!(u.pv_id, Some(42));
        assert!(u.is_resolved());
    }

    #[test]
    fn test_as_f64() {
        assert_eq!(scalar_update(4.217).as_f64(), Some(4.217));
    }

    #[test]
    fn test_severity_status() {
        let u = alarm_update(AlarmSeverity::Major);
        assert_eq!(u.severity(), 2);
        assert_eq!(u.status(), AlarmStatus::Device as i16);
    }

    #[test]
    fn test_timestamp() {
        assert_eq!(scalar_update(0.0).timestamp().timestamp(), 1713520000);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(scalar_update(0.0).type_name(), "NTScalar");
    }

    #[test]
    fn test_display() {
        assert_eq!(scalar_update(4.217).to_string(), "CRYO:TEMP:NTScalar=4.217");
        assert!(
            alarm_update(AlarmSeverity::Major)
                .to_string()
                .contains("[MAJOR]")
        );
    }

    #[test]
    fn test_serde_roundtrip() {
        let u = scalar_update(4.217);
        let back: PvUpdate = serde_json::from_str(&serde_json::to_string(&u).unwrap()).unwrap();
        assert_eq!(u.pv_name, back.pv_name);
        assert_eq!(u.as_f64(), back.as_f64());
    }

    #[test]
    fn test_serde_skips_none_pv_id() {
        assert!(
            !serde_json::to_string(&scalar_update(0.0))
                .unwrap()
                .contains("pv_id")
        );
    }

    #[test]
    fn test_store_reason_all() {
        assert_eq!(StoreReason::ALL.len(), 11);
        use std::collections::HashSet;
        let set: HashSet<StoreReason> = StoreReason::ALL.iter().copied().collect();
        assert_eq!(set.len(), 11);
    }

    #[test]
    fn test_store_reason_categories() {
        assert!(StoreReason::Disconnected.is_connectivity_event());
        assert!(!StoreReason::ValueChanged.is_connectivity_event());
        assert!(StoreReason::ValueChanged.is_value_change());
        assert!(!StoreReason::Heartbeat.is_value_change());
    }

    #[test]
    fn test_store_reason_display() {
        assert_eq!(StoreReason::ValueChanged.to_string(), "value_changed");
        assert_eq!(StoreReason::Heartbeat.to_string(), "heartbeat");
    }

    #[test]
    fn test_store_reason_serde() {
        for r in StoreReason::ALL {
            assert_eq!(
                r,
                serde_json::from_str::<StoreReason>(&serde_json::to_string(&r).unwrap()).unwrap()
            );
        }
    }
}
