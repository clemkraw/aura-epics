//! Every piece of data flowing through AURA's pipeline is a [`PvUpdate`]:
//!
//! The update wraps a full PVAccess Normative Type.
//! This preserves the complete EPICS semantics (alarm, timestamp, metadata) through the entire pipeline.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

use crate::pva::{Alarm, NormativeType, PvDataType};

/// A single monitor update from a Process Variable.
///
/// This is the atomic unit flowing through AURA's pipeline.
/// It wraps the full PVA Normative Type with routing metadata.
///
/// The PV name is allocated once at registration time and shared via atomic reference counting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PvUpdate {
    /// PV name as published by the IOC (e.g., "CRYO:SECT2:TEMP:READ").
    pub pv_name: Arc<str>,
    /// Numeric PV ID from `pv_lookup`. `None` until resolved by aura-store.
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

    /// Try to extract a single f64 value (for the epsilon filter).
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

    /// Whether this update can be filtered with the epsilon-deadband.
    #[inline]
    pub fn is_scalar_filterable(&self) -> bool {
        self.data_type().is_scalar_filterable()
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

    /// Whether the PV is currently in alarm.
    #[inline]
    pub fn is_in_alarm(&self) -> bool {
        self.data.alarm().is_active()
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
            write!(f, "={}", v)?;
        }
        if self.is_in_alarm() {
            write!(f, " [{}]", self.alarm().severity)?;
        }
        Ok(())
    }
}

/// Reason why a sample was stored (for metrics, debugging, and auditing).
///
/// Every stored sample is tagged with the reason it passed the filter.
/// This is exposed in Prometheus metrics (`aura_samples_stored_total{reason="..."}`) and available via the API for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StoreReason {
    /// Value exceeded the epsilon-deadband threshold.
    EpsilonExceeded,
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
    /// Array/waveform content changed (L2 norm exceeded threshold).
    ArrayChanged,
    /// Table content changed.
    TableChanged,
    /// Image frame received (always stored — no deadband on images).
    ImageFrame,
    /// Custom structure changed (JSON diff).
    CustomChanged,
}

impl StoreReason {
    /// All possible store reasons.
    pub const ALL: [Self; 10] = [
        Self::EpsilonExceeded,
        Self::Heartbeat,
        Self::Initial,
        Self::Disconnected,
        Self::Reconnected,
        Self::AlarmChange,
        Self::ArrayChanged,
        Self::TableChanged,
        Self::ImageFrame,
        Self::CustomChanged,
    ];

    /// Whether this reason indicates a connectivity event (not a value change).
    #[inline]
    pub fn is_connectivity_event(&self) -> bool {
        matches!(self, Self::Disconnected | Self::Reconnected | Self::Initial)
    }

    /// Whether this reason indicates a value-driven store.
    #[inline]
    pub fn is_value_change(&self) -> bool {
        matches!(
            self,
            Self::EpsilonExceeded
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
            Self::EpsilonExceeded => "epsilon",
            Self::Heartbeat => "heartbeat",
            Self::Initial => "initial",
            Self::Disconnected => "disconnected",
            Self::Reconnected => "reconnected",
            Self::AlarmChange => "alarm_change",
            Self::ArrayChanged => "array_changed",
            Self::TableChanged => "table_changed",
            Self::ImageFrame => "image_frame",
            Self::CustomChanged => "custom_changed",
        })
    }
}

/// The outcome of a filter decision for a single update.
///
/// Returned by the epsilon-deadband filter in `aura-ingest`.
/// `Store(reason)` means the sample passes through to Redis/TimescaleDB.
/// `Drop` means the sample is discarded (carries no new information).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDecision {
    /// Store this update in the archive.
    Store(StoreReason),
    /// Drop this update — it carries no new information.
    Drop,
}

impl FilterDecision {
    /// Whether this decision results in storage.
    #[inline]
    pub fn is_stored(&self) -> bool {
        matches!(self, Self::Store(_))
    }

    /// Whether this decision results in dropping.
    #[inline]
    pub fn is_dropped(&self) -> bool {
        matches!(self, Self::Drop)
    }

    /// Extract the store reason, if stored.
    #[inline]
    pub fn reason(&self) -> Option<StoreReason> {
        match self {
            Self::Store(r) => Some(*r),
            Self::Drop => None,
        }
    }
}

impl fmt::Display for FilterDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(r) => write!(f, "STORE({})", r),
            Self::Drop => f.write_str("DROP"),
        }
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
        assert!(u.pv_id.is_none());
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
    fn test_as_f64_scalar_double() {
        assert_eq!(scalar_update(4.217).as_f64(), Some(4.217));
    }

    #[test]
    fn test_as_f64_scalar_int() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTScalar(NTScalar {
                value: ScalarValue::Int(-42),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        assert_eq!(u.as_f64(), Some(-42.0));
    }

    #[test]
    fn test_as_f64_scalar_string_none() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTScalar(NTScalar {
                value: ScalarValue::String("hello".into()),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        assert_eq!(u.as_f64(), None);
    }

    #[test]
    fn test_as_f64_enum() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTEnum(NTEnum {
                value: EnumValue::from_strs(2, &["A", "B", "C"]),
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
        );
        assert_eq!(u.as_f64(), Some(2.0));
    }

    #[test]
    fn test_as_f64_aggregate() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTAggregate(NTAggregate {
                value: 99.9,
                n: 1,
                dispersion: 0.0,
                first: 99.9,
                last: 99.9,
                max: 99.9,
                min: 99.9,
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
        );
        assert_eq!(u.as_f64(), Some(99.9));
    }

    #[test]
    fn test_as_f64_union_scalar() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTUnion(NTUnion {
                value: UnionValue::Scalar(ScalarValue::Double(7.0)),
                descriptor: String::new(),
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
        );
        assert_eq!(u.as_f64(), Some(7.0));
    }

    #[test]
    fn test_as_f64_array_none() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTScalarArray(NTScalarArray {
                value: ArrayValue::DoubleArray(vec![1.0]),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        assert_eq!(u.as_f64(), None);
    }

    #[test]
    fn test_as_f64_table_none() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTTable(NTTable {
                labels: vec![],
                columns: vec![],
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
        );
        assert_eq!(u.as_f64(), None);
    }

    #[test]
    fn test_as_f64_custom_none() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::Custom(CustomStructure {
                data: serde_json::json!({}),
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
        );
        assert_eq!(u.as_f64(), None);
    }

    #[test]
    fn test_data_type_scalar() {
        assert_eq!(scalar_update(0.0).data_type(), PvDataType::Scalar);
    }

    #[test]
    fn test_data_type_string() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTScalar(NTScalar {
                value: ScalarValue::String("x".into()),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        assert_eq!(u.data_type(), PvDataType::String);
    }

    #[test]
    fn test_data_type_array() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTScalarArray(NTScalarArray {
                value: ArrayValue::DoubleArray(vec![]),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        assert_eq!(u.data_type(), PvDataType::Array);
    }

    #[test]
    fn test_data_type_image() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTNDArray(NTNDArray {
                value: ArrayValue::UByteArray(vec![]),
                codec: Codec::default(),
                compressed_size: 0,
                uncompressed_size: 0,
                dimension: vec![],
                unique_id: 0,
                data_timestamp: None,
                alarm: Alarm::default(),
                timestamp: ts(),
                attribute: vec![],
            }),
        );
        assert_eq!(u.data_type(), PvDataType::Image);
    }

    #[test]
    fn test_is_scalar_filterable() {
        assert!(scalar_update(0.0).is_scalar_filterable());

        let arr = PvUpdate::new(
            "PV",
            NormativeType::NTScalarArray(NTScalarArray {
                value: ArrayValue::DoubleArray(vec![]),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        assert!(!arr.is_scalar_filterable());
    }

    #[test]
    fn test_alarm_no_alarm() {
        let u = scalar_update(4.2);
        assert_eq!(u.severity(), 0);
        assert_eq!(u.status(), 0);
        assert!(!u.is_in_alarm());
    }

    #[test]
    fn test_alarm_minor() {
        let u = alarm_update(AlarmSeverity::Minor);
        assert_eq!(u.severity(), 1);
        assert!(u.is_in_alarm());
    }

    #[test]
    fn test_alarm_major() {
        let u = alarm_update(AlarmSeverity::Major);
        assert_eq!(u.severity(), 2);
        assert!(u.is_in_alarm());
    }

    #[test]
    fn test_alarm_invalid() {
        let u = alarm_update(AlarmSeverity::Invalid);
        assert_eq!(u.severity(), 3);
        assert!(u.is_in_alarm());
    }

    #[test]
    fn test_alarm_struct_access() {
        let u = alarm_update(AlarmSeverity::Major);
        let alarm = u.alarm();
        assert_eq!(alarm.severity, AlarmSeverity::Major);
        assert_eq!(alarm.status, AlarmStatus::Device);
        assert_eq!(alarm.message, "test");
    }

    #[test]
    fn test_timestamp() {
        let u = scalar_update(0.0);
        let dt = u.timestamp();
        assert_eq!(dt.timestamp(), 1713520000);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(scalar_update(0.0).type_name(), "NTScalar");

        let u = PvUpdate::new(
            "PV",
            NormativeType::NTEnum(NTEnum {
                value: EnumValue::from_strs(0, &["A"]),
                alarm: Alarm::default(),
                timestamp: ts(),
            }),
        );
        assert_eq!(u.type_name(), "NTEnum");
    }

    #[test]
    fn test_display_scalar() {
        let u = scalar_update(4.217);
        assert_eq!(u.to_string(), "CRYO:TEMP:NTScalar=4.217");
    }

    #[test]
    fn test_display_with_alarm() {
        let u = alarm_update(AlarmSeverity::Major);
        let s = u.to_string();
        assert!(s.contains("MAG:I"));
        assert!(s.contains("[MAJOR]"));
    }

    #[test]
    fn test_display_array_no_value() {
        let u = PvUpdate::new(
            "PV",
            NormativeType::NTScalarArray(NTScalarArray {
                value: ArrayValue::DoubleArray(vec![1.0]),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        let s = u.to_string();
        assert_eq!(s, "PV:NTScalarArray"); // no =value for arrays
    }

    #[test]
    fn test_clone_eq() {
        let a = scalar_update(4.2);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_ne() {
        let a = scalar_update(4.2);
        let b = scalar_update(4.3);
        assert_ne!(a, b);
    }

    #[test]
    fn test_serde_roundtrip() {
        let u = scalar_update(4.217);
        let json = serde_json::to_string(&u).unwrap();
        let back: PvUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(u.pv_name, back.pv_name);
        assert_eq!(u.as_f64(), back.as_f64());
    }

    #[test]
    fn test_serde_skips_none_pv_id() {
        let u = scalar_update(0.0);
        let json = serde_json::to_string(&u).unwrap();
        assert!(!json.contains("pv_id"));
    }

    #[test]
    fn test_serde_includes_pv_id() {
        let u = PvUpdate::with_id(
            "PV",
            7,
            NormativeType::NTScalar(NTScalar {
                value: ScalarValue::Int(0),
                alarm: Alarm::default(),
                timestamp: ts(),
                display: None,
                control: None,
                value_alarm: None,
            }),
        );
        let json = serde_json::to_string(&u).unwrap();
        assert!(json.contains(r#""pv_id":7"#));
    }

    #[test]
    fn test_store_reason_all_count() {
        assert_eq!(StoreReason::ALL.len(), 10);
    }

    #[test]
    fn test_store_reason_all_unique() {
        use std::collections::HashSet;
        let set: HashSet<StoreReason> = StoreReason::ALL.iter().copied().collect();
        assert_eq!(set.len(), 10);
    }

    #[test]
    fn test_store_reason_is_connectivity_event() {
        assert!(StoreReason::Disconnected.is_connectivity_event());
        assert!(StoreReason::Reconnected.is_connectivity_event());
        assert!(StoreReason::Initial.is_connectivity_event());
        assert!(!StoreReason::EpsilonExceeded.is_connectivity_event());
        assert!(!StoreReason::Heartbeat.is_connectivity_event());
        assert!(!StoreReason::AlarmChange.is_connectivity_event());
    }

    #[test]
    fn test_store_reason_is_value_change() {
        assert!(StoreReason::EpsilonExceeded.is_value_change());
        assert!(StoreReason::ArrayChanged.is_value_change());
        assert!(StoreReason::TableChanged.is_value_change());
        assert!(StoreReason::ImageFrame.is_value_change());
        assert!(StoreReason::CustomChanged.is_value_change());
        assert!(!StoreReason::Heartbeat.is_value_change());
        assert!(!StoreReason::Initial.is_value_change());
    }

    #[test]
    fn test_store_reason_display() {
        assert_eq!(StoreReason::EpsilonExceeded.to_string(), "epsilon");
        assert_eq!(StoreReason::Heartbeat.to_string(), "heartbeat");
        assert_eq!(StoreReason::ImageFrame.to_string(), "image_frame");
    }

    #[test]
    fn test_store_reason_serde_roundtrip() {
        for r in StoreReason::ALL {
            let json = serde_json::to_string(&r).unwrap();
            let back: StoreReason = serde_json::from_str(&json).unwrap();
            assert_eq!(r, back);
        }
    }

    #[test]
    fn test_filter_decision_store() {
        let d = FilterDecision::Store(StoreReason::EpsilonExceeded);
        assert!(d.is_stored());
        assert!(!d.is_dropped());
        assert_eq!(d.reason(), Some(StoreReason::EpsilonExceeded));
    }

    #[test]
    fn test_filter_decision_drop() {
        let d = FilterDecision::Drop;
        assert!(!d.is_stored());
        assert!(d.is_dropped());
        assert_eq!(d.reason(), None);
    }

    #[test]
    fn test_filter_decision_display() {
        assert_eq!(
            FilterDecision::Store(StoreReason::Heartbeat).to_string(),
            "STORE(heartbeat)"
        );
        assert_eq!(FilterDecision::Drop.to_string(), "DROP");
    }

    #[test]
    fn test_filter_decision_eq() {
        assert_eq!(
            FilterDecision::Store(StoreReason::Initial),
            FilterDecision::Store(StoreReason::Initial),
        );
        assert_ne!(
            FilterDecision::Store(StoreReason::Initial),
            FilterDecision::Store(StoreReason::Heartbeat),
        );
        assert_ne!(
            FilterDecision::Store(StoreReason::Initial),
            FilterDecision::Drop,
        );
    }

    #[test]
    fn test_debug_all() {
        assert!(!format!("{:?}", scalar_update(0.0)).is_empty());
        assert!(!format!("{:?}", StoreReason::Initial).is_empty());
        assert!(!format!("{:?}", FilterDecision::Drop).is_empty());
    }
}