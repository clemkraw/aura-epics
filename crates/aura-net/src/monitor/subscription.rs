//! Single PV monitor subscription state machine.
//!
//! Tracks the lifecycle of a CMD_MONITOR subscription: INIT -> START -> receiving updates -> DESTROY.

use crate::codec::field_desc::FieldDesc;
use crate::types::pva_value::PvaValue;
use std::fmt;

/// Subscription lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    /// INIT sent, waiting for type description.
    Initializing,
    /// Type received, START sent, receiving updates.
    Active,
    /// DESTROY sent or error, terminal.
    Destroyed,
}

impl SubscriptionState {
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }
}

impl fmt::Display for SubscriptionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Initializing => "INIT",
            Self::Active => "ACTIVE",
            Self::Destroyed => "DESTROYED",
        })
    }
}

/// Events emitted by a monitor subscription.
#[derive(Debug, Clone, PartialEq)]
pub enum MonitorEvent {
    /// New value received (full or delta-decoded via generic PvaValue path).
    Value(PvaValue),
    /// Scalar update from fast NTScalar decode.
    ScalarDelta {
        value: f64,
        seconds: i64,
        nanos: i32,
        severity: i32,
        status: i32,
    },
    /// String update from fast NTScalar(String) decode.
    StringDelta {
        value: String,
        seconds: i64,
        nanos: i32,
        severity: i32,
        status: i32,
    },
    /// Array update from fast NTScalarArray decode.
    ArrayDelta {
        values: Vec<f64>,
        seconds: i64,
        nanos: i32,
        severity: i32,
        status: i32,
    },
    /// Server disconnected.
    Disconnect,
    /// Reconnected after disconnect.
    Reconnect,
    /// Protocol error.
    Error(String),
}

impl MonitorEvent {
    pub fn is_value(&self) -> bool {
        matches!(self, Self::Value(_))
    }
    pub fn is_disconnect(&self) -> bool {
        matches!(self, Self::Disconnect)
    }
}

impl fmt::Display for MonitorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(v) => write!(f, "Value({v})"),
            Self::ScalarDelta { value, .. } => write!(f, "ScalarDelta({value})"),
            Self::StringDelta { value, .. } => write!(f, "StringDelta({value})"),
            Self::ArrayDelta { values, .. } => write!(f, "ArrayDelta({} elements)", values.len()),
            Self::Disconnect => write!(f, "Disconnect"),
            Self::Reconnect => write!(f, "Reconnect"),
            Self::Error(e) => write!(f, "Error({e})"),
        }
    }
}

/// Per-PV subscription state, owned by session.rs MonitorEntry.
pub struct MonitorSubscription {
    pub pv_name: std::sync::Arc<str>,
    pub request_id: i32,
    pub state: SubscriptionState,
    pub type_desc: Option<std::sync::Arc<FieldDesc>>,
    pub last_value: Option<PvaValue>,
    pub updates_received: u64,
    /// Channel ID on the server side (for diagnostics).
    channel_id: i32,
}

impl MonitorSubscription {
    pub fn new(pv_name: impl Into<std::sync::Arc<str>>, channel_id: i32, request_id: i32) -> Self {
        Self {
            pv_name: pv_name.into(),
            channel_id,
            request_id,
            state: SubscriptionState::Initializing,
            type_desc: None,
            last_value: None,
            updates_received: 0,
        }
    }

    /// Set type description from INIT response.
    pub fn set_type_desc(&mut self, desc: FieldDesc) {
        self.type_desc = Some(std::sync::Arc::new(desc));
    }

    /// Transition to Active state.
    pub fn activate(&mut self) {
        self.state = SubscriptionState::Active;
    }

    /// Transition to Destroyed state.
    pub fn destroy(&mut self) {
        self.state = SubscriptionState::Destroyed;
    }
}

impl fmt::Debug for MonitorSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MonitorSubscription")
            .field("pv", &self.pv_name)
            .field("cid", &self.channel_id)
            .field("state", &self.state)
            .field("updates", &self.updates_received)
            .finish()
    }
}

impl fmt::Display for MonitorSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Monitor[{} cid={} {} updates={}]",
            self.pv_name, self.channel_id, self.state, self.updates_received
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::pva_value::PvaScalar;
    use aura_core::ScalarType;
    use std::sync::Arc;

    fn scalar_val(v: f64) -> PvaValue {
        PvaValue::Scalar(PvaScalar::Double(v))
    }

    #[test]
    fn state_init() {
        assert!(!SubscriptionState::Initializing.is_active());
    }

    #[test]
    fn state_active() {
        assert!(SubscriptionState::Active.is_active());
    }

    #[test]
    fn state_destroyed() {
        assert!(!SubscriptionState::Destroyed.is_active());
    }

    #[test]
    fn state_display() {
        assert_eq!(SubscriptionState::Initializing.to_string(), "INIT");
        assert_eq!(SubscriptionState::Active.to_string(), "ACTIVE");
        assert_eq!(SubscriptionState::Destroyed.to_string(), "DESTROYED");
    }

    #[test]
    fn state_eq() {
        assert_eq!(SubscriptionState::Active, SubscriptionState::Active);
        assert_ne!(SubscriptionState::Active, SubscriptionState::Destroyed);
    }

    #[test]
    fn state_copy() {
        let a = SubscriptionState::Active;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn event_value() {
        assert!(MonitorEvent::Value(PvaValue::Null).is_value());
        assert!(!MonitorEvent::Value(PvaValue::Null).is_disconnect());
    }

    #[test]
    fn event_disconnect() {
        assert!(MonitorEvent::Disconnect.is_disconnect());
        assert!(!MonitorEvent::Disconnect.is_value());
    }

    #[test]
    fn event_scalar_delta() {
        let e = MonitorEvent::ScalarDelta {
            value: 3.96,
            seconds: 0,
            nanos: 0,
            severity: 0,
            status: 0,
        };
        assert!(!e.is_value());
        assert!(e.to_string().contains("3.96"));
    }

    #[test]
    fn event_string_delta() {
        let e = MonitorEvent::StringDelta {
            value: "ON".into(),
            seconds: 0,
            nanos: 0,
            severity: 0,
            status: 0,
        };
        assert!(e.to_string().contains("ON"));
    }

    #[test]
    fn event_array_delta() {
        let e = MonitorEvent::ArrayDelta {
            values: vec![1.0, 2.0],
            seconds: 0,
            nanos: 0,
            severity: 0,
            status: 0,
        };
        assert!(e.to_string().contains("2 elements"));
    }

    #[test]
    fn event_display_all() {
        assert!(MonitorEvent::Disconnect.to_string() == "Disconnect");
        assert!(MonitorEvent::Reconnect.to_string() == "Reconnect");
        assert!(
            MonitorEvent::Error("fail".into())
                .to_string()
                .contains("fail")
        );
    }

    #[test]
    fn event_clone() {
        let a = MonitorEvent::Disconnect;
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn sub_new() {
        let s = MonitorSubscription::new("PERLE:Gun:Vacuum", 10, 1);
        assert_eq!(s.pv_name, Arc::from("PERLE:Gun:Vacuum"));
        assert_eq!(s.request_id, 1);
        assert_eq!(s.state, SubscriptionState::Initializing);
        assert!(s.type_desc.is_none());
        assert!(s.last_value.is_none());
        assert_eq!(s.updates_received, 0);
    }

    #[test]
    fn sub_activate() {
        let mut s = MonitorSubscription::new("PV", 1, 1);
        s.activate();
        assert!(s.state.is_active());
    }

    #[test]
    fn sub_destroy() {
        let mut s = MonitorSubscription::new("PV", 1, 1);
        s.destroy();
        assert_eq!(s.state, SubscriptionState::Destroyed);
    }

    #[test]
    fn sub_set_type_desc() {
        let mut s = MonitorSubscription::new("PV", 1, 1);
        s.set_type_desc(FieldDesc::scalar(ScalarType::Double));
        assert!(s.type_desc.is_some());
    }

    #[test]
    fn direct_field_access() {
        let mut s = MonitorSubscription::new("PV", 1, 1);
        s.last_value = Some(scalar_val(42.0));
        s.updates_received = 100;
        assert_eq!(s.last_value.as_ref().unwrap().as_f64(), Some(42.0));
        assert_eq!(s.updates_received, 100);
    }

    #[test]
    fn sub_display() {
        let s = MonitorSubscription::new("PV:A", 10, 1).to_string();
        assert!(s.contains("PV:A"));
        assert!(s.contains("cid=10"));
        assert!(s.contains("INIT"));
    }

    #[test]
    fn sub_debug() {
        assert!(
            format!("{:?}", MonitorSubscription::new("PV", 1, 1)).contains("MonitorSubscription")
        );
    }
}
