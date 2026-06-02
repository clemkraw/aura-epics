//! Single TCP connection to one IOC.
//!
//! Manages the PVA handshake, channel multiplexing, introspection
//! registry, and echo keep-alive for one TCP socket.
//!
//! ## Lifecycle
//!
//! ```text
//! connect(addr) -> handshake() -> create_channel(pv) -> subscribe/get
//! ```

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::codec::field_desc::{IntrospectionRegistry};
use crate::codec::header::ByteOrder;

/// Unique ID generator for client channel/request IDs.
static NEXT_ID: AtomicI32 = AtomicI32::new(1);

/// Allocate a unique client-side ID (channel or request).
pub fn next_client_id() -> i32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// State of a single channel within a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelState {
    /// CREATE_CHANNEL sent, waiting for response.
    Creating,
    /// Channel created, server assigned an ID.
    Active { server_channel_id: i32 },
    /// DESTROY_CHANNEL sent.
    Destroying,
    /// Channel destroyed or error.
    Closed,
}

impl ChannelState {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }
    pub fn server_id(&self) -> Option<i32> {
        match self {
            Self::Active { server_channel_id } => Some(*server_channel_id),
            _ => None,
        }
    }
}

impl fmt::Display for ChannelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Creating => write!(f, "CREATING"),
            Self::Active { server_channel_id } => write!(f, "ACTIVE(sid={server_channel_id})"),
            Self::Destroying => write!(f, "DESTROYING"),
            Self::Closed => write!(f, "CLOSED"),
        }
    }
}

/// Tracked state for a TCP connection to one IOC.
pub struct ConnectionState {
    /// Remote IOC address.
    pub addr: SocketAddr,
    /// Negotiated byte order.
    pub byte_order: ByteOrder,
    /// Server's receive buffer size (from CONNECTION_VALIDATION).
    pub server_buffer_size: i32,
    /// Introspection registry (FieldDesc cache).
    pub registry: IntrospectionRegistry,
    /// Active channels: client_channel_id -> state.
    pub channels: HashMap<i32, ChannelInfo>,
    /// Whether the handshake is complete.
    pub handshake_complete: bool,
    /// Total messages sent.
    pub messages_sent: u64,
    /// Total messages received.
    pub messages_received: u64,
}

/// Info about a single channel on this connection.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub client_channel_id: i32,
    pub pv_name: std::sync::Arc<str>,
    pub state: ChannelState,
}

impl ConnectionState {
    pub fn new(addr: SocketAddr, registry_size: usize) -> Self {
        Self {
            addr,
            byte_order: ByteOrder::LittleEndian,
            server_buffer_size: 0,
            registry: IntrospectionRegistry::new(registry_size),
            channels: HashMap::new(),
            handshake_complete: false,
            messages_sent: 0,
            messages_received: 0,
        }
    }

    /// Register a new channel (before sending CREATE_CHANNEL).
    pub fn add_channel(&mut self, pv_name: impl Into<std::sync::Arc<str>>) -> i32 {
        let cid = next_client_id();
        self.channels.insert(
            cid,
            ChannelInfo {
                client_channel_id: cid,
                pv_name: pv_name.into(),
                state: ChannelState::Creating,
            },
        );
        cid
    }

    /// Mark a channel as active (after receiving CREATE_CHANNEL response).
    pub fn activate_channel(&mut self, client_id: i32, server_id: i32) -> bool {
        if let Some(info) = self.channels.get_mut(&client_id) {
            info.state = ChannelState::Active {
                server_channel_id: server_id,
            };
            true
        } else {
            false
        }
    }

    /// Mark a channel as closed.
    pub fn close_channel(&mut self, client_id: i32) {
        if let Some(info) = self.channels.get_mut(&client_id) {
            info.state = ChannelState::Closed;
        }
    }

    /// Find channel info by PV name.
    pub fn find_channel_by_name(&self, pv_name: &str) -> Option<&ChannelInfo> {
        self.channels.values().find(|c| &*c.pv_name == pv_name)
    }

    /// Number of active channels.
    pub fn active_channel_count(&self) -> usize {
        self.channels
            .values()
            .filter(|c| c.state.is_active())
            .count()
    }

    /// Complete the handshake.
    pub fn complete_handshake(&mut self, byte_order: ByteOrder, server_buffer_size: i32) {
        self.byte_order = byte_order;
        self.server_buffer_size = server_buffer_size;
        self.handshake_complete = true;
    }
}

impl fmt::Debug for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionState")
            .field("addr", &self.addr)
            .field("byte_order", &self.byte_order)
            .field("handshake", &self.handshake_complete)
            .field("channels", &self.channels.len())
            .field("active", &self.active_channel_count())
            .field("sent", &self.messages_sent)
            .field("recv", &self.messages_received)
            .finish()
    }
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Conn[{} {} {} channels ({} active) tx={} rx={}]",
            self.addr,
            self.byte_order,
            self.channels.len(),
            self.active_channel_count(),
            self.messages_sent,
            self.messages_received
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::sync::Arc;

    fn addr() -> SocketAddr {
        SocketAddr::new(Ipv4Addr::new(10, 0, 1, 100).into(), 5075)
    }

    #[test]
    fn test_next_id_unique() {
        let a = next_client_id();
        let b = next_client_id();
        assert_ne!(a, b);
    }
    #[test]
    fn test_next_id_monotonic() {
        let a = next_client_id();
        let b = next_client_id();
        assert!(b > a);
    }

    #[test]
    fn test_cs_creating() {
        assert!(!ChannelState::Creating.is_active());
        assert!(ChannelState::Creating.server_id().is_none());
    }
    #[test]
    fn test_cs_active() {
        let s = ChannelState::Active {
            server_channel_id: 42,
        };
        assert!(s.is_active());
        assert_eq!(s.server_id(), Some(42));
    }
    #[test]
    fn test_cs_destroying() {
        assert!(!ChannelState::Destroying.is_active());
    }
    #[test]
    fn test_cs_closed() {
        assert!(!ChannelState::Closed.is_active());
    }
    #[test]
    fn test_cs_display() {
        assert!(
            ChannelState::Active {
                server_channel_id: 42
            }
            .to_string()
            .contains("sid=42")
        );
    }
    #[test]
    fn test_cs_eq() {
        assert_eq!(ChannelState::Creating, ChannelState::Creating);
        assert_ne!(ChannelState::Creating, ChannelState::Closed);
    }

    #[test]
    fn test_conn_new() {
        let c = ConnectionState::new(addr(), 128);
        assert_eq!(c.addr, addr());
        assert_eq!(c.byte_order, ByteOrder::LittleEndian);
        assert!(!c.handshake_complete);
        assert!(c.channels.is_empty());
        assert_eq!(c.messages_sent, 0);
    }

    #[test]
    fn test_conn_add_channel() {
        let mut c = ConnectionState::new(addr(), 128);
        let cid = c.add_channel("PERLE:Gun:Vacuum");
        assert!(cid > 0);
        assert_eq!(c.channels.len(), 1);
        assert_eq!(c.channels[&cid].pv_name, Arc::from("PERLE:Gun:Vacuum"));
        assert_eq!(c.channels[&cid].state, ChannelState::Creating);
    }

    #[test]
    fn test_conn_activate() {
        let mut c = ConnectionState::new(addr(), 128);
        let cid = c.add_channel("PV:A");
        assert!(c.activate_channel(cid, 500));
        assert_eq!(c.channels[&cid].state.server_id(), Some(500));
        assert_eq!(c.active_channel_count(), 1);
    }

    #[test]
    fn test_conn_activate_missing() {
        let mut c = ConnectionState::new(addr(), 128);
        assert!(!c.activate_channel(999, 500));
    }

    #[test]
    fn test_conn_close() {
        let mut c = ConnectionState::new(addr(), 128);
        let cid = c.add_channel("PV:A");
        c.activate_channel(cid, 500);
        c.close_channel(cid);
        assert_eq!(c.channels[&cid].state, ChannelState::Closed);
        assert_eq!(c.active_channel_count(), 0);
    }

    #[test]
    fn test_conn_find_by_name() {
        let mut c = ConnectionState::new(addr(), 128);
        c.add_channel("PV:A");
        c.add_channel("PV:B");
        assert_eq!(
            c.find_channel_by_name("PV:A").unwrap().pv_name,
            Arc::from("PV:A")
        );
        assert!(c.find_channel_by_name("PV:C").is_none());
    }

    #[test]
    fn test_conn_active_count() {
        let mut c = ConnectionState::new(addr(), 128);
        let a = c.add_channel("PV:A");
        let b = c.add_channel("PV:B");
        assert_eq!(c.active_channel_count(), 0);
        c.activate_channel(a, 1);
        assert_eq!(c.active_channel_count(), 1);
        c.activate_channel(b, 2);
        assert_eq!(c.active_channel_count(), 2);
        c.close_channel(a);
        assert_eq!(c.active_channel_count(), 1);
    }

    #[test]
    fn test_conn_handshake() {
        let mut c = ConnectionState::new(addr(), 128);
        c.complete_handshake(ByteOrder::BigEndian, 65536);
        assert!(c.handshake_complete);
        assert_eq!(c.byte_order, ByteOrder::BigEndian);
        assert_eq!(c.server_buffer_size, 65536);
    }

    #[test]
    fn test_conn_multiple_channels() {
        let mut c = ConnectionState::new(addr(), 128);
        for i in 0..10 {
            c.add_channel(format!("PV:{i}"));
        }
        assert_eq!(c.channels.len(), 10);
    }

    #[test]
    fn test_conn_display() {
        let s = ConnectionState::new(addr(), 128).to_string();
        assert!(s.contains("Conn["));
        assert!(s.contains("10.0.1.100"));
    }
    #[test]
    fn test_conn_debug() {
        assert!(format!("{:?}", ConnectionState::new(addr(), 128)).contains("ConnectionState"));
    }
}
