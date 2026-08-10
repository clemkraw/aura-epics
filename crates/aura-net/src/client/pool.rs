//! Session pool — one TCP session per IOC, multiplexed channels.
//!
//! Each `PvaSession` owns a `ConnectionState` (channel tracking, registry,
//! stats) and a `PvaTcp` (async I/O). The pool manages sessions by address
//! and provides O(1) aggregate counters.
//!
//! ## Responsibilities
//!
//! - `SessionPool`: lifecycle (add, remove, cleanup) + aggregate stats
//! - `PvaSession`: async I/O (TCP frames, handshake, monitors)
//! - `ConnectionState`: channel state, registry, message counters

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

use crate::runtime::session::PvaSession;

/// Pool of active TCP sessions — one per IOC address.
pub struct SessionPool {
    sessions: HashMap<SocketAddr, PvaSession>,
}

impl SessionPool {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    /// Insert a connected session.
    pub fn insert(&mut self, session: PvaSession) {
        self.sessions.insert(session.addr(), session);
    }

    /// Get an existing session by address.
    pub fn get(&self, addr: &SocketAddr) -> Option<&PvaSession> {
        self.sessions.get(addr)
    }

    /// Get a mutable reference to an existing session.
    pub fn get_mut(&mut self, addr: &SocketAddr) -> Option<&mut PvaSession> {
        self.sessions.get_mut(addr)
    }

    /// Check if a session exists for this address.
    pub fn contains(&self, addr: &SocketAddr) -> bool {
        self.sessions.contains_key(addr)
    }

    /// Remove a session (after disconnect).
    pub fn remove(&mut self, addr: &SocketAddr) -> Option<PvaSession> {
        self.sessions.remove(addr)
    }

    /// Number of sessions.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Total active channels across all sessions — O(N sessions).
    pub fn total_active_channels(&self) -> usize {
        self.sessions
            .values()
            .map(|s| s.active_channel_count())
            .sum()
    }

    /// Total monitors across all sessions.
    pub fn total_monitors(&self) -> usize {
        self.sessions.values().map(|s| s.monitor_count()).sum()
    }

    /// All connected addresses.
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.sessions.keys().copied().collect()
    }

    /// Iterate over all sessions.
    pub fn iter(&self) -> impl Iterator<Item = (&SocketAddr, &PvaSession)> {
        self.sessions.iter()
    }

    /// Mutable iteration (for event loop dispatch).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&SocketAddr, &mut PvaSession)> {
        self.sessions.iter_mut()
    }

    /// Remove sessions with no active channels and no monitors.
    pub fn remove_idle(&mut self) -> usize {
        let idle: Vec<SocketAddr> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.active_channel_count() == 0 && s.monitor_count() == 0)
            .map(|(a, _)| *a)
            .collect();
        let count = idle.len();
        for addr in idle {
            self.sessions.remove(&addr);
        }
        count
    }
}

impl Default for SessionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SessionPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionPool")
            .field("sessions", &self.sessions.len())
            .field("channels", &self.total_active_channels())
            .field("monitors", &self.total_monitors())
            .finish()
    }
}

impl fmt::Display for SessionPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SessionPool[{} sessions, {} channels, {} monitors]",
            self.sessions.len(),
            self.total_active_channels(),
            self.total_monitors()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::new(Ipv4Addr::new(10, 0, 1, last).into(), 5075)
    }

    #[test]
    fn test_new_empty() {
        let p = SessionPool::new();
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
        assert_eq!(p.total_active_channels(), 0);
        assert_eq!(p.total_monitors(), 0);
    }

    #[test]
    fn test_default() {
        assert!(SessionPool::default().is_empty());
    }

    #[test]
    fn test_contains_empty() {
        assert!(!SessionPool::new().contains(&addr(1)));
    }

    #[test]
    fn test_get_empty() {
        assert!(SessionPool::new().get(&addr(1)).is_none());
    }

    #[test]
    fn test_remove_empty() {
        assert!(SessionPool::new().remove(&addr(1)).is_none());
    }

    #[test]
    fn test_addresses_empty() {
        assert!(SessionPool::new().addresses().is_empty());
    }

    #[test]
    fn test_iter_empty() {
        assert_eq!(SessionPool::new().iter().count(), 0);
    }

    #[test]
    fn test_remove_idle_empty() {
        assert_eq!(SessionPool::new().remove_idle(), 0);
    }

    #[test]
    fn test_display() {
        assert!(SessionPool::new().to_string().contains("SessionPool"));
    }
    #[test]
    fn test_debug() {
        assert!(format!("{:?}", SessionPool::new()).contains("SessionPool"));
    }
}
