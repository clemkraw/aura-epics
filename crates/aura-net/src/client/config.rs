//! PVA client configuration.
//!
//! AURA connects to IOCs and PVA gateways via direct TCP (name_servers).
//! No UDP broadcast or beacon tracking — all endpoints are explicitly
//! configured in `aura.toml` or `ioc_config` table.

use std::net::SocketAddr;
use std::time::Duration;

pub const DEFAULT_CONN_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_BUFFER_SIZE: i32 = 65536;
pub const DEFAULT_REGISTRY_SIZE: i16 = 128;

#[derive(Debug, Clone)]
pub struct PvaClientConfig {
    /// TCP endpoints for IOCs and PVA gateways (IP:PORT).
    pub name_servers: Vec<SocketAddr>,
    /// TCP connection timeout.
    pub conn_timeout: Duration,
    /// PVA protocol buffer size (negotiated during handshake).
    pub buffer_size: i32,
    /// PVA type registry size (negotiated during handshake).
    pub registry_size: i16,
}

impl PvaClientConfig {
    pub fn new() -> Self {
        Self {
            name_servers: Vec::new(),
            conn_timeout: DEFAULT_CONN_TIMEOUT,
            buffer_size: DEFAULT_BUFFER_SIZE,
            registry_size: DEFAULT_REGISTRY_SIZE,
        }
    }

    /// Read overrides from EPICS_PVA_* environment variables.
    pub fn from_env() -> Self {
        let mut cfg = Self::new();
        if let Ok(ns) = std::env::var("EPICS_PVA_NAME_SERVERS") {
            cfg.name_servers = parse_addr_list(&ns);
        }
        cfg
    }

    pub fn with_conn_timeout(mut self, d: Duration) -> Self {
        self.conn_timeout = d;
        self
    }
    pub fn with_buffer_size(mut self, n: i32) -> Self {
        self.buffer_size = n;
        self
    }
}

impl Default for PvaClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PvaClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PvaConfig[servers={}, timeout={}s, buf={}KB]",
            self.name_servers.len(),
            self.conn_timeout.as_secs(),
            self.buffer_size / 1024,
        )
    }
}

fn parse_addr_list(s: &str) -> Vec<SocketAddr> {
    use std::net::IpAddr;
    s.split_whitespace()
        .filter_map(|tok| {
            if let Ok(sa) = tok.parse::<SocketAddr>() {
                return Some(sa);
            }
            // Bare IP without port → use default PVA port 5075
            if let Ok(ip) = tok.parse::<IpAddr>() {
                return Some(SocketAddr::new(ip, 5075));
            }
            None
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_defaults() {
        let c = PvaClientConfig::new();
        assert!(c.name_servers.is_empty());
        assert_eq!(c.conn_timeout.as_secs(), 30);
        assert_eq!(c.buffer_size, 65536);
        assert_eq!(c.registry_size, 128);
    }

    #[test]
    fn test_default_trait() {
        let c = PvaClientConfig::default();
        assert_eq!(c.conn_timeout.as_secs(), 30);
    }

    #[test]
    fn test_with_conn_timeout() {
        let c = PvaClientConfig::new().with_conn_timeout(Duration::from_secs(5));
        assert_eq!(c.conn_timeout.as_secs(), 5);
    }

    #[test]
    fn test_with_buffer_size() {
        let c = PvaClientConfig::new().with_buffer_size(32768);
        assert_eq!(c.buffer_size, 32768);
    }

    #[test]
    fn test_parse_ip_port() {
        assert_eq!(parse_addr_list("10.0.1.1:5075").len(), 1);
    }

    #[test]
    fn test_parse_bare_ip() {
        let addrs = parse_addr_list("10.0.1.1");
        assert_eq!(addrs[0].port(), 5075);
    }

    #[test]
    fn test_parse_multiple() {
        let addrs = parse_addr_list("10.0.1.1:5075 10.0.2.1:5076");
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn test_parse_empty() {
        assert!(parse_addr_list("").is_empty());
    }

    #[test]
    fn test_parse_invalid() {
        assert!(parse_addr_list("garbage not_ip").is_empty());
    }

    #[test]
    fn test_display() {
        let mut c = PvaClientConfig::new();
        c.name_servers = vec!["10.0.1.1:5075".parse().unwrap()];
        let s = c.to_string();
        assert!(s.contains("servers=1"));
        assert!(s.contains("timeout=30s"));
    }

    #[test]
    fn test_clone() {
        let a = PvaClientConfig::new().with_conn_timeout(Duration::from_secs(10));
        let b = a.clone();
        assert_eq!(a.conn_timeout, b.conn_timeout);
    }
}
