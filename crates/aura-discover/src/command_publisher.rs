//! Command publisher - batched subscribe/unsubscribe commands for Redis.
//!
//! Publishes commands on the `aura:commands` Redis channel.
//! aura-ingest subscribes to this channel and reacts in real time.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Redis channel for discover -> ingest commands.
pub const COMMAND_CHANNEL: &str = "aura:commands";

/// A command from discover to ingest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum IngestCommand {
    /// Subscribe to a PV. IOC address included for connection reuse.
    Subscribe {
        pv: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        ioc: Option<String>,
    },
    /// Subscribe to many PVs at once (batch mode for fast startup).
    SubscribeBatch {
        pvs: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ioc: Option<String>,
    },
    /// Unsubscribe from a PV.
    Unsubscribe { pv: String },
    /// Unsubscribe from many PVs at once (batch mode for bulk deletes).
    UnsubscribeBatch { pvs: Vec<String> },
    /// Reload all PV configs from DB.
    Reload,
}

impl IngestCommand {
    pub fn subscribe(pv: impl Into<String>, ioc: Option<SocketAddr>) -> Self {
        Self::Subscribe {
            pv: pv.into(),
            ioc: ioc.map(|a| a.to_string()),
        }
    }

    pub fn unsubscribe(pv: impl Into<String>) -> Self {
        Self::Unsubscribe { pv: pv.into() }
    }

    pub fn reload() -> Self {
        Self::Reload
    }

    /// Serialize to JSON for Redis PUBLISH.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("IngestCommand serialization is infallible")
    }

    /// Deserialize from JSON (received by aura-ingest).
    pub fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok()
    }
}

impl std::fmt::Display for IngestCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Subscribe { pv, ioc } => {
                write!(f, "SUBSCRIBE {pv}")?;
                if let Some(addr) = ioc {
                    write!(f, " (ioc={addr})")?;
                }
                Ok(())
            }
            Self::SubscribeBatch { pvs, ioc } => {
                write!(f, "SUBSCRIBE_BATCH [{} PVs]", pvs.len())?;
                if let Some(addr) = ioc {
                    write!(f, " (ioc={addr})")?;
                }
                Ok(())
            }
            Self::Unsubscribe { pv } => write!(f, "UNSUBSCRIBE {pv}"),
            Self::UnsubscribeBatch { pvs } => write!(f, "UNSUBSCRIBE_BATCH [{} PVs]", pvs.len()),
            Self::Reload => write!(f, "RELOAD"),
        }
    }
}

/// Accumulates commands for batched publishing.
///
/// Collect commands during a reconciliation cycle, then flush
/// all at once via a Redis pipeline (1 round-trip for N commands).
pub struct CommandBatch {
    commands: Vec<IngestCommand>,
}

impl CommandBatch {
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
        }
    }

    /// Queue a command for the next flush.
    pub fn push(&mut self, cmd: IngestCommand) {
        self.commands.push(cmd);
    }

    /// Queue an UnsubscribeBatch for multiple PVs (1 JSON message for N PVs).
    pub fn unsubscribe_batch(&mut self, pvs: &[String]) {
        if !pvs.is_empty() {
            self.commands
                .push(IngestCommand::UnsubscribeBatch { pvs: pvs.to_vec() });
        }
    }

    /// Take all pending commands for flushing (caller does Redis I/O).
    /// Returns JSON-serialized commands ready for PUBLISH.
    pub fn take(&mut self) -> Vec<String> {
        if self.commands.is_empty() {
            return Vec::new();
        }
        std::mem::take(&mut self.commands)
            .into_iter()
            .map(|cmd| cmd.to_json())
            .collect()
    }

    pub fn pending_count(&self) -> usize {
        self.commands.len()
    }
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

impl Default for CommandBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CommandBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommandBatch[{} pending]", self.commands.len())
    }
}

impl std::fmt::Debug for CommandBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandBatch")
            .field("pending", &self.commands.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::new(std::net::Ipv4Addr::new(10, 0, 1, n).into(), 5075)
    }

    #[test]
    fn test_subscribe() {
        let cmd = IngestCommand::subscribe("PV:A", Some(addr(1)));
        assert!(matches!(cmd, IngestCommand::Subscribe { .. }));
    }

    #[test]
    fn test_subscribe_no_ioc() {
        match IngestCommand::subscribe("PV:A", None) {
            IngestCommand::Subscribe { ioc, .. } => assert!(ioc.is_none()),
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_unsubscribe() {
        let cmd = IngestCommand::unsubscribe("PV:A");
        assert!(matches!(cmd, IngestCommand::Unsubscribe { .. }));
    }

    #[test]
    fn test_reload() {
        assert!(matches!(IngestCommand::reload(), IngestCommand::Reload));
    }

    #[test]
    fn test_subscribe_json_roundtrip() {
        let cmd = IngestCommand::subscribe("PV:A", Some(addr(1)));
        let back = IngestCommand::from_json(&cmd.to_json()).unwrap();
        assert_eq!(cmd, back);
    }

    #[test]
    fn test_unsubscribe_json_roundtrip() {
        let cmd = IngestCommand::unsubscribe("PV:B");
        assert_eq!(IngestCommand::from_json(&cmd.to_json()).unwrap(), cmd);
    }

    #[test]
    fn test_reload_json_roundtrip() {
        let cmd = IngestCommand::reload();
        assert_eq!(IngestCommand::from_json(&cmd.to_json()).unwrap(), cmd);
    }

    #[test]
    fn test_subscribe_batch_roundtrip() {
        let cmd = IngestCommand::SubscribeBatch {
            pvs: vec!["A".into(), "B".into()],
            ioc: Some("10.0.1.1:5075".into()),
        };
        assert_eq!(IngestCommand::from_json(&cmd.to_json()).unwrap(), cmd);
    }

    #[test]
    fn test_unsubscribe_batch_roundtrip() {
        let cmd = IngestCommand::UnsubscribeBatch {
            pvs: vec!["A".into(), "B".into()],
        };
        assert_eq!(IngestCommand::from_json(&cmd.to_json()).unwrap(), cmd);
    }

    #[test]
    fn test_subscribe_json_format() {
        let json = IngestCommand::subscribe("PV:A", None).to_json();
        assert!(json.contains(r#""cmd":"subscribe""#));
        assert!(json.contains(r#""pv":"PV:A""#));
        assert!(!json.contains("ioc"));
    }

    #[test]
    fn test_subscribe_json_with_ioc() {
        let json = IngestCommand::subscribe("PV:A", Some(addr(1))).to_json();
        assert!(json.contains("ioc"));
        assert!(json.contains("10.0.1.1:5075"));
    }

    #[test]
    fn test_from_json_invalid() {
        assert!(IngestCommand::from_json("garbage").is_none());
    }
    #[test]
    fn test_from_json_empty() {
        assert!(IngestCommand::from_json("").is_none());
    }

    #[test]
    fn test_display_subscribe() {
        assert_eq!(
            IngestCommand::subscribe("PV:A", None).to_string(),
            "SUBSCRIBE PV:A"
        );
    }

    #[test]
    fn test_display_subscribe_ioc() {
        let s = IngestCommand::subscribe("PV:A", Some(addr(1))).to_string();
        assert!(s.contains("SUBSCRIBE PV:A") && s.contains("ioc="));
    }

    #[test]
    fn test_display_unsubscribe() {
        assert_eq!(
            IngestCommand::unsubscribe("PV:A").to_string(),
            "UNSUBSCRIBE PV:A"
        );
    }

    #[test]
    fn test_display_unsubscribe_batch() {
        let cmd = IngestCommand::UnsubscribeBatch {
            pvs: vec!["A".into(), "B".into()],
        };
        assert!(cmd.to_string().contains("2 PVs"));
    }

    #[test]
    fn test_display_reload() {
        assert_eq!(IngestCommand::reload().to_string(), "RELOAD");
    }

    #[test]
    fn test_batch_new() {
        let b = CommandBatch::new();
        assert!(b.is_empty());
        assert_eq!(b.pending_count(), 0);
    }

    #[test]
    fn test_batch_default() {
        assert!(CommandBatch::default().is_empty());
    }

    #[test]
    fn test_batch_push() {
        let mut b = CommandBatch::new();
        b.push(IngestCommand::subscribe("PV:A", None));
        assert_eq!(b.pending_count(), 1);
    }

    #[test]
    fn test_batch_unsubscribe_batch() {
        let mut b = CommandBatch::new();
        b.unsubscribe_batch(&["A".into(), "B".into(), "C".into()]);
        assert_eq!(b.pending_count(), 1); // 1 command, 3 PVs inside
        let jsons = b.take();
        assert_eq!(jsons.len(), 1);
        let cmd = IngestCommand::from_json(&jsons[0]).unwrap();
        assert!(matches!(cmd, IngestCommand::UnsubscribeBatch { pvs } if pvs.len() == 3));
    }

    #[test]
    fn test_batch_unsubscribe_batch_empty_noop() {
        let mut b = CommandBatch::new();
        b.unsubscribe_batch(&[]);
        assert!(b.is_empty());
    }

    #[test]
    fn test_batch_take() {
        let mut b = CommandBatch::new();
        b.push(IngestCommand::subscribe("A", None));
        b.push(IngestCommand::unsubscribe("B"));
        let jsons = b.take();
        assert_eq!(jsons.len(), 2);
        assert!(b.is_empty());
    }

    #[test]
    fn test_batch_take_empty() {
        assert!(CommandBatch::new().take().is_empty());
    }

    #[test]
    fn test_batch_take_consecutive() {
        let mut b = CommandBatch::new();
        b.push(IngestCommand::subscribe("A", None));
        b.take();
        b.push(IngestCommand::subscribe("B", None));
        let jsons = b.take();
        assert_eq!(jsons.len(), 1);
        assert_eq!(
            IngestCommand::from_json(&jsons[0]).unwrap(),
            IngestCommand::subscribe("B", None)
        );
    }

    #[test]
    fn test_batch_display() {
        assert!(CommandBatch::new().to_string().contains("CommandBatch"));
    }
    #[test]
    fn test_batch_debug() {
        assert!(format!("{:?}", CommandBatch::new()).contains("CommandBatch"));
    }

    #[test]
    fn test_batch_100k_subscribe() {
        let mut b = CommandBatch::new();
        let pvs: Vec<String> = (0..100_000).map(|i| format!("PV:{i}")).collect();
        b.push(IngestCommand::SubscribeBatch {
            pvs,
            ioc: Some("10.0.1.1:5075".into()),
        });
        let t = std::time::Instant::now();
        let jsons = b.take();
        let elapsed = t.elapsed();
        assert_eq!(jsons.len(), 1);
        assert!(
            elapsed.as_millis() < 2000,
            "serialization took {}ms",
            elapsed.as_millis()
        );
    }
}