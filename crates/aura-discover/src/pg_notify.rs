//! PostgreSQL LISTEN/NOTIFY - real-time config change notifications.
//!
//! Spawns a tokio task that listens for `ioc_changes` and `pv_changes`,
//! converts NOTIFY payloads to IngestCommand JSON, and sends via mpsc.
//!
//! Payload format (set by DB triggers):
//! - `pv_changes`: `"add:PV:A,PV:B,PV:C"` or `"del:PV:X,PV:Y"`
//! - `ioc_changes`: `"10.0.1.5:5075"` (IOC address)

use crate::command_publisher::IngestCommand;

/// Spawn the PgListener task. Returns the notify receiver for the main select! loop.
pub fn spawn_listener(db_url: String) -> tokio::sync::mpsc::Receiver<String> {
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(256);
    tokio::spawn(async move {
        let mut listener = match sqlx::postgres::PgListener::connect(&db_url).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "PgListener connect failed");
                return;
            }
        };
        if let Err(e) = listener.listen_all(vec!["ioc_changes", "pv_changes"]).await {
            tracing::error!(error = %e, "LISTEN setup failed");
            return;
        }
        tracing::info!("LISTEN ioc_changes + pv_changes active");

        loop {
            match listener.recv().await {
                Ok(notification) => {
                    let cmd_json = match notification.channel() {
                        "pv_changes" => parse_pv_change(notification.payload()),
                        "ioc_changes" => Some(
                            serde_json::json!({"cmd": "ioc_change", "address": notification.payload()}).to_string()
                        ),
                        _ => None,
                    };
                    if let Some(json) = cmd_json {
                        if tx.send(json).await.is_err() {
                            tracing::warn!("pg_notify channel closed — exiting listener");
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "PgListener error — reconnecting in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    match sqlx::postgres::PgListener::connect(&db_url).await {
                        Ok(new_l) => {
                            listener = new_l;
                            if let Err(e2) =
                                listener.listen_all(vec!["ioc_changes", "pv_changes"]).await
                            {
                                tracing::error!(error = %e2, "LISTEN re-setup failed");
                            } else {
                                tracing::info!("PgListener reconnected");
                            }
                        }
                        Err(e2) => tracing::error!(error = %e2, "PgListener reconnect failed"),
                    }
                }
            }
        }
    });
    rx
}

/// Parse a `pv_changes` NOTIFY payload into an IngestCommand JSON string.
///
/// Format: `"add:PV:A,PV:B"` or `"del:PV:X,PV:Y"`
fn parse_pv_change(payload: &str) -> Option<String> {
    if let Some(pv_list) = payload.strip_prefix("add:") {
        let pvs: Vec<String> = pv_list
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        if pvs.is_empty() {
            return None;
        }
        Some(IngestCommand::SubscribeBatch { pvs, ioc: None }.to_json())
    } else if let Some(pv_list) = payload.strip_prefix("del:") {
        let pvs: Vec<String> = pv_list
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        if pvs.is_empty() {
            return None;
        }
        Some(IngestCommand::UnsubscribeBatch { pvs }.to_json())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_add_single() {
        let json = parse_pv_change("add:PV:A").unwrap();
        let cmd = IngestCommand::from_json(&json).unwrap();
        assert!(matches!(cmd, IngestCommand::SubscribeBatch { pvs, ioc }
            if pvs == vec!["PV:A"] && ioc.is_none()));
    }

    #[test]
    fn test_parse_add_batch() {
        let json = parse_pv_change("add:PV:A,PV:B,PV:C").unwrap();
        let cmd = IngestCommand::from_json(&json).unwrap();
        assert!(matches!(cmd, IngestCommand::SubscribeBatch { pvs, .. } if pvs.len() == 3));
    }

    #[test]
    fn test_parse_del_single() {
        let json = parse_pv_change("del:PV:X").unwrap();
        let cmd = IngestCommand::from_json(&json).unwrap();
        assert!(matches!(cmd, IngestCommand::UnsubscribeBatch { pvs } if pvs == vec!["PV:X"]));
    }

    #[test]
    fn test_parse_del_batch() {
        let json = parse_pv_change("del:PV:X,PV:Y").unwrap();
        let cmd = IngestCommand::from_json(&json).unwrap();
        assert!(matches!(cmd, IngestCommand::UnsubscribeBatch { pvs } if pvs.len() == 2));
    }

    #[test]
    fn test_parse_empty_payload() {
        assert!(parse_pv_change("").is_none());
    }

    #[test]
    fn test_parse_unknown_prefix() {
        assert!(parse_pv_change("update:PV:A").is_none());
    }

    #[test]
    fn test_parse_add_empty_list() {
        assert!(parse_pv_change("add:").is_none());
    }

    #[test]
    fn test_parse_del_empty_list() {
        assert!(parse_pv_change("del:").is_none());
    }

    #[test]
    fn test_parse_add_trailing_comma() {
        let json = parse_pv_change("add:PV:A,PV:B,").unwrap();
        let cmd = IngestCommand::from_json(&json).unwrap();
        assert!(matches!(cmd, IngestCommand::SubscribeBatch { pvs, .. } if pvs.len() == 2));
    }

    #[test]
    fn test_roundtrip_through_ingest_command() {
        let json = parse_pv_change("add:PV:1,PV:2").unwrap();
        let cmd = IngestCommand::from_json(&json).unwrap();
        let rejson = cmd.to_json();
        assert_eq!(IngestCommand::from_json(&rejson).unwrap(), cmd);
    }
}
