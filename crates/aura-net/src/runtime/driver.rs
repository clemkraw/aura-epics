//! PVA driver - top-level async API for AURA's PVAccess ingest.
//!
//! ## Architecture: TCP Search + Targeted Subscribe (pvxs only)
//!
//! IOCs run pvxs (QSRV2) and are listed in `ioc_config` table.
//!
//! ## Protocol flow
//!
//! Phase A — TCP SEARCH (parallel per IOC)
//! Phase B — TARGETED SUBSCRIBE (parallel per IOC)
//! Phase C — EVENT LOOPS (one per IOC, with auto-reconnect)

use std::collections::HashMap;
use std::net::SocketAddr;

use super::session::{PvaSession, SessionError};
use crate::client::config::PvaClientConfig;
use crate::monitor::handle::MonitorHandle;

pub enum SessionCommand {
    AddMonitor {
        pv_name: String,
        reply: tokio::sync::oneshot::Sender<Result<MonitorHandle, SessionError>>,
    },
    AddMonitorBatch {
        pv_names: Vec<String>,
        reply: tokio::sync::oneshot::Sender<Vec<(String, Result<MonitorHandle, SessionError>)>>,
    },
    /// D4: Stop monitoring a PV — sends CMD_MONITOR STOP + CMD_DESTROY_CHANNEL.
    RemoveMonitor { pv_name: String },
    /// Graceful shutdown — close TCP connection cleanly (FIN, not RST).
    Shutdown,
}

#[derive(Debug)]
pub enum DriverError {
    Session(SessionError),
    SearchFailed(String),
    Io(std::io::Error),
}

impl From<SessionError> for DriverError {
    fn from(e: SessionError) -> Self {
        Self::Session(e)
    }
}
impl From<std::io::Error> for DriverError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Session(e) => write!(f, "session: {e}"),
            Self::SearchFailed(msg) => write!(f, "search: {msg}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
        }
    }
}

/// Lifecycle event emitted by event loops to the main loop.
#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    Disconnected {
        addr: SocketAddr,
        pvs: Vec<String>,
        reason: String,
    },
    Reconnected {
        addr: SocketAddr,
        pvs: Vec<String>,
    },
}

pub struct PvaDriver {
    config: PvaClientConfig,
    name_servers: Vec<SocketAddr>,
    /// PV name -> IOC address (for unsubscribe routing + reconnect).
    pv_server_cache: HashMap<String, SocketAddr>,
    /// PVs that failed discovery - retried on hot-add IOC (D6).
    failed_pvs: Vec<String>,
    /// Command channels to running session event loops.
    session_commands: HashMap<SocketAddr, tokio::sync::mpsc::Sender<SessionCommand>>,
    /// Tracks which PVs are on which IOC for reconnect (D5).
    ioc_pvs: HashMap<SocketAddr, Vec<String>>,
    bus_tx: Option<crate::monitor::bus::MonitorBusTx>,
    /// CancellationToken per IOC - cancel to stop event loop + reconnect.
    cancel_tokens: HashMap<SocketAddr, tokio_util::sync::CancellationToken>,
    /// Shared metadata buffer - sessions push first full update here. Main.rs drains.
    metadata_buf: std::sync::Arc<
        std::sync::Mutex<Vec<(std::sync::Arc<str>, crate::types::pva_value::PvaValue)>>,
    >,
    pv_cache: Option<
        std::sync::Arc<arc_swap::ArcSwap<std::collections::HashMap<std::sync::Arc<str>, i32>>>,
    >,
    /// Reconnect notification - event loop sends (addr, new_cmd_tx) on successful reconnect.
    reconnect_tx:
        tokio::sync::mpsc::Sender<(SocketAddr, tokio::sync::mpsc::Sender<SessionCommand>)>,
    reconnect_rx: Option<
        tokio::sync::mpsc::Receiver<(SocketAddr, tokio::sync::mpsc::Sender<SessionCommand>)>,
    >,
    /// Lifecycle events - disconnect/reconnect notifications for pv_events table.
    lifecycle_tx: tokio::sync::mpsc::Sender<LifecycleEvent>,
    lifecycle_rx: Option<tokio::sync::mpsc::Receiver<LifecycleEvent>>,
}

impl PvaDriver {
    pub fn new(config: PvaClientConfig) -> Self {
        let name_servers = config.name_servers.clone();
        let (reconnect_tx, reconnect_rx) = tokio::sync::mpsc::channel(64);
        let (lifecycle_tx, lifecycle_rx) = tokio::sync::mpsc::channel(256);
        Self {
            config,
            name_servers,
            pv_server_cache: HashMap::new(),
            failed_pvs: Vec::new(),
            session_commands: HashMap::new(),
            ioc_pvs: HashMap::new(),
            bus_tx: None,
            cancel_tokens: HashMap::new(),
            metadata_buf: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            pv_cache: None,
            reconnect_tx: reconnect_tx,
            reconnect_rx: Some(reconnect_rx),
            lifecycle_tx: lifecycle_tx,
            lifecycle_rx: Some(lifecycle_rx),
        }
    }

    /// Take the reconnect notification receiver (called once by main.rs for the select! loop).
    pub fn take_reconnect_rx(
        &mut self,
    ) -> tokio::sync::mpsc::Receiver<(SocketAddr, tokio::sync::mpsc::Sender<SessionCommand>)> {
        self.reconnect_rx
            .take()
            .expect("reconnect_rx already taken")
    }

    /// Take the lifecycle event receiver (disconnect/reconnect notifications).
    pub fn take_lifecycle_rx(&mut self) -> tokio::sync::mpsc::Receiver<LifecycleEvent> {
        self.lifecycle_rx
            .take()
            .expect("lifecycle_rx already taken")
    }

    pub fn set_bus_tx(&mut self, bus: crate::monitor::bus::MonitorBusTx) {
        self.bus_tx = Some(bus);
    }

    pub fn set_name_servers(&mut self, servers: Vec<SocketAddr>) {
        self.name_servers = servers;
    }

    pub async fn monitor_batch(
        &mut self,
        pv_names: &[String],
    ) -> Vec<(String, Result<MonitorHandle, DriverError>)> {
        let mut results: Vec<(String, Result<MonitorHandle, DriverError>)> =
            Vec::with_capacity(pv_names.len());

        let servers = self.name_servers.clone();
        if servers.is_empty() {
            for pv in pv_names {
                results.push((
                    pv.clone(),
                    Err(DriverError::SearchFailed("no IOCs configured".into())),
                ));
            }
            return results;
        }

        let total_pvs = pv_names.len();
        let all_pvs: Vec<String> = pv_names.to_vec();
        let timeout = self.config.conn_timeout;
        let buf_sz = self.config.buffer_size;
        let reg_sz = self.config.registry_size;

        // PHASE A: TWO-ROUND TCP SEARCH (progressive elimination)
        // Round 1: distribute PVs evenly - each IOC gets N/K PVs.
        // Round 2: broadcast only unfound PVs to all IOCs.
        // Reduces search traffic from O(N×K) to ~O(N) for even distributions.

        let mut pv_to_server: HashMap<String, SocketAddr> = HashMap::with_capacity(total_pvs);
        let n_servers = servers.len();

        // Round 1: split PVs across IOCs (each gets N/K PVs).
        let mut partitions: Vec<Vec<String>> = (0..n_servers)
            .map(|_| Vec::with_capacity(total_pvs / n_servers + 1))
            .collect();
        for (i, pv) in all_pvs.iter().enumerate() {
            partitions[i % n_servers].push(pv.clone());
        }

        let mut r1_handles: Vec<tokio::task::JoinHandle<(SocketAddr, Vec<String>)>> = Vec::new();
        for (i, &srv) in servers.iter().enumerate() {
            let pvs: std::sync::Arc<[String]> = std::mem::take(&mut partitions[i]).into();
            r1_handles.push(tokio::spawn(async move {
                let mut session = match PvaSession::connect(srv, timeout, buf_sz, reg_sz).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::debug!(%srv, error = %e, "TCP search R1 connect failed");
                        return (srv, Vec::new());
                    }
                };
                tracing::info!(%srv, pvs = pvs.len(), "TCP search R1");
                let found = session.search_pvs(&pvs).await;
                tracing::info!(%srv, found = found.len(), "TCP search R1 done");
                (srv, found)
            }));
        }
        for h in r1_handles {
            if let Ok((addr, found_pvs)) = h.await {
                for pv in found_pvs {
                    pv_to_server.entry(pv).or_insert(addr);
                }
            }
        }
        tracing::info!(
            found_r1 = pv_to_server.len(),
            total = total_pvs,
            "search round 1 complete"
        );

        // Round 2: broadcast unfound PVs to all IOCs.
        let unfound: Vec<String> = all_pvs
            .iter()
            .filter(|pv| !pv_to_server.contains_key(*pv))
            .cloned()
            .collect();
        if !unfound.is_empty() {
            let unfound_arc: std::sync::Arc<[String]> = unfound.into();
            let mut r2_handles: Vec<tokio::task::JoinHandle<(SocketAddr, Vec<String>)>> =
                Vec::new();
            for &srv in &servers {
                let pvs = std::sync::Arc::clone(&unfound_arc);
                r2_handles.push(tokio::spawn(async move {
                    let mut session = match PvaSession::connect(srv, timeout, buf_sz, reg_sz).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::debug!(%srv, error = %e, "TCP search R2 connect failed");
                            return (srv, Vec::new());
                        }
                    };
                    let found = session.search_pvs(&pvs).await;
                    tracing::info!(%srv, found = found.len(), unfound = pvs.len(), "TCP search R2 done");
                    (srv, found)
                }));
            }
            for h in r2_handles {
                if let Ok((addr, found_pvs)) = h.await {
                    for pv in found_pvs {
                        pv_to_server.entry(pv).or_insert(addr);
                    }
                }
            }
        }

        let n_iocs = pv_to_server
            .values()
            .collect::<std::collections::HashSet<_>>()
            .len();
        // Log PV count per IOC for operational visibility.
        {
            let mut per_ioc: HashMap<SocketAddr, usize> = HashMap::new();
            for addr in pv_to_server.values() {
                *per_ioc.entry(*addr).or_default() += 1;
            }
            for (addr, count) in &per_ioc {
                tracing::info!(%addr, pvs = count, "IOC PV distribution");
            }
        }
        tracing::info!(
            found = pv_to_server.len(),
            total = total_pvs,
            iocs = n_iocs,
            "search complete"
        );

        // Track failed PVs for D6 hot-add.
        self.failed_pvs = pv_names
            .iter()
            .filter(|pv| !pv_to_server.contains_key(pv.as_str()))
            .cloned()
            .collect();

        // PHASE B: TARGETED SUBSCRIBE

        let mut server_pvs: HashMap<SocketAddr, Vec<String>> = HashMap::new();
        for (pv, addr) in &pv_to_server {
            server_pvs.entry(*addr).or_default().push(pv.clone());
        }

        let bus_tx_clone = self.bus_tx.clone();
        let config_timeout = self.config.conn_timeout;
        let config_buf = self.config.buffer_size;
        let config_reg = self.config.registry_size;

        // Split: reuse existing sessions vs create new ones.
        let mut new_ioc_pvs: HashMap<SocketAddr, Vec<String>> = HashMap::new();
        let mut existing_ioc_pvs: HashMap<SocketAddr, Vec<String>> = HashMap::new();
        for (addr, pvs) in server_pvs {
            if self.session_commands.contains_key(&addr) {
                existing_ioc_pvs.insert(addr, pvs);
            } else {
                new_ioc_pvs.insert(addr, pvs);
            }
        }

        // Existing IOCs: add monitors on the SAME TCP connection.
        for (addr, pvs) in existing_ioc_pvs {
            if let Some(tx) = self.session_commands.get(&addr) {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                if tx
                    .send(SessionCommand::AddMonitorBatch {
                        pv_names: pvs.clone(),
                        reply: reply_tx,
                    })
                    .await
                    .is_ok()
                {
                    let timeout =
                        std::time::Duration::from_secs(30 + (pvs.len() as u64 / 500).max(1));
                    match tokio::time::timeout(timeout, reply_rx).await {
                        Ok(Ok(batch_results)) => {
                            let ok = batch_results.iter().filter(|(_, r)| r.is_ok()).count();
                            for (pv, res) in batch_results {
                                if res.is_ok() {
                                    self.pv_server_cache.insert(pv.clone(), addr);
                                }
                                results.push((pv, res.map_err(DriverError::Session)));
                            }
                            if let Some(existing) = self.ioc_pvs.get_mut(&addr) {
                                existing.extend(
                                    pvs.iter()
                                        .filter(|p| self.pv_server_cache.contains_key(p.as_str()))
                                        .cloned(),
                                );
                            }
                            tracing::info!(%addr, ok, total = pvs.len(), "added to existing session");
                        }
                        _ => {
                            tracing::warn!(%addr, "AddMonitorBatch timeout — falling back to new session");
                            new_ioc_pvs.insert(addr, pvs);
                        }
                    }
                } else {
                    new_ioc_pvs.insert(addr, pvs);
                }
            }
        }

        // New IOCs: create fresh TCP sessions.
        let mut subscribe_handles: Vec<
            tokio::task::JoinHandle<
                Option<(
                    SocketAddr,
                    PvaSession,
                    Vec<String>,
                    Vec<(String, Result<MonitorHandle, DriverError>)>,
                )>,
            >,
        > = Vec::new();

        for (addr, pvs) in new_ioc_pvs {
            let bus = bus_tx_clone.clone();
            let meta_buf = self.metadata_buf.clone();
            let pv_cache_clone = self.pv_cache.clone();
            let pvs_clone = pvs.clone();
            subscribe_handles.push(tokio::spawn(async move {
                let mut session =
                    match PvaSession::connect(addr, config_timeout, config_buf, config_reg).await {
                        Ok(mut s) => {
                            if let Some(ref b) = bus {
                                s.set_bus_tx(b.clone());
                            }
                            s.set_metadata_buf(meta_buf.clone());
                            if let Some(ref pc) = pv_cache_clone {
                                s.set_pv_cache(pc.clone());
                            }
                            s
                        }
                        Err(e) => {
                            tracing::warn!(%addr, error = %e, "subscribe connect failed");
                            return None;
                        }
                    };
                tracing::info!(%addr, pvs = pvs.len(), "subscribing");
                let bulk = session.add_monitors_bulk(&pvs).await;
                let mapped: Vec<(String, Result<MonitorHandle, DriverError>)> = bulk
                    .into_iter()
                    .map(|(pv, r)| (pv, r.map_err(DriverError::Session)))
                    .collect();
                let ok = mapped.iter().filter(|(_, r)| r.is_ok()).count();
                tracing::info!(%addr, ok = ok, total = pvs.len(), "subscribe complete");
                Some((addr, session, pvs_clone, mapped))
            }));
        }

        // PHASE C: COLLECT + SPAWN EVENT LOOPS (with D5 reconnect)

        for h in subscribe_handles {
            match h.await {
                Ok(Some((addr, session, pvs_on_ioc, mapped))) => {
                    let any_ok = mapped.iter().any(|(_, r)| r.is_ok());
                    for (pv, res) in mapped {
                        if res.is_ok() {
                            self.pv_server_cache.insert(pv.clone(), addr);
                        }
                        results.push((pv, res));
                    }
                    if any_ok {
                        // Track PVs per IOC for reconnect.
                        self.ioc_pvs.insert(addr, pvs_on_ioc);
                        self.spawn_event_loop(addr, session);
                    }
                }
                Ok(None) => {}
                Err(e) => tracing::error!("subscribe task panicked: {e}"),
            }
        }

        for pv in pv_names {
            if !pv_to_server.contains_key(pv) {
                results.push((
                    pv.clone(),
                    Err(DriverError::SearchFailed("not found".into())),
                ));
            }
        }

        let ok = results.iter().filter(|(_, r)| r.is_ok()).count();
        tracing::info!(
            subscribed = ok,
            total = total_pvs,
            iocs = n_iocs,
            "discovery complete"
        );

        results
    }

    /// Spawn an event loop with auto-reconnect (D5) and clean cancellation (D3).
    fn spawn_event_loop(&mut self, addr: SocketAddr, session: PvaSession) {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<SessionCommand>(256);
        self.session_commands.insert(addr, cmd_tx);

        // CancellationToken — cancel to stop event loop + reconnect loop cleanly.
        let cancel = tokio_util::sync::CancellationToken::new();
        self.cancel_tokens.insert(addr, cancel.clone());

        let reconnect_pvs = self.ioc_pvs.get(&addr).cloned().unwrap_or_default();
        let bus_tx = self.bus_tx.clone();
        let meta_buf = self.metadata_buf.clone();
        let pv_cache_clone = self.pv_cache.clone();
        let reconnect_tx = self.reconnect_tx.clone();
        let lifecycle_tx = self.lifecycle_tx.clone();
        let config_timeout = self.config.conn_timeout;
        let config_buf = self.config.buffer_size;
        let config_reg = self.config.registry_size;

        tokio::spawn(async move {
            let n = session.monitor_count();
            tracing::info!(%addr, monitors = n, "event loop started");

            let disconnect_reason = match session.run_event_loop_with_commands(cmd_rx).await {
                Ok(_) => {
                    tracing::info!(%addr, "event loop ended cleanly");
                    "clean shutdown".to_string()
                }
                Err(e) => {
                    tracing::warn!(%addr, error = %e, "event loop disconnected");
                    format!("{e}")
                }
            };

            // Emit DISCONNECT lifecycle event.
            if !reconnect_pvs.is_empty() {
                let _ = lifecycle_tx.try_send(LifecycleEvent::Disconnected {
                    addr,
                    pvs: reconnect_pvs.clone(),
                    reason: disconnect_reason,
                });
            }

            // D5: AUTO-RECONNECT (with D3 cancellation)
            if reconnect_pvs.is_empty() || cancel.is_cancelled() {
                return;
            }
            tracing::info!(%addr, pvs = reconnect_pvs.len(), "starting reconnect loop");

            let mut attempt = 0u32;
            loop {
                if cancel.is_cancelled() {
                    tracing::info!(%addr, "reconnect loop cancelled (IOC removed)");
                    return;
                }
                attempt += 1;

                // Exponential backoff: 5s -> 10s -> 30s -> 60s -> 300s (cap).
                // Resets to 5s on successful reconnect.
                let backoff_secs = match attempt {
                    1 => 5,
                    2 => 10,
                    3..=5 => 30,
                    6..=10 => 60,
                    _ => 300,
                };
                tracing::debug!(%addr, attempt, backoff_secs, "reconnect backoff");

                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                    _ = cancel.cancelled() => {
                        tracing::info!(%addr, "reconnect cancelled during wait");
                        return;
                    }
                }

                let mut session =
                    match PvaSession::connect(addr, config_timeout, config_buf, config_reg).await {
                        Ok(mut s) => {
                            if let Some(ref b) = bus_tx {
                                s.set_bus_tx(b.clone());
                            }
                            s.set_metadata_buf(meta_buf.clone());
                            if let Some(ref pc) = pv_cache_clone {
                                s.set_pv_cache(pc.clone());
                            }
                            s
                        }
                        Err(e) => {
                            if attempt % 12 == 1 {
                                tracing::warn!(%addr, attempt, error = %e, "reconnect failed");
                            }
                            continue;
                        }
                    };

                tracing::info!(%addr, attempt, "reconnected, re-subscribing {} PVs", reconnect_pvs.len());
                let bulk = session.add_monitors_bulk(&reconnect_pvs).await;
                let ok = bulk.iter().filter(|(_, r)| r.is_ok()).count();
                tracing::info!(%addr, ok, total = reconnect_pvs.len(), "reconnect subscribe done");

                if ok > 0 {
                    // Emit RECONNECT lifecycle event.
                    let _ = lifecycle_tx.try_send(LifecycleEvent::Reconnected {
                        addr,
                        pvs: reconnect_pvs.clone(),
                    });
                    // Send new cmd_tx to main loop.
                    let (new_tx, new_rx) = tokio::sync::mpsc::channel::<SessionCommand>(256);
                    let _ = reconnect_tx.send((addr, new_tx)).await;
                    let n = session.monitor_count();
                    tracing::info!(%addr, monitors = n, "reconnect event loop started");
                    let re_disconnect_reason =
                        match session.run_event_loop_with_commands(new_rx).await {
                            Ok(_) => {
                                tracing::info!(%addr, "reconnect event loop ended");
                                "clean shutdown".to_string()
                            }
                            Err(e) => {
                                tracing::warn!(%addr, error = %e, "reconnect event loop error");
                                format!("{e}")
                            }
                        };
                    // Emit DISCONNECT for the re-disconnect (mirrors first disconnect).
                    let _ = lifecycle_tx.try_send(LifecycleEvent::Disconnected {
                        addr,
                        pvs: reconnect_pvs.clone(),
                        reason: re_disconnect_reason,
                    });
                    attempt = 0;
                }
            }
        });
    }

    // D4: UNSUBSCRIBE

    /// Unsubscribe from PVs — sends RemoveMonitor to the appropriate session.
    pub async fn unsubscribe(&mut self, pv_names: &[String]) {
        for pv in pv_names {
            if let Some(addr) = self.pv_server_cache.remove(pv) {
                if let Some(tx) = self.session_commands.get(&addr) {
                    let _ = tx
                        .send(SessionCommand::RemoveMonitor {
                            pv_name: pv.clone(),
                        })
                        .await;
                }
                // Remove from ioc_pvs tracking.
                if let Some(pvs) = self.ioc_pvs.get_mut(&addr) {
                    pvs.retain(|p| p != pv);
                }
            }
        }
    }

    // D6: HOT-ADD IOC (re-search failed PVs)

    /// Called when a new IOC is added to ioc_config.
    /// Re-searches failed PVs on the new IOC and subscribes any found.
    /// Hot-add IOC: search failed PVs + PVs not yet in pv_server_cache.
    /// Returns recovered PV names for pv_lookup + pv_cache registration.
    pub async fn hot_add_ioc(
        &mut self,
        addr: SocketAddr,
        all_configured_pvs: &[String],
    ) -> Vec<String> {
        // Build search set: failed_pvs + configured PVs not yet subscribed.
        let mut search_set: std::collections::HashSet<String> =
            self.failed_pvs.iter().cloned().collect();
        for pv in all_configured_pvs {
            if !self.pv_server_cache.contains_key(pv) {
                search_set.insert(pv.clone());
            }
        }
        if search_set.is_empty() {
            return Vec::new();
        }
        let pvs: Vec<String> = search_set.into_iter().collect();

        tracing::info!(%addr, search_count = pvs.len(), failed = self.failed_pvs.len(), "hot-add IOC search");
        let timeout = self.config.conn_timeout;
        let buf = self.config.buffer_size;
        let reg = self.config.registry_size;

        // TCP search on the new IOC only.
        let mut session = match PvaSession::connect(addr, timeout, buf, reg).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(%addr, error = %e, "hot-add IOC connect failed");
                return Vec::new();
            }
        };
        let found = session.search_pvs(&pvs).await;
        drop(session);

        if found.is_empty() {
            tracing::info!(%addr, "hot-add IOC: no failed PVs found on this IOC");
            return Vec::new();
        }

        tracing::info!(%addr, found = found.len(), "hot-add IOC: found PVs, subscribing");

        // Subscribe on a fresh connection (search session can't subscribe).
        let bus = self.bus_tx.clone();
        let meta_buf = self.metadata_buf.clone();
        let pv_cache_clone = self.pv_cache.clone();
        let mut session = match PvaSession::connect(addr, timeout, buf, reg).await {
            Ok(mut s) => {
                if let Some(ref b) = bus {
                    s.set_bus_tx(b.clone());
                }
                s.set_metadata_buf(meta_buf);
                if let Some(ref pc) = pv_cache_clone {
                    s.set_pv_cache(pc.clone());
                }
                s
            }
            Err(_) => return Vec::new(),
        };

        let bulk = session.add_monitors_bulk(&found).await;
        let ok = bulk.iter().filter(|(_, r)| r.is_ok()).count();

        let mut recovered = Vec::new();
        if ok > 0 {
            for (pv, res) in &bulk {
                if res.is_ok() {
                    self.pv_server_cache.insert(pv.clone(), addr);
                    self.failed_pvs.retain(|p| p != pv);
                    recovered.push(pv.clone());
                }
            }
            self.ioc_pvs.insert(addr, found.clone());
            self.spawn_event_loop(addr, session);
        }

        recovered
    }

    // SESSION MANAGEMENT

    pub fn remove_session(&mut self, addr: &SocketAddr) {
        // Cancel the event loop + reconnect loop for this IOC.
        if let Some(cancel) = self.cancel_tokens.remove(addr) {
            cancel.cancel();
        }
        // Move PVs to failed_pvs so they can be re-searched on a new IOC.
        if let Some(pvs) = self.ioc_pvs.remove(addr) {
            self.failed_pvs.extend(pvs);
        }
        self.session_commands.remove(addr);
        self.pv_server_cache.retain(|_, v| v != addr);
    }

    // Accessors

    pub fn session_count(&self) -> usize {
        self.session_commands.len()
    }
    pub fn ioc_addresses(&self) -> Vec<SocketAddr> {
        self.name_servers.clone()
    }
    pub fn pv_server_cache(&self) -> &HashMap<String, SocketAddr> {
        &self.pv_server_cache
    }
    pub fn ioc_pvs(&self) -> &HashMap<SocketAddr, Vec<String>> {
        &self.ioc_pvs
    }

    /// Graceful shutdown: send TCP FIN to all IOCs (not RST).
    pub async fn shutdown_sessions(&self) {
        for (addr, tx) in &self.session_commands {
            let _ = tx.send(SessionCommand::Shutdown).await;
            tracing::debug!(%addr, "sent shutdown to session");
        }
        // Cancel reconnect loops so they don't restart.
        for cancel in self.cancel_tokens.values() {
            cancel.cancel();
        }
        // Brief yield to let sessions flush their TCP FIN.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    pub fn session_commands_mut(
        &mut self,
    ) -> &mut HashMap<SocketAddr, tokio::sync::mpsc::Sender<SessionCommand>> {
        &mut self.session_commands
    }

    /// Drain all pending metadata (O(1) swap). Scales to 1M+ PVs.
    pub fn set_pv_cache(
        &mut self,
        cache: std::sync::Arc<arc_swap::ArcSwap<HashMap<std::sync::Arc<str>, i32>>>,
    ) {
        self.pv_cache = Some(cache);
    }

    pub fn drain_metadata(&self) -> Vec<(std::sync::Arc<str>, crate::types::pva_value::PvaValue)> {
        if let Ok(mut buf) = self.metadata_buf.lock() {
            std::mem::take(&mut *buf)
        } else {
            Vec::new()
        }
    }
}

impl std::fmt::Display for PvaDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PvaDriver[{} IOCs, {} cached, {} failed]",
            self.name_servers.len(),
            self.pv_server_cache.len(),
            self.failed_pvs.len()
        )
    }
}

impl std::fmt::Debug for PvaDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PvaDriver")
            .field("iocs", &self.name_servers.len())
            .field("pv_cache", &self.pv_server_cache.len())
            .field("failed", &self.failed_pvs.len())
            .finish()
    }
}
