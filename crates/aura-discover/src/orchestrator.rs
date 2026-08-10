//! Discover orchestrator - the main async loop.
//!
//! Composes all discover modules into a single reconciliation loop:
//! 1. Poll pv_config for changes (incremental via NOTIFY or fallback poll)
//! 2. Reconcile changes -> actions
//! 3. Execute actions (subscribe, unsubscribe, alert)
//!
//! ## Architecture (pvxs TCP mode)
//!
//! IOCs are listed in `ioc_config` table with fixed ports.
//! PV discovery uses TCP CMD_SEARCH (pvxs QSRV2).
//! No UDP search, no beacons — everything is TCP + DB.
//!
//! ```text
//! orchestrator.run()
//!   └── loop every 30s:
//!         1. poll pv_config (incremental)
//!         2. reconcile -> actions
//!         3. resolve new PVs -> publish subscribe commands
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;

use aura_core::config::AuraConfig;

use crate::command_publisher::{COMMAND_CHANNEL, CommandBatch, IngestCommand};
use crate::config_poller::{ConfigPoller, PvChange};
use crate::reconciler::{Action, Reconciler};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Orchestrator state.
pub struct Orchestrator {
    config: AuraConfig,
    poller: ConfigPoller,
    reconciler: Reconciler,
    commands: CommandBatch,
    /// IOC server addresses (from ioc_config table).
    name_servers: Vec<std::net::SocketAddr>,
    last_poll: DateTime<Utc>,
    /// Total cycles completed.
    pub cycles: u64,
    /// Total PVs successfully resolved.
    pub total_resolved: u64,
    /// Total alerts generated.
    pub total_alerts: u64,
}

impl Orchestrator {
    pub fn new(config: AuraConfig) -> Self {
        let name_servers: Vec<std::net::SocketAddr> = config
            .discover
            .name_servers
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        Self {
            config,
            poller: ConfigPoller::new(),
            reconciler: Reconciler::new(),
            commands: CommandBatch::new(),
            name_servers,
            last_poll: DateTime::<Utc>::MIN_UTC,
            cycles: 0,
            total_resolved: 0,
            total_alerts: 0,
        }
    }

    /// Initial startup: load all enabled PVs and trigger resolution.
    pub async fn startup(&mut self, pool: &PgPool) -> Result<Vec<Action>, BoxError> {
        tracing::info!("discover startup: loading pv_config...");

        let all_configs = aura_store::pv_config::PvConfigDao::get_all_enabled(pool).await?;
        // Build changes before load_initial consumes the Vec.
        let changes: Vec<PvChange> = all_configs
            .iter()
            .map(|c| PvChange::Added(c.clone()))
            .collect();
        let count = self.poller.load_initial(all_configs);
        tracing::info!(pvs = count, "loaded initial pv_config");

        let actions = self.reconciler.reconcile(&changes);
        self.last_poll = Utc::now();

        tracing::info!(
            actions = actions.len(),
            pending = self.reconciler.pending_count(),
            "startup reconciliation complete"
        );

        Ok(actions)
    }

    /// Run one reconciliation cycle (called every poll_interval).
    pub async fn cycle(&mut self, pool: &PgPool) -> Result<CycleResult, BoxError> {
        self.cycles += 1;
        let mut result = CycleResult::default();

        // Incremental poll: only rows changed since last_poll.
        let fresh =
            aura_store::pv_config::PvConfigDao::get_changed_since(pool, self.last_poll).await?;
        self.last_poll = Utc::now();

        if fresh.is_empty() && self.reconciler.pending_count() == 0 {
            return Ok(result);
        }

        let changes = if !fresh.is_empty() {
            let all_enabled = aura_store::pv_config::PvConfigDao::get_all_enabled(pool).await?;
            self.poller.diff(all_enabled)
        } else {
            Vec::new()
        };

        result.config_changes = changes.len();

        let actions = self.reconciler.reconcile(&changes);

        for action in &actions {
            match action {
                Action::Resolve(pv_names) => {
                    result.resolved += self.resolve_batch(pv_names, pool).await;
                }
                Action::Unsubscribe(pv_names) => {
                    self.commands.unsubscribe_batch(pv_names);
                    result.unsubscribed += pv_names.len();
                }
                Action::Subscribe(entries) => {
                    for (pv, ioc) in entries {
                        self.commands.push(IngestCommand::subscribe(pv, *ioc));
                    }
                    result.subscribed += entries.len();
                }
                Action::UpdateFilter(pv_names) => {
                    result.filter_updates += pv_names.len();
                }
                Action::Alert(alert) => {
                    self.total_alerts += 1;
                    tracing::warn!("{alert}");
                    result.alerts += 1;
                }
            }
        }

        // Retry pending PVs that failed last time.
        if self.reconciler.pending_count() > 0 {
            let pending: Vec<String> = self
                .reconciler
                .pending_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            result.resolved += self.resolve_batch(&pending, pool).await;
        }

        Ok(result)
    }

    /// Flush pending commands to Redis.
    pub async fn flush_commands(
        &mut self,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> Result<usize, BoxError> {
        let jsons = self.commands.take();
        if jsons.is_empty() {
            return Ok(0);
        }

        let count = jsons.len();
        let mut pipe = redis::pipe();
        for json in &jsons {
            pipe.cmd("PUBLISH").arg(COMMAND_CHANNEL).arg(json).ignore();
        }
        pipe.query_async::<()>(conn).await?;

        tracing::debug!(count, "published commands to Redis");
        Ok(count)
    }

    /// Resolve a batch of PV names: publish subscribe commands.
    async fn resolve_batch(&mut self, pv_names: &[String], pool: &PgPool) -> usize {
        if pv_names.is_empty() {
            return 0;
        }

        let ioc_addr = self
            .name_servers
            .first()
            .copied()
            .unwrap_or_else(|| "0.0.0.0:5075".parse().unwrap());

        let to_subscribe = self.reconciler.mark_resolved(pv_names, ioc_addr);

        if !to_subscribe.is_empty() {
            let pvs: Vec<String> = to_subscribe.iter().map(|(pv, _)| pv.clone()).collect();
            let ioc_str = self.name_servers.first().map(|a| a.to_string());
            self.commands
                .push(IngestCommand::SubscribeBatch { pvs, ioc: ioc_str });
        }

        let resolved = to_subscribe.len();

        if let Some(ns) = self.name_servers.first() {
            let ns_str = ns.to_string();
            let names: Vec<&str> = pv_names.iter().map(|s| s.as_str()).collect();
            let _ = sqlx::query(
                "UPDATE pv_config SET expected_ioc = $1, updated_at = NOW() \
                 WHERE pv_name = ANY($2::text[])",
            )
            .bind(&ns_str)
            .bind(&names)
            .execute(pool)
            .await;
        }

        tracing::info!(resolved, total = pv_names.len(), "batch resolve complete");
        self.total_resolved += resolved as u64;
        resolved
    }

    /// Main run loop.
    pub async fn run(
        &mut self,
        pool: &PgPool,
        redis_conn: &mut redis::aio::MultiplexedConnection,
    ) -> Result<(), BoxError> {
        let startup_actions = self.startup(pool).await?;
        for action in &startup_actions {
            if let Action::Resolve(names) = action {
                self.resolve_batch(names, pool).await;
            }
        }
        self.flush_commands(redis_conn).await?;

        tracing::info!(
            resolved = self.reconciler.resolved_count(),
            pending = self.reconciler.pending_count(),
            not_found = self.reconciler.not_found_count(),
            "discover startup complete"
        );

        let poll_interval = Duration::from_secs(self.config.discover.config_poll_interval_s);
        let mut interval = tokio::time::interval(poll_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.cycle(pool).await {
                        Ok(result) if !result.is_empty() => {
                            tracing::info!("{result}");
                            self.flush_commands(redis_conn).await?;
                        }
                        Err(e) => tracing::error!("discover cycle error: {e}"),
                        _ => {}
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("discover shutdown");
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn resolved_count(&self) -> usize {
        self.reconciler.resolved_count()
    }

    pub fn pending_count(&self) -> usize {
        self.reconciler.pending_count()
    }

    pub fn not_found_count(&self) -> usize {
        self.reconciler.not_found_count()
    }

    pub fn tracked_count(&self) -> usize {
        self.poller.tracked_count()
    }

    pub fn poller(&self) -> &ConfigPoller {
        &self.poller
    }

    pub fn reconciler(&self) -> &Reconciler {
        &self.reconciler
    }

    pub fn commands(&self) -> &CommandBatch {
        &self.commands
    }
}

/// Result of one reconciliation cycle.
#[derive(Debug, Clone, Default)]
pub struct CycleResult {
    pub config_changes: usize,
    pub resolved: usize,
    pub subscribed: usize,
    pub unsubscribed: usize,
    pub filter_updates: usize,
    pub alerts: usize,
}

impl CycleResult {
    pub fn is_empty(&self) -> bool {
        self.config_changes == 0
            && self.resolved == 0
            && self.subscribed == 0
            && self.unsubscribed == 0
            && self.alerts == 0
    }
}

impl std::fmt::Display for CycleResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cycle: cfg={} res={} sub={} unsub={} alerts={}",
            self.config_changes, self.resolved, self.subscribed, self.unsubscribed, self.alerts
        )
    }
}

impl std::fmt::Display for Orchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Discover[{} tracked, {} resolved, {} pending, {} not_found, {} cycles]",
            self.poller.tracked_count(),
            self.reconciler.resolved_count(),
            self.reconciler.pending_count(),
            self.reconciler.not_found_count(),
            self.cycles
        )
    }
}

impl std::fmt::Debug for Orchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Orchestrator")
            .field("tracked", &self.poller.tracked_count())
            .field("resolved", &self.reconciler.resolved_count())
            .field("pending", &self.reconciler.pending_count())
            .field("not_found", &self.reconciler.not_found_count())
            .field("cycles", &self.cycles)
            .field("total_resolved", &self.total_resolved)
            .field("total_alerts", &self.total_alerts)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cycle_result_empty() {
        assert!(CycleResult::default().is_empty());
    }

    #[test]
    fn test_cycle_result_not_empty() {
        let r = CycleResult {
            resolved: 1,
            ..Default::default()
        };
        assert!(!r.is_empty());
    }

    #[test]
    fn test_cycle_result_display() {
        let r = CycleResult {
            config_changes: 3,
            resolved: 2,
            subscribed: 2,
            ..Default::default()
        };
        let s = r.to_string();
        assert!(s.contains("cfg=3"));
        assert!(s.contains("res=2"));
        assert!(s.contains("sub=2"));
    }
}
