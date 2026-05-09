//! Filter engine — manages per-PV filter instances.
//!
//! The [`FilterEngine`] is the main entry point for the filtering
//! stage of the pipeline. It maintains a registry of [`PvFilter`]
//! instances (one per PV) and routes incoming [`PvUpdate`]s to the
//! correct filter.
//!
//! Filters are created lazily on the first sample for each PV.
//! Configuration can be updated at runtime via [`update_pv_config`].
//!
//! ## Hot path
//!
//! `process()` is called for every sample.
//! It avoids allocations on the common path (filter already exists)
//! by doing a `get_mut` lookup first, and only cloning the PV name
//! on the cold path (first sample for a new PV).
//!
//! ```text
//! consumer.read_batch()
//!     ↓
//! for update in batch {
//!     let decision = engine.process(&update);
//!     if decision.is_stored() { writer.push(update); }
//! }
//! ```

use std::collections::HashMap;

use aura_core::pv::PvConfig;
use aura_core::sample::{FilterDecision, StoreReason};
use aura_core::PvUpdate;

pub mod calibrator;
pub mod pv_filter;
pub mod stats;

use pv_filter::{PvFilter, PvFilterConfig};

/// Central filter engine — one per `aura-store` instance.
///
/// Routes each [`PvUpdate`] to its per-PV filter and returns
/// the [`FilterDecision`].
pub struct FilterEngine {
    /// Per-PV filter instances, keyed by PV name.
    filters: HashMap<String, PvFilter>,
    /// Default filter config for PVs without explicit configuration.
    default_config: PvFilterConfig,
    /// PV-specific overrides from pv_config table.
    pv_configs: HashMap<String, PvFilterConfig>,
    /// Total samples processed.
    total_received: u64,
    /// Total samples stored.
    total_stored: u64,
}

impl FilterEngine {
    /// Create a new filter engine with the given default configuration.
    ///
    /// PVs without explicit configuration will use this default.
    pub fn new(default_config: PvFilterConfig) -> Self {
        Self {
            filters: HashMap::new(),
            default_config,
            pv_configs: HashMap::new(),
            total_received: 0,
            total_stored: 0,
        }
    }

    /// Create a filter engine with default settings
    /// (auto-calibrate, 60s heartbeat).
    pub fn with_defaults() -> Self {
        Self::new(PvFilterConfig::default())
    }

    /// Create a filter engine pre-sized for a known number of PVs.
    ///
    /// Avoids HashMap re-allocations during the initial subscription burst.
    pub fn with_capacity(default_config: PvFilterConfig, expected_pvs: usize) -> Self {
        Self {
            filters: HashMap::with_capacity(expected_pvs),
            default_config,
            pv_configs: HashMap::new(),
            total_received: 0,
            total_stored: 0,
        }
    }

    /// Process a PV update through its per-PV filter.
    ///
    /// **Hot path** — called for every sample.
    ///
    /// Uses a two-phase lookup to avoid cloning the PV name on the common path (filter already exists):
    /// - Phase 1: `get_mut`
    /// - Phase 2 (cold): `insert` with cloned key (only on first sample per PV)
    #[inline]
    pub fn process(&mut self, update: &PvUpdate) -> FilterDecision {
        self.total_received += 1;

        // Fast path: filter already exists (99.99% of calls).
        if let Some(filter) = self.filters.get_mut(&update.pv_name) {
            let decision = filter.decide(update);
            if decision.is_stored() {
                self.total_stored += 1;
            }
            return decision;
        }

        // Cold path: first sample for this PV — create filter.
        self.process_cold(update)
    }

    /// Process a batch of updates efficiently.
    ///
    /// Returns the number of samples that were stored (passed the filter).
    /// More efficient than calling `process()` in a loop when you don't
    /// need per-sample decisions (the common case in `pipeline.rs`).
    pub fn process_batch<'a>(
        &mut self,
        updates: impl IntoIterator<Item = &'a PvUpdate>,
        stored: &mut Vec<(usize, StoreReason)>,
    ) -> usize {
        stored.clear();
        let mut count = 0;

        for (idx, update) in updates.into_iter().enumerate() {
            let decision = self.process(update);
            if let FilterDecision::Store(reason) = decision {
                stored.push((idx, reason));
                count += 1;
            }
        }

        count
    }

    /// Load PV-specific configurations (from pv_config table).
    ///
    /// Called after polling pv_config. Updates existing filters
    /// and stores config for future PVs not yet seen.
    pub fn load_pv_configs(&mut self, configs: &[PvConfig]) {
        for pv_config in configs {
            self.apply_pv_config(pv_config);
        }
    }

    /// Update a single PV's configuration at runtime.
    ///
    /// If the filter already exists, updates epsilon and heartbeat
    /// immediately. Otherwise, stores the config for when the first
    /// sample arrives.
    pub fn update_pv_config(&mut self, pv_config: &PvConfig) {
        self.apply_pv_config(pv_config);
    }

    /// Remove a PV's filter and config (e.g., PV removed from config).
    pub fn remove_pv(&mut self, pv_name: &str) {
        self.filters.remove(pv_name);
        self.pv_configs.remove(pv_name);
    }

    /// Get a reference to a PV's filter (for metrics/inspection).
    pub fn get_filter(&self, pv_name: &str) -> Option<&PvFilter> {
        self.filters.get(pv_name)
    }

    /// Check whether a filter exists for the given PV.
    #[inline]
    pub fn has_filter(&self, pv_name: &str) -> bool {
        self.filters.contains_key(pv_name)
    }

    /// Number of active PV filters.
    #[inline]
    pub fn pv_count(&self) -> usize {
        self.filters.len()
    }

    /// Number of PV-specific configs loaded (may include PVs not yet seen).
    #[inline]
    pub fn config_count(&self) -> usize {
        self.pv_configs.len()
    }

    /// Total samples received across all PVs.
    #[inline]
    pub fn total_received(&self) -> u64 {
        self.total_received
    }

    /// Total samples stored across all PVs.
    #[inline]
    pub fn total_stored(&self) -> u64 {
        self.total_stored
    }

    /// Total samples dropped across all PVs.
    #[inline]
    pub fn total_dropped(&self) -> u64 {
        self.total_received - self.total_stored
    }

    /// Global compression ratio (stored / received).
    /// Returns 1.0 if no samples received.
    pub fn compression_ratio(&self) -> f64 {
        if self.total_received == 0 {
            return 1.0;
        }
        self.total_stored as f64 / self.total_received as f64
    }

    /// Iterate over all active filters (for Prometheus metrics export).
    pub fn iter_filters(&self) -> impl Iterator<Item = (&str, &PvFilter)> {
        self.filters.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Reset all counters and remove all filters.
    pub fn reset(&mut self) {
        self.filters.clear();
        self.pv_configs.clear();
        self.total_received = 0;
        self.total_stored = 0;
    }
    
    /// Cold path: create a new filter and process the first sample.
    #[cold]
    fn process_cold(&mut self, update: &PvUpdate) -> FilterDecision {
        let config = self.pv_configs
            .get(&update.pv_name)
            .cloned()
            .unwrap_or_else(|| self.default_config.clone());

        let mut filter = PvFilter::new(&update.pv_name, config);
        let decision = filter.decide(update);

        if decision.is_stored() {
            self.total_stored += 1;
        }

        self.filters.insert(update.pv_name.clone(), filter);
        decision
    }

    /// Apply a PvConfig to the internal state.
    fn apply_pv_config(&mut self, pv_config: &PvConfig) {
        let filter_config = pv_config_to_filter_config(pv_config, &self.default_config);
        self.pv_configs.insert(pv_config.pv_name.clone(), filter_config.clone());

        // Update existing filter if it already exists.
        if let Some(filter) = self.filters.get_mut(&pv_config.pv_name) {
            filter.update_epsilon(filter_config.epsilon);
            let hb_s = filter_config.heartbeat.num_milliseconds() as f64 / 1000.0;
            filter.update_heartbeat(hb_s);
        }
    }
}

/// Convert a PvConfig (from DB) to a PvFilterConfig.
fn pv_config_to_filter_config(
    pv: &PvConfig,
    defaults: &PvFilterConfig,
) -> PvFilterConfig {
    match pv.epsilon {
        Some(eps) => PvFilterConfig::fixed(eps, pv.heartbeat_s),
        None => PvFilterConfig::auto(pv.heartbeat_s, defaults.calibrator.clone()),
    }
}

impl std::fmt::Debug for FilterEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterEngine")
            .field("pv_count", &self.pv_count())
            .field("config_count", &self.config_count())
            .field("total_received", &self.total_received)
            .field("total_stored", &self.total_stored)
            .field("compression", &format!("{:.1}%", self.compression_ratio() * 100.0))
            .finish()
    }
}

impl std::fmt::Display for FilterEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f, "FilterEngine: {} PVs, recv={} stored={} ({:.1}%)",
            self.pv_count(), self.total_received, self.total_stored,
            self.compression_ratio() * 100.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aura_core::pva::*;

    fn ts(secs: i64) -> TimeStamp { TimeStamp::new(secs, 0) }

    fn scalar(pv: &str, value: f64, time_s: i64) -> PvUpdate {
        PvUpdate::new(pv, NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(value),
            alarm: Alarm::default(),
            timestamp: ts(time_s),
            display: None, control: None, value_alarm: None,
        }))
    }

    fn default_engine() -> FilterEngine {
        FilterEngine::new(PvFilterConfig::fixed(0.5, 60.0))
    }

    // ── Construction ─────────────────────────────────────────────────

    #[test]
    fn test_new() {
        let e = default_engine();
        assert_eq!(e.pv_count(), 0);
        assert_eq!(e.config_count(), 0);
        assert_eq!(e.total_received(), 0);
        assert_eq!(e.total_stored(), 0);
        assert_eq!(e.total_dropped(), 0);
        assert_eq!(e.compression_ratio(), 1.0);
    }

    #[test]
    fn test_with_defaults() {
        let e = FilterEngine::with_defaults();
        assert_eq!(e.pv_count(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let e = FilterEngine::with_capacity(PvFilterConfig::fixed(0.1, 60.0), 1000);
        assert_eq!(e.pv_count(), 0);
        // capacity is internal, just verify it doesn't panic
    }

    // ── Lazy filter creation ─────────────────────────────────────────

    #[test]
    fn test_creates_filter_on_first_sample() {
        let mut e = default_engine();
        assert!(!e.has_filter("CRYO:TEMP"));
        assert!(e.get_filter("CRYO:TEMP").is_none());

        e.process(&scalar("CRYO:TEMP", 4.2, 1000));

        assert!(e.has_filter("CRYO:TEMP"));
        assert!(e.get_filter("CRYO:TEMP").is_some());
        assert_eq!(e.pv_count(), 1);
    }

    #[test]
    fn test_multiple_pvs() {
        let mut e = default_engine();

        e.process(&scalar("PV:A", 1.0, 1000));
        e.process(&scalar("PV:B", 2.0, 1000));
        e.process(&scalar("PV:C", 3.0, 1000));

        assert_eq!(e.pv_count(), 3);
        assert!(e.has_filter("PV:A"));
        assert!(e.has_filter("PV:B"));
        assert!(e.has_filter("PV:C"));
        assert!(!e.has_filter("PV:D"));
    }

    #[test]
    fn test_second_sample_uses_fast_path() {
        let mut e = default_engine();

        // First sample → cold path (creates filter)
        let d1 = e.process(&scalar("PV:A", 10.0, 1000));
        assert_eq!(d1, FilterDecision::Store(StoreReason::Initial));

        // Second sample → fast path (filter exists, no allocation)
        let d2 = e.process(&scalar("PV:A", 10.0, 1001));
        assert_eq!(d2, FilterDecision::Drop);
    }

    // ── Filter routing ───────────────────────────────────────────────

    #[test]
    fn test_routes_to_correct_pv() {
        let mut e = default_engine();

        e.process(&scalar("PV:A", 10.0, 1000));
        e.process(&scalar("PV:B", 20.0, 1000));

        // Small change on PV:A (< ε=0.5) → drop
        let d = e.process(&scalar("PV:A", 10.1, 1001));
        assert_eq!(d, FilterDecision::Drop);

        // Big change on PV:B (> ε=0.5) → store
        let d = e.process(&scalar("PV:B", 21.0, 1001));
        assert_eq!(d, FilterDecision::Store(StoreReason::EpsilonExceeded));
    }

    #[test]
    fn test_independent_pv_state() {
        let mut e = default_engine();

        e.process(&scalar("PV:A", 10.0, 1000));
        e.process(&scalar("PV:B", 100.0, 1000));

        // PV:A epsilon stores independently of PV:B
        e.process(&scalar("PV:A", 20.0, 1001)); // big change → stored
        assert_eq!(e.get_filter("PV:A").unwrap().stored(), 2);
        assert_eq!(e.get_filter("PV:B").unwrap().stored(), 1); // untouched
    }

    // ── Global counters ──────────────────────────────────────────────

    #[test]
    fn test_global_counters() {
        let mut e = default_engine();

        e.process(&scalar("PV:A", 10.0, 1000)); // stored (initial)
        e.process(&scalar("PV:A", 10.0, 1001)); // dropped
        e.process(&scalar("PV:A", 10.0, 1002)); // dropped
        e.process(&scalar("PV:A", 20.0, 1003)); // stored (epsilon)

        assert_eq!(e.total_received(), 4);
        assert_eq!(e.total_stored(), 2);
        assert_eq!(e.total_dropped(), 2);
        assert!((e.compression_ratio() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_global_counters_multi_pv() {
        let mut e = default_engine();

        e.process(&scalar("PV:A", 10.0, 1000)); // stored
        e.process(&scalar("PV:B", 20.0, 1000)); // stored
        e.process(&scalar("PV:A", 10.0, 1001)); // dropped
        e.process(&scalar("PV:B", 20.0, 1001)); // dropped

        assert_eq!(e.total_received(), 4);
        assert_eq!(e.total_stored(), 2); // 2 initials
        assert_eq!(e.total_dropped(), 2);
    }

    // ── Batch processing ─────────────────────────────────────────────

    #[test]
    fn test_process_batch() {
        let mut e = default_engine();
        let updates = vec![
            scalar("PV:A", 10.0, 1000),  // stored (initial)
            scalar("PV:A", 10.0, 1001),  // dropped
            scalar("PV:A", 20.0, 1002),  // stored (epsilon)
            scalar("PV:B", 5.0, 1000),   // stored (initial)
        ];

        let mut stored = Vec::new();
        let count = e.process_batch(&updates, &mut stored);

        assert_eq!(count, 3); // 2 initials + 1 epsilon
        assert_eq!(stored.len(), 3);

        // Check indices
        assert_eq!(stored[0].0, 0); // first update stored
        assert_eq!(stored[0].1, StoreReason::Initial);
        assert_eq!(stored[1].0, 2); // third update stored
        assert_eq!(stored[1].1, StoreReason::EpsilonExceeded);
        assert_eq!(stored[2].0, 3); // fourth update stored
        assert_eq!(stored[2].1, StoreReason::Initial);
    }

    #[test]
    fn test_process_batch_empty() {
        let mut e = default_engine();
        let updates: Vec<PvUpdate> = vec![];
        let mut stored = Vec::new();
        let count = e.process_batch(&updates, &mut stored);

        assert_eq!(count, 0);
        assert!(stored.is_empty());
    }

    #[test]
    fn test_process_batch_all_dropped() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 10.0, 1000)); // prime the filter

        let updates = vec![
            scalar("PV:A", 10.0, 1001), // dropped
            scalar("PV:A", 10.0, 1002), // dropped
            scalar("PV:A", 10.0, 1003), // dropped
        ];

        let mut stored = Vec::new();
        let count = e.process_batch(&updates, &mut stored);

        assert_eq!(count, 0);
        assert!(stored.is_empty());
    }

    #[test]
    fn test_process_batch_reuses_vec() {
        let mut e = default_engine();
        let mut stored = Vec::new();

        // First batch
        let b1 = vec![scalar("PV:A", 10.0, 1000)];
        e.process_batch(&b1, &mut stored);
        assert_eq!(stored.len(), 1);

        // Second batch — stored vec is cleared and reused
        let b2 = vec![scalar("PV:B", 20.0, 1000)];
        e.process_batch(&b2, &mut stored);
        assert_eq!(stored.len(), 1); // not 2 — cleared first
    }

    // ── PV config loading ────────────────────────────────────────────

    #[test]
    fn test_load_pv_configs() {
        let mut e = default_engine();

        let configs = vec![
            PvConfig::new("PV:A").with_epsilon(0.01).with_heartbeat(30.0),
        ];
        e.load_pv_configs(&configs);
        assert_eq!(e.config_count(), 1);

        // First sample creates filter with loaded config
        e.process(&scalar("PV:A", 10.0, 1000));
        assert_eq!(e.get_filter("PV:A").unwrap().effective_epsilon(), 0.01);
    }

    #[test]
    fn test_load_pv_configs_updates_existing() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 10.0, 1000)); // creates with default ε=0.5

        assert_eq!(e.get_filter("PV:A").unwrap().effective_epsilon(), 0.5);

        let configs = vec![
            PvConfig::new("PV:A").with_epsilon(0.001),
        ];
        e.load_pv_configs(&configs);

        assert_eq!(e.get_filter("PV:A").unwrap().effective_epsilon(), 0.001);
    }

    #[test]
    fn test_load_pv_configs_multiple() {
        let mut e = default_engine();

        let configs = vec![
            PvConfig::new("PV:A").with_epsilon(0.01),
            PvConfig::new("PV:B").with_epsilon(0.02),
            PvConfig::new("PV:C").with_epsilon(0.03),
        ];
        e.load_pv_configs(&configs);
        assert_eq!(e.config_count(), 3);

        e.process(&scalar("PV:B", 1.0, 1000));
        assert_eq!(e.get_filter("PV:B").unwrap().effective_epsilon(), 0.02);
    }

    #[test]
    fn test_load_auto_epsilon_config() {
        let mut e = default_engine();

        let configs = vec![PvConfig::new("PV:AUTO")];
        e.load_pv_configs(&configs);

        e.process(&scalar("PV:AUTO", 10.0, 1000));
        assert!(!e.get_filter("PV:AUTO").unwrap().is_calibrated());
    }

    // ── Single PV config update ──────────────────────────────────────

    #[test]
    fn test_update_pv_config() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 10.0, 1000));

        e.update_pv_config(&PvConfig::new("PV:A").with_epsilon(0.99));
        assert_eq!(e.get_filter("PV:A").unwrap().effective_epsilon(), 0.99);
    }

    #[test]
    fn test_update_config_before_first_sample() {
        let mut e = default_engine();
        e.update_pv_config(&PvConfig::new("PV:NEW").with_epsilon(0.77));
        assert_eq!(e.config_count(), 1);

        // Config stored, filter not yet created
        assert!(!e.has_filter("PV:NEW"));

        // First sample picks up the pre-configured epsilon
        e.process(&scalar("PV:NEW", 10.0, 1000));
        assert_eq!(e.get_filter("PV:NEW").unwrap().effective_epsilon(), 0.77);
    }

    // ── Remove PV ────────────────────────────────────────────────────

    #[test]
    fn test_remove_pv() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 10.0, 1000));
        assert_eq!(e.pv_count(), 1);

        e.remove_pv("PV:A");
        assert_eq!(e.pv_count(), 0);
        assert!(!e.has_filter("PV:A"));
    }

    #[test]
    fn test_remove_pv_clears_config() {
        let mut e = default_engine();
        e.update_pv_config(&PvConfig::new("PV:A").with_epsilon(0.01));
        e.process(&scalar("PV:A", 10.0, 1000));

        e.remove_pv("PV:A");
        assert_eq!(e.config_count(), 0);

        // Re-create uses default config, not the removed one
        e.process(&scalar("PV:A", 10.0, 2000));
        assert_eq!(e.get_filter("PV:A").unwrap().effective_epsilon(), 0.5); // default
    }

    #[test]
    fn test_remove_nonexistent_pv() {
        let mut e = default_engine();
        e.remove_pv("PV:GHOST"); // should not panic
        assert_eq!(e.pv_count(), 0);
    }

    // ── Reset ────────────────────────────────────────────────────────

    #[test]
    fn test_reset() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 10.0, 1000));
        e.process(&scalar("PV:B", 20.0, 1000));
        e.update_pv_config(&PvConfig::new("PV:C").with_epsilon(0.01));

        e.reset();

        assert_eq!(e.pv_count(), 0);
        assert_eq!(e.config_count(), 0);
        assert_eq!(e.total_received(), 0);
        assert_eq!(e.total_stored(), 0);
    }

    // ── Iter filters ─────────────────────────────────────────────────

    #[test]
    fn test_iter_filters() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 1.0, 1000));
        e.process(&scalar("PV:B", 2.0, 1000));

        let mut names: Vec<&str> = e.iter_filters().map(|(name, _)| name).collect();
        names.sort();
        assert_eq!(names, vec!["PV:A", "PV:B"]);
    }

    #[test]
    fn test_iter_filters_empty() {
        let e = default_engine();
        assert_eq!(e.iter_filters().count(), 0);
    }

    #[test]
    fn test_iter_filters_with_metrics() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 10.0, 1000));
        e.process(&scalar("PV:A", 10.0, 1001)); // dropped

        for (name, filter) in e.iter_filters() {
            assert_eq!(name, "PV:A");
            assert_eq!(filter.received(), 2);
            assert_eq!(filter.stored(), 1);
        }
    }

    // ── Per-PV filter inspection ─────────────────────────────────────

    #[test]
    fn test_get_filter_metrics() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 10.0, 1000)); // stored
        e.process(&scalar("PV:A", 10.0, 1001)); // dropped

        let f = e.get_filter("PV:A").unwrap();
        assert_eq!(f.received(), 2);
        assert_eq!(f.stored(), 1);
        assert_eq!(f.dropped(), 1);
        assert_eq!(f.pv_name(), "PV:A");
    }

    #[test]
    fn test_get_filter_nonexistent() {
        let e = default_engine();
        assert!(e.get_filter("PV:NOPE").is_none());
    }

    // ── Display / Debug ──────────────────────────────────────────────

    #[test]
    fn test_display() {
        let mut e = default_engine();
        e.process(&scalar("PV:A", 1.0, 1000));
        let s = e.to_string();
        assert!(s.contains("1 PVs"));
        assert!(s.contains("recv=1"));
        assert!(s.contains("stored=1"));
    }

    #[test]
    fn test_display_empty() {
        let e = default_engine();
        let s = e.to_string();
        assert!(s.contains("0 PVs"));
        assert!(s.contains("recv=0"));
    }

    #[test]
    fn test_debug() {
        let e = default_engine();
        let d = format!("{:?}", e);
        assert!(d.contains("FilterEngine"));
        assert!(d.contains("pv_count"));
        assert!(d.contains("config_count"));
        assert!(d.contains("total_received"));
    }
}