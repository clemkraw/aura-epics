//! Incremental pv_config polling - O(delta), not O(N).
//!
//! Polls the `pv_config` table for changes since the last poll.
//! Uses the `updated_at` index for incremental queries.

use aura_core::pv::PvConfig;
use std::collections::{HashMap, HashSet};

/// A detected change in pv_config.
#[derive(Debug, Clone, PartialEq)]
pub enum PvChange {
    /// New PV added (or re-enabled).
    Added(PvConfig),
    /// PV removed (or disabled).
    Removed(String),
    /// PV config modified (epsilon, heartbeat, etc.).
    Modified(PvConfig),
}

impl std::fmt::Display for PvChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Added(c) => write!(f, "+{}", c.pv_name),
            Self::Removed(n) => write!(f, "-{n}"),
            Self::Modified(c) => write!(f, "~{}", c.pv_name),
        }
    }
}

/// Tracks the known PV set and computes diffs.
///
/// Keeps an in-memory snapshot of pv_config (names only for the fast path, full configs
/// for modification detection).
pub struct ConfigPoller {
    /// Current known configs: pv_name -> PvConfig.
    known: HashMap<String, PvConfig>,
}

impl ConfigPoller {
    pub fn new() -> Self {
        Self {
            known: HashMap::new(),
        }
    }

    /// Pre-populate with an initial snapshot (call once at startup).
    /// Returns the count of PVs loaded.
    pub fn load_initial(&mut self, configs: Vec<PvConfig>) -> usize {
        let count = configs.len();
        self.known.clear();
        self.known.reserve(count);
        for cfg in configs {
            if cfg.enabled {
                self.known.insert(cfg.pv_name.clone(), cfg);
            }
        }
        count
    }

    /// Compute changes from a fresh set of enabled configs.
    ///
    /// This is the core diff algorithm:
    /// 1. New names not in `known` -> `Added`
    /// 2. Known names not in `fresh` -> `Removed`
    /// 3. Names in both but config changed -> `Modified`
    pub fn diff(&mut self, fresh: Vec<PvConfig>) -> Vec<PvChange> {
        let fresh_map: HashMap<String, PvConfig> = fresh
            .into_iter()
            .filter(|c| c.enabled)
            .map(|c| (c.pv_name.clone(), c))
            .collect();

        let known_names: HashSet<&str> = self.known.keys().map(|s| s.as_str()).collect();
        let fresh_names: HashSet<&str> = fresh_map.keys().map(|s| s.as_str()).collect();

        let mut changes = Vec::new();

        for name in fresh_names.difference(&known_names) {
            if let Some(cfg) = fresh_map.get(*name) {
                changes.push(PvChange::Added(cfg.clone()));
            }
        }

        for name in known_names.difference(&fresh_names) {
            changes.push(PvChange::Removed(name.to_string()));
        }

        for name in known_names.intersection(&fresh_names) {
            let old = &self.known[*name];
            let new = &fresh_map[*name];
            if config_changed(old, new) {
                changes.push(PvChange::Modified(new.clone()));
            }
        }

        // Apply changes to known state.
        for change in &changes {
            match change {
                PvChange::Added(c) | PvChange::Modified(c) => {
                    self.known.insert(c.pv_name.clone(), c.clone());
                }
                PvChange::Removed(name) => {
                    self.known.remove(name.as_str());
                }
            }
        }

        changes
    }

    /// Number of currently tracked PVs.
    pub fn tracked_count(&self) -> usize {
        self.known.len()
    }
}

impl Default for ConfigPoller {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if the archiving-relevant fields changed.
/// Ignores updated_at (always changes) and created_at (immutable).
#[inline]
fn config_changed(old: &PvConfig, new: &PvConfig) -> bool {
    old.epsilon != new.epsilon
        || old.heartbeat_s != new.heartbeat_s
        || old.enabled != new.enabled
        || old.expected_ioc != new.expected_ioc
}

impl std::fmt::Display for ConfigPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConfigPoller[{} tracked]", self.known.len())
    }
}

impl std::fmt::Debug for ConfigPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigPoller")
            .field("tracked", &self.known.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pv(name: &str) -> PvConfig {
        PvConfig::new(name)
    }

    fn pv_eps(name: &str, eps: f64) -> PvConfig {
        PvConfig::new(name).with_epsilon(eps)
    }

    fn pv_disabled(name: &str) -> PvConfig {
        PvConfig::new(name).with_enabled(false)
    }

    fn pv_hb(name: &str, hb: f64) -> PvConfig {
        PvConfig::new(name).with_heartbeat(hb)
    }

    fn pv_ioc(name: &str, ioc: &str) -> PvConfig {
        PvConfig::new(name).with_expected_ioc(ioc)
    }

    #[test]
    fn test_new() {
        assert_eq!(ConfigPoller::new().tracked_count(), 0);
    }

    #[test]
    fn test_default() {
        assert_eq!(ConfigPoller::default().tracked_count(), 0);
    }

    #[test]
    fn test_load_initial() {
        let mut p = ConfigPoller::new();
        assert_eq!(p.load_initial(vec![pv("A"), pv("B"), pv("C")]), 3);
        assert_eq!(p.tracked_count(), 3);
    }

    #[test]
    fn test_load_initial_filters_disabled() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A"), pv_disabled("B"), pv("C")]);
        assert_eq!(p.tracked_count(), 2);
    }

    #[test]
    fn test_load_initial_replaces() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A"), pv("B")]);
        p.load_initial(vec![pv("C")]);
        assert_eq!(p.tracked_count(), 1);
    }

    #[test]
    fn test_diff_add() {
        let mut p = ConfigPoller::new();
        let changes = p.diff(vec![pv("A")]);
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], PvChange::Added(c) if c.pv_name == "A"));
        assert_eq!(p.tracked_count(), 1);
    }

    #[test]
    fn test_diff_add_many() {
        let mut p = ConfigPoller::new();
        let changes = p.diff(vec![pv("A"), pv("B"), pv("C")]);
        assert_eq!(changes.len(), 3);
        assert!(changes.iter().all(|c| matches!(c, PvChange::Added(_))));
    }

    #[test]
    fn test_diff_remove() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A"), pv("B")]);
        let changes = p.diff(vec![pv("A")]);
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], PvChange::Removed(n) if n == "B"));
        assert_eq!(p.tracked_count(), 1);
    }

    #[test]
    fn test_diff_remove_all() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A"), pv("B")]);
        let changes = p.diff(vec![]);
        assert_eq!(changes.len(), 2);
        assert_eq!(p.tracked_count(), 0);
    }

    #[test]
    fn test_diff_modify_epsilon() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A")]);
        let changes = p.diff(vec![pv_eps("A", 0.5)]);
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], PvChange::Modified(_)));
    }

    #[test]
    fn test_diff_modify_heartbeat() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A")]);
        assert_eq!(p.diff(vec![pv_hb("A", 30.0)]).len(), 1);
    }

    #[test]
    fn test_diff_modify_ioc() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A")]);
        assert_eq!(p.diff(vec![pv_ioc("A", "10.0.1.5")]).len(), 1);
    }

    #[test]
    fn test_diff_no_change() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A"), pv("B")]);
        assert!(p.diff(vec![pv("A"), pv("B")]).is_empty());
    }

    #[test]
    fn test_diff_mixed() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A"), pv("B"), pv("C")]);
        let changes = p.diff(vec![pv("A"), pv_eps("C", 0.1), pv("D")]);
        assert_eq!(
            changes
                .iter()
                .filter(|c| matches!(c, PvChange::Added(_)))
                .count(),
            1
        );
        assert_eq!(
            changes
                .iter()
                .filter(|c| matches!(c, PvChange::Removed(_)))
                .count(),
            1
        );
        assert_eq!(
            changes
                .iter()
                .filter(|c| matches!(c, PvChange::Modified(_)))
                .count(),
            1
        );
        assert_eq!(p.tracked_count(), 3);
    }

    #[test]
    fn test_diff_disabled_treated_as_removed() {
        let mut p = ConfigPoller::new();
        p.load_initial(vec![pv("A")]);
        let changes = p.diff(vec![pv_disabled("A")]);
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], PvChange::Removed(_)));
    }

    #[test]
    fn test_diff_disabled_not_added() {
        let mut p = ConfigPoller::new();
        assert!(p.diff(vec![pv_disabled("A")]).is_empty());
    }

    #[test]
    fn test_consecutive_no_change() {
        let mut p = ConfigPoller::new();
        p.diff(vec![pv("A"), pv("B")]);
        assert!(p.diff(vec![pv("A"), pv("B")]).is_empty());
        assert!(p.diff(vec![pv("A"), pv("B")]).is_empty());
    }

    #[test]
    fn test_change_display() {
        assert_eq!(PvChange::Added(pv("A")).to_string(), "+A");
        assert_eq!(PvChange::Removed("B".into()).to_string(), "-B");
        assert_eq!(PvChange::Modified(pv("C")).to_string(), "~C");
    }

    #[test]
    fn test_display() {
        assert!(ConfigPoller::new().to_string().contains("ConfigPoller"));
    }

    #[test]
    fn test_debug() {
        assert!(format!("{:?}", ConfigPoller::new()).contains("ConfigPoller"));
    }

    #[test]
    fn test_100k_pvs_diff_perf() {
        let mut p = ConfigPoller::new();
        let initial: Vec<PvConfig> = (0..100_000).map(|i| pv(&format!("PV:{i}"))).collect();
        p.load_initial(initial);
        assert_eq!(p.tracked_count(), 100_000);
        let same: Vec<PvConfig> = (0..100_000).map(|i| pv(&format!("PV:{i}"))).collect();
        let t = std::time::Instant::now();
        let changes = p.diff(same);
        let elapsed = t.elapsed();
        assert!(changes.is_empty());
        // Threshold generous for debug mode; catches O(N²) regressions (>10s).
        assert!(
            elapsed.as_millis() < 2000,
            "diff took {}ms",
            elapsed.as_millis()
        );
    }
}
