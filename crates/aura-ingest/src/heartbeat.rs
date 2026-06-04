//! Per-PV heartbeat tracking - forces periodic stores for stable PVs.
//!
//! When a PV's value doesn't change, the IOC sends no monitor update (MDEL deadband).
//! Without heartbeat, the archiver has no data for that PV during stable periods - indistinguishable from a crash.
//!
//! Each PV can have its own `heartbeat_s`:
//! - `pv_config.heartbeat_s = 60.0` -> per-PV override
//! - `pv_config.heartbeat_s = NULL` -> uses global default from aura.toml
//! - `pv_config.heartbeat_s = 0.0` -> disabled for this PV

use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use aura_core::sample::StoreReason;
use aura_store::writer::scalar::ScalarRow;
use aura_store::writer::shared_buf::SharedBuffer;

/// Scan interval - how often `emit_heartbeats` should be called.
pub const SCAN_INTERVAL_SECS: u64 = 10;

/// Safety cap on pv_id to prevent unbounded Vec growth.
const MAX_PV_ID: usize = 2_000_000;

/// Per-PV heartbeat state.
///
/// 32 bytes per entry. 250k PVs per shard = 8 MB.
#[derive(Clone)]
struct HeartbeatEntry {
    last_value: f64,
    last_severity: i16,
    last_status: i16,
    /// Per-PV heartbeat interval. 0.0 = disabled for this PV.
    heartbeat_s: f32,
    /// Last time this PV stored a sample (wire event or heartbeat).
    last_store_at: Instant,
}

impl HeartbeatEntry {
    fn new_inactive() -> Self {
        Self {
            last_value: 0.0,
            last_severity: 0,
            last_status: 0,
            heartbeat_s: 0.0,
            last_store_at: Instant::now(),
        }
    }
}

/// Per-PV heartbeat tracker with configurable intervals.
pub struct HeartbeatTracker {
    entries: Vec<HeartbeatEntry>,
    /// Global default from aura.toml. Used when per-PV is not set.
    default_heartbeat_s: f32,
    /// Shared per-PV heartbeat config, updated by main thread from pv_config.
    config_source: Arc<ArcSwap<Vec<(i32, f32)>>>,
    /// Pointer to last-seen config - cheap change detection via Arc::as_ptr.
    config_ptr: usize,
}

impl HeartbeatTracker {
    /// Create a tracker. `default_heartbeat_s = 0.0` disables globally.
    pub fn new(default_heartbeat_s: f64, config_source: Arc<ArcSwap<Vec<(i32, f32)>>>) -> Self {
        Self {
            entries: Vec::new(),
            default_heartbeat_s: default_heartbeat_s as f32,
            config_source,
            config_ptr: 0,
        }
    }

    /// Whether heartbeat is globally disabled (default = 0.0).
    #[inline]
    pub fn is_disabled(&self) -> bool {
        self.default_heartbeat_s <= 0.0
    }

    /// Override heartbeat interval for a specific PV.
    ///
    /// `heartbeat_s = 0.0` disables heartbeat for this PV.
    pub fn set_pv_heartbeat(&mut self, pv_id: i32, heartbeat_s: f64) {
        let idx = pv_id as usize;
        if idx > MAX_PV_ID {
            return;
        }
        if idx >= self.entries.len() {
            self.entries.resize(idx + 1, HeartbeatEntry::new_inactive());
        }
        self.entries[idx].heartbeat_s = heartbeat_s as f32;
    }

    /// Record a scalar store on the hot path.
    #[inline]
    pub fn record_store(
        &mut self,
        pv_id: i32,
        value: f64,
        severity: i16,
        status: i16,
        now: Instant,
    ) {
        let idx = pv_id as usize;
        if idx > MAX_PV_ID {
            return;
        }
        if idx >= self.entries.len() {
            self.entries.resize(idx + 1, HeartbeatEntry::new_inactive());
        }
        let e = &mut self.entries[idx];
        e.last_value = value;
        e.last_severity = severity;
        e.last_status = status;
        e.last_store_at = now;
        if e.heartbeat_s <= 0.0 && self.default_heartbeat_s > 0.0 {
            e.heartbeat_s = self.default_heartbeat_s;
        }
    }

    /// Scan all PVs and emit heartbeats for stale ones.
    ///
    /// `active_pv_ids` is the set of currently subscribed pv_ids - entries not in
    /// this set are auto-disabled (PV was unsubscribed).
    pub fn emit_heartbeats(
        &mut self,
        shared_buf: &SharedBuffer,
        active_pv_ids: &std::collections::HashSet<i32>,
    ) -> usize {
        if self.is_disabled() {
            return 0;
        }

        // Hot-reload per-PV heartbeat_s from pv_config (via ArcSwap).
        {
            let cfg = self.config_source.load();
            let ptr = Arc::as_ptr(&*cfg) as usize;
            if ptr != self.config_ptr {
                for &(pv_id, hs) in cfg.iter() {
                    self.set_pv_heartbeat(pv_id, hs as f64);
                }
                self.config_ptr = ptr;
            }
        }

        let now = Instant::now();
        let now_pg_us = {
            let utc = chrono::Utc::now();
            utc.timestamp_micros() - aura_store::writer::copy_pool::PG_EPOCH_OFFSET_US
        };

        let mut count = 0;
        for (pv_id, entry) in self.entries.iter_mut().enumerate() {
            if entry.heartbeat_s <= 0.0 {
                continue;
            }

            if !active_pv_ids.contains(&(pv_id as i32)) {
                entry.heartbeat_s = 0.0;
                continue;
            }

            let elapsed = now.duration_since(entry.last_store_at).as_secs_f32();
            if elapsed < entry.heartbeat_s {
                continue;
            }

            shared_buf.push_scalar(ScalarRow::from_parts(
                now_pg_us,
                pv_id as i32,
                entry.last_value,
                entry.last_severity,
                entry.last_status,
                StoreReason::Heartbeat,
            ));
            entry.last_store_at = now;
            count += 1;
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn active(ids: &[i32]) -> std::collections::HashSet<i32> {
        ids.iter().copied().collect()
    }

    fn make_buf() -> Arc<SharedBuffer> {
        Arc::new(SharedBuffer::new(
            1000,
            10_000,
            Arc::new(tokio::sync::Notify::new()),
        ))
    }

    fn empty_config() -> Arc<ArcSwap<Vec<(i32, f32)>>> {
        Arc::new(ArcSwap::from_pointee(Vec::new()))
    }

    /// Helper: set last_store_at to 1 second in the past for a PV.
    fn age_pv(t: &mut HeartbeatTracker, pv_id: i32) {
        let idx = pv_id as usize;
        if idx < t.entries.len() {
            t.entries[idx].last_store_at = Instant::now() - Duration::from_secs(1);
        }
    }

    #[test]
    fn disabled_does_nothing() {
        let mut t = HeartbeatTracker::new(0.0, empty_config());
        assert!(t.is_disabled());
        t.record_store(1, 3.14, 0, 0, Instant::now());
        assert_eq!(t.emit_heartbeats(&make_buf(), &active(&[])), 0);
    }

    #[test]
    fn no_heartbeat_when_fresh() {
        let mut t = HeartbeatTracker::new(60.0, empty_config());
        t.record_store(1, 3.14, 0, 0, Instant::now());
        assert_eq!(t.emit_heartbeats(&make_buf(), &active(&[1])), 0);
    }

    #[test]
    fn heartbeat_after_timeout() {
        let mut t = HeartbeatTracker::new(0.5, empty_config());
        let past = Instant::now() - Duration::from_secs(1);
        t.record_store(1, 4.2, 0, 0, past);
        let buf = make_buf();
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1])), 1);
        let rows = buf.take_scalars();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pv_id, 1);
        assert!((rows[0].value - 4.2).abs() < f64::EPSILON);
    }

    #[test]
    fn per_pv_override() {
        let mut t = HeartbeatTracker::new(60.0, empty_config());
        let past = Instant::now() - Duration::from_secs(2);
        t.record_store(1, 1.0, 0, 0, past);
        t.record_store(2, 2.0, 0, 0, past);
        t.set_pv_heartbeat(2, 1.0);
        let buf = make_buf();
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1, 2])), 1);
    }

    #[test]
    fn per_pv_disabled() {
        let mut t = HeartbeatTracker::new(0.5, empty_config());
        let past = Instant::now() - Duration::from_secs(1);
        t.record_store(1, 1.0, 0, 0, past);
        t.record_store(2, 2.0, 0, 0, past);
        t.set_pv_heartbeat(2, 0.0);
        let buf = make_buf();
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1, 2])), 1);
    }

    #[test]
    fn multiple_pvs_different_intervals() {
        let mut t = HeartbeatTracker::new(10.0, empty_config());
        let past_5s = Instant::now() - Duration::from_secs(5);
        let past_15s = Instant::now() - Duration::from_secs(15);
        t.record_store(1, 1.0, 0, 0, past_5s);
        t.record_store(2, 2.0, 0, 0, past_15s);
        t.record_store(3, 3.0, 0, 0, past_5s);
        t.set_pv_heartbeat(3, 3.0);
        let buf = make_buf();
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1, 2, 3])), 2);
    }

    #[test]
    fn preserves_severity_status() {
        let mut t = HeartbeatTracker::new(0.5, empty_config());
        let past = Instant::now() - Duration::from_secs(1);
        t.record_store(5, 273.15, 3, 7, past);
        let buf = make_buf();
        t.emit_heartbeats(&buf, &active(&[5]));
        let rows = buf.take_scalars();
        assert_eq!(rows[0].severity, 3);
        assert_eq!(rows[0].status, 7);
    }

    #[test]
    fn unsubscribed_pv_auto_disabled() {
        let mut t = HeartbeatTracker::new(0.5, empty_config());
        let past = Instant::now() - Duration::from_secs(1);
        t.record_store(1, 1.0, 0, 0, past);
        t.record_store(2, 2.0, 0, 0, past);
        let buf = make_buf();
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1, 2])), 2);
        age_pv(&mut t, 1);
        age_pv(&mut t, 2);
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1])), 1);
        age_pv(&mut t, 1);
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1, 2])), 1);
    }

    #[test]
    fn resubscribe_reactivates_on_next_event() {
        let mut t = HeartbeatTracker::new(0.5, empty_config());
        let past = Instant::now() - Duration::from_secs(1);
        t.record_store(1, 1.0, 0, 0, past);
        let buf = make_buf();
        t.emit_heartbeats(&buf, &active(&[]));
        t.record_store(1, 2.0, 0, 0, Instant::now() - Duration::from_secs(1));
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1])), 1);
    }

    #[test]
    fn config_hot_reload() {
        let cfg = empty_config();
        let mut t = HeartbeatTracker::new(60.0, cfg.clone());
        let past = Instant::now() - Duration::from_secs(5);
        t.record_store(1, 1.0, 0, 0, past);
        let buf = make_buf();
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1])), 0);
        cfg.store(Arc::new(vec![(1, 3.0)]));
        assert_eq!(t.emit_heartbeats(&buf, &active(&[1])), 1);
    }

    #[test]
    fn safety_cap() {
        let mut t = HeartbeatTracker::new(60.0, empty_config());
        t.record_store(3_000_000, 1.0, 0, 0, Instant::now());
        assert!(t.entries.is_empty());
    }
}