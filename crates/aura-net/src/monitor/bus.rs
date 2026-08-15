//! Sharded monitor event bus — aggregated channel per ingest shard.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::subscription::MonitorEvent;

/// A tagged event carrying the PV name alongside the monitor event.
pub struct TaggedEvent {
    pub pv_name: Arc<str>,
    pub pv_id: i32,
    pub event: MonitorEvent,
}

/// Sender side — cloned into each session.
#[derive(Clone)]
pub struct MonitorBusTx {
    txs: Arc<Vec<crossbeam_channel::Sender<TaggedEvent>>>,
    n_shards: usize,
    dropped: Arc<AtomicU64>,
}

/// FNV-1a hash for shard assignment.
#[inline]
pub fn shard_for_pv(pv_name: &str, n_shards: usize) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in pv_name.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % n_shards
}

impl MonitorBusTx {
    #[inline]
    pub fn send_to_shard(
        &self,
        pv_name: Arc<str>,
        pv_id: i32,
        shard: usize,
        event: MonitorEvent,
    ) -> Result<(), crossbeam_channel::TrySendError<TaggedEvent>> {
        let result = self.txs[shard].try_send(TaggedEvent {
            pv_name,
            pv_id,
            event,
        });
        if result.is_err() {
            // Counted here (not at call sites) so that no `let _ =` can
            // ever make a drop invisible again.
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Total events dropped on full shard channels since startup.
    #[inline]
    pub fn total_dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn n_shards(&self) -> usize {
        self.n_shards
    }
}

/// Receiver side — one per ingest shard.
pub struct MonitorBusRx {
    rx: crossbeam_channel::Receiver<TaggedEvent>,
}

impl MonitorBusRx {
    /// Drain all available events without blocking (for ingest hot loop).
    #[inline]
    pub fn drain_into(&mut self, out: &mut Vec<TaggedEvent>, max: usize) {
        for _ in 0..max {
            match self.rx.try_recv() {
                Ok(e) => out.push(e),
                Err(_) => break,
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.rx.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

/// Create a sharded monitor bus with N shards.
pub fn create_bus(n_shards: usize, buffer_per_shard: usize) -> (MonitorBusTx, Vec<MonitorBusRx>) {
    let mut txs = Vec::with_capacity(n_shards);
    let mut rxs = Vec::with_capacity(n_shards);

    for _ in 0..n_shards {
        let (tx, rx) = crossbeam_channel::bounded(buffer_per_shard);
        txs.push(tx);
        rxs.push(MonitorBusRx { rx });
    }

    let bus_tx = MonitorBusTx {
        txs: Arc::new(txs),
        n_shards,
        dropped: Arc::new(AtomicU64::new(0)),
    };

    (bus_tx, rxs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pv(name: &str) -> Arc<str> {
        Arc::from(name)
    }

    fn disconnect(pv_name: &str, pv_id: i32) -> (Arc<str>, i32, MonitorEvent) {
        (pv(pv_name), pv_id, MonitorEvent::Disconnect)
    }

    fn scalar(pv_name: &str, pv_id: i32, value: f64) -> (Arc<str>, i32, MonitorEvent) {
        (
            pv(pv_name),
            pv_id,
            MonitorEvent::ScalarDelta {
                value,
                seconds: 1_700_000_000,
                nanos: 0,
                severity: 0,
                status: 0,
            },
        )
    }

    #[test]
    fn shard_deterministic() {
        let a = shard_for_pv("PERLE:Gun:Vacuum", 4);
        let b = shard_for_pv("PERLE:Gun:Vacuum", 4);
        assert_eq!(a, b);
    }

    #[test]
    fn shard_in_range() {
        for n in 1..=16 {
            let s = shard_for_pv("PV:TEST", n);
            assert!(s < n, "shard {s} >= n_shards {n}");
        }
    }

    #[test]
    fn shard_single() {
        assert_eq!(shard_for_pv("anything", 1), 0);
    }

    #[test]
    fn shard_different_pvs_spread() {
        let mut counts = [0u32; 4];
        for i in 0..100 {
            let name = format!("PV:TEST:{i}");
            counts[shard_for_pv(&name, 4)] += 1;
        }
        for c in counts {
            assert!(c > 5, "poor distribution: {counts:?}");
        }
    }

    #[test]
    fn shard_empty_string() {
        let s = shard_for_pv("", 4);
        assert!(s < 4);
    }

    #[test]
    fn shard_utf8() {
        let s = shard_for_pv("PV:café:日本語", 8);
        assert!(s < 8);
    }

    #[test]
    fn shard_similar_names_differ() {
        let a = shard_for_pv("PV:A", 1024);
        let b = shard_for_pv("PV:B", 1024);
        assert_ne!(a, b, "single-char difference should hash differently");
    }

    #[test]
    fn create_single_shard() {
        let (tx, rxs) = create_bus(1, 64);
        assert_eq!(tx.n_shards(), 1);
        assert_eq!(rxs.len(), 1);
    }

    #[test]
    fn create_multi_shard() {
        let (tx, rxs) = create_bus(8, 128);
        assert_eq!(tx.n_shards(), 8);
        assert_eq!(rxs.len(), 8);
    }

    #[test]
    fn send_recv_single() {
        let (tx, mut rxs) = create_bus(1, 64);
        let (name, id, ev) = disconnect("PV:A", 42);
        assert!(tx.send_to_shard(name, id, 0, ev).is_ok());
        let mut buf = Vec::new();
        rxs[0].drain_into(&mut buf, 100);
        assert_eq!(buf.len(), 1);
        assert_eq!(&*buf[0].pv_name, "PV:A");
        assert_eq!(buf[0].pv_id, 42);
        assert!(buf[0].event.is_disconnect());
    }

    #[test]
    fn send_recv_scalar_value() {
        let (tx, mut rxs) = create_bus(1, 64);
        let (name, id, ev) = scalar("PV:T", 7, 3.96);
        tx.send_to_shard(name, id, 0, ev).unwrap();
        let mut buf = Vec::new();
        rxs[0].drain_into(&mut buf, 10);
        assert_eq!(buf.len(), 1);
        if let MonitorEvent::ScalarDelta { value, .. } = &buf[0].event {
            assert!((value - 3.96).abs() < f64::EPSILON);
        } else {
            panic!("expected ScalarDelta");
        }
    }

    #[test]
    fn send_batch() {
        let (tx, mut rxs) = create_bus(1, 1000);
        for i in 0..100 {
            let (n, id, ev) = scalar("PV:X", i, i as f64);
            tx.send_to_shard(n, id, 0, ev).unwrap();
        }
        let mut buf = Vec::new();
        rxs[0].drain_into(&mut buf, 1000);
        assert_eq!(buf.len(), 100);
        assert_eq!(buf[0].pv_id, 0);
        assert_eq!(buf[99].pv_id, 99);
    }

    #[test]
    fn drain_respects_max() {
        let (tx, mut rxs) = create_bus(1, 1000);
        for i in 0..50 {
            let (n, id, ev) = disconnect("PV", i);
            tx.send_to_shard(n, id, 0, ev).unwrap();
        }
        let mut buf = Vec::new();
        rxs[0].drain_into(&mut buf, 10);
        assert_eq!(buf.len(), 10);
        rxs[0].drain_into(&mut buf, 100);
        assert_eq!(buf.len(), 50);
    }

    #[test]
    fn drain_empty_channel() {
        let (_tx, mut rxs) = create_bus(1, 64);
        let mut buf = Vec::new();
        rxs[0].drain_into(&mut buf, 100);
        assert!(buf.is_empty());
    }

    #[test]
    fn drain_appends() {
        let (tx, mut rxs) = create_bus(1, 64);
        let (n, id, ev) = disconnect("PV:1", 1);
        tx.send_to_shard(n, id, 0, ev).unwrap();
        let mut buf = Vec::new();
        buf.push(TaggedEvent {
            pv_name: pv("existing"),
            pv_id: 0,
            event: MonitorEvent::Reconnect,
        });
        rxs[0].drain_into(&mut buf, 10);
        assert_eq!(buf.len(), 2);
        assert_eq!(&*buf[0].pv_name, "existing");
        assert_eq!(&*buf[1].pv_name, "PV:1");
    }

    #[test]
    fn shard_isolation() {
        let (tx, mut rxs) = create_bus(4, 64);
        let (n, id, ev) = disconnect("PV:A", 1);
        tx.send_to_shard(n, id, 2, ev).unwrap();
        for (i, rx) in rxs.iter_mut().enumerate() {
            let mut buf = Vec::new();
            rx.drain_into(&mut buf, 100);
            if i == 2 {
                assert_eq!(buf.len(), 1);
            } else {
                assert!(buf.is_empty(), "shard {i} should be empty");
            }
        }
    }

    #[test]
    fn backpressure_when_full() {
        let (tx, _rxs) = create_bus(1, 4);
        for i in 0..4 {
            let (n, id, ev) = disconnect("PV", i);
            assert!(tx.send_to_shard(n, id, 0, ev).is_ok());
        }
        let (n, id, ev) = disconnect("PV", 99);
        assert!(tx.send_to_shard(n, id, 0, ev).is_err());
        assert_eq!(tx.total_dropped(), 1);
    }

    #[test]
    fn dropped_counter_shared_across_clones() {
        let (tx, _rxs) = create_bus(1, 1);
        let tx2 = tx.clone();
        let (n1, id1, ev1) = disconnect("PV", 1);
        tx.send_to_shard(n1, id1, 0, ev1).unwrap();
        let (n2, id2, ev2) = disconnect("PV", 2);
        assert!(tx2.send_to_shard(n2, id2, 0, ev2).is_err());
        // The drop on the clone is visible from the original handle.
        assert_eq!(tx.total_dropped(), 1);
        assert_eq!(tx2.total_dropped(), 1);
    }

    #[test]
    fn backpressure_clears_after_drain() {
        let (tx, mut rxs) = create_bus(1, 2);
        let (n1, id1, ev1) = disconnect("PV", 1);
        let (n2, id2, ev2) = disconnect("PV", 2);
        tx.send_to_shard(n1, id1, 0, ev1).unwrap();
        tx.send_to_shard(n2, id2, 0, ev2).unwrap();
        let (n3, id3, ev3) = disconnect("PV", 3);
        assert!(tx.send_to_shard(n3, id3, 0, ev3).is_err());
        let mut buf = Vec::new();
        rxs[0].drain_into(&mut buf, 10);
        assert_eq!(buf.len(), 2);
        let (n4, id4, ev4) = disconnect("PV", 4);
        assert!(tx.send_to_shard(n4, id4, 0, ev4).is_ok());
    }

    #[test]
    fn tx_clone_shares_channel() {
        let (tx, mut rxs) = create_bus(1, 64);
        let tx2 = tx.clone();
        let (n1, id1, ev1) = disconnect("A", 1);
        let (n2, id2, ev2) = disconnect("B", 2);
        tx.send_to_shard(n1, id1, 0, ev1).unwrap();
        tx2.send_to_shard(n2, id2, 0, ev2).unwrap();
        let mut buf = Vec::new();
        rxs[0].drain_into(&mut buf, 10);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn concurrent_sends() {
        let (tx, mut rxs) = create_bus(2, 10_000);
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    for i in 0..1000_i32 {
                        let shard = ((t * 1000 + i) % 2) as usize;
                        let (n, id, ev) = disconnect("PV", t * 1000 + i);
                        tx.send_to_shard(n, id, shard, ev).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let mut total = 0;
        for rx in rxs.iter_mut() {
            let mut buf = Vec::new();
            rx.drain_into(&mut buf, 100_000);
            total += buf.len();
        }
        assert_eq!(total, 4000);
    }

    #[test]
    fn rx_len() {
        let (tx, rxs) = create_bus(1, 64);
        assert_eq!(rxs[0].len(), 0);
        let (n, id, ev) = disconnect("PV", 1);
        tx.send_to_shard(n, id, 0, ev).unwrap();
        assert_eq!(rxs[0].len(), 1);
    }
}
