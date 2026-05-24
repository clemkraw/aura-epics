//! Shard assignment — determines which PVs an instance handles.
//!
//! Shared by `aura-ingest` (which PVs to subscribe) and
//! `aura-discover` (which PVs to assign to which shard).
//!
//! ## Single-shard mode
//!
//! For small installations, use `ShardAssigner::single()` — all PVs go to one instance.

use crate::pv::PvConfig;

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x00000100000001B3;

/// Assigns PVs to shards.
///
/// Single-shard mode (`ShardAssigner::single()`) accepts every PV.
/// Multi-shard mode uses explicit `pv_config.shard_id` or falls back to deterministic FNV-1a hashing of the PV name.
#[derive(Debug, Clone)]
pub struct ShardAssigner {
    pub shard_id: i32,
    pub total_shards: i32,
}

impl ShardAssigner {
    pub fn new(shard_id: i32, total_shards: i32) -> Self {
        assert!(total_shards > 0, "total_shards must be > 0");
        assert!(
            shard_id >= 0 && shard_id < total_shards,
            "shard_id {shard_id} out of range [0, {total_shards})"
        );
        Self {
            shard_id,
            total_shards,
        }
    }

    pub fn single() -> Self {
        Self {
            shard_id: 0,
            total_shards: 1,
        }
    }

    #[inline]
    pub fn is_single(&self) -> bool {
        self.total_shards == 1
    }

    /// Is this PV assigned to this shard
    #[inline]
    pub fn is_mine(&self, config: &PvConfig) -> bool {
        if self.total_shards == 1 {
            return true;
        }
        match config.shard_id {
            Some(sid) => sid == self.shard_id,
            None => Self::hash_shard(&config.pv_name, self.total_shards) == self.shard_id,
        }
    }

    /// Partition configs into (mine, not_mine).
    pub fn partition<'a>(&self, configs: &'a [PvConfig]) -> (Vec<&'a PvConfig>, Vec<&'a PvConfig>) {
        configs.iter().partition(|c| self.is_mine(c))
    }

    /// FNV-1a hash-based shard assignment.
    pub fn hash_shard(pv_name: &str, total: i32) -> i32 {
        (fnv1a(pv_name.as_bytes()) % total as u64) as i32
    }
}

/// FNV-1a hash — fast, deterministic, good distribution.
#[inline]
fn fnv1a(data: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

impl std::fmt::Display for ShardAssigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.total_shards == 1 {
            write!(f, "Shard[single]")
        } else {
            write!(f, "Shard[{}/{}]", self.shard_id, self.total_shards)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pv(name: &str, shard: Option<i32>) -> PvConfig {
        PvConfig {
            pv_name: name.into(),
            shard_id: shard,
            enabled: true,
            description: None,
            unit: None,
            epsilon: None,
            heartbeat_s: 60.0,
            expected_ioc: None,
            created_at: None,
            updated_at: None,
        }
    }

    #[test]
    fn test_new() {
        let s = ShardAssigner::new(0, 4);
        assert_eq!(s.shard_id, 0);
        assert_eq!(s.total_shards, 4);
    }
    #[test]
    fn test_single() {
        let s = ShardAssigner::single();
        assert_eq!(s.total_shards, 1);
        assert!(s.is_single());
    }
    #[test]
    fn test_multi_not_single() {
        assert!(!ShardAssigner::new(0, 4).is_single());
    }
    #[test]
    #[should_panic]
    fn test_invalid_total() {
        ShardAssigner::new(0, 0);
    }
    #[test]
    #[should_panic]
    fn test_invalid_id() {
        ShardAssigner::new(4, 4);
    }
    #[test]
    #[should_panic]
    fn test_negative_id() {
        ShardAssigner::new(-1, 4);
    }

    #[test]
    fn test_single_always_mine() {
        let s = ShardAssigner::single();
        assert!(s.is_mine(&pv("PV:A", None)));
        assert!(s.is_mine(&pv("PV:B", Some(0))));
        assert!(s.is_mine(&pv("PV:C", Some(99)))); // even mismatched shard_id
    }

    #[test]
    fn test_explicit_mine() {
        assert!(ShardAssigner::new(2, 4).is_mine(&pv("PV:A", Some(2))));
    }
    #[test]
    fn test_explicit_not_mine() {
        assert!(!ShardAssigner::new(2, 4).is_mine(&pv("PV:A", Some(3))));
    }

    #[test]
    fn test_hash_deterministic() {
        let s = ShardAssigner::new(0, 4);
        let c = pv("PV:Test", None);
        assert_eq!(s.is_mine(&c), s.is_mine(&c));
    }
    #[test]
    fn test_hash_distribution() {
        let mut counts = [0u32; 4];
        for i in 0..1000 {
            let shard = ShardAssigner::hash_shard(&format!("PV:Test:{i}"), 4);
            counts[shard as usize] += 1;
        }
        for c in counts {
            assert!(c > 150 && c < 350, "bad distribution: {counts:?}");
        }
    }
    #[test]
    fn test_hash_all_pvs_assigned_to_exactly_one() {
        for i in 0..100 {
            let c = pv(&format!("PV:{i}"), None);
            let assigned: i32 = (0..4)
                .filter(|&sid| ShardAssigner::new(sid, 4).is_mine(&c))
                .count() as i32;
            assert_eq!(assigned, 1, "PV:{i} assigned to {assigned} shards");
        }
    }

    #[test]
    fn test_partition() {
        let configs = vec![
            pv("PV:A", Some(0)),
            pv("PV:B", Some(1)),
            pv("PV:C", Some(0)),
        ];
        let (mine, not) = ShardAssigner::new(0, 2).partition(&configs);
        assert_eq!(mine.len(), 2);
        assert_eq!(not.len(), 1);
    }
    #[test]
    fn test_partition_empty() {
        let (mine, not) = ShardAssigner::single().partition(&[]);
        assert!(mine.is_empty());
        assert!(not.is_empty());
    }
    #[test]
    fn test_partition_single_gets_all() {
        let configs = vec![pv("A", Some(0)), pv("B", Some(1)), pv("C", None)];
        let (mine, not) = ShardAssigner::single().partition(&configs);
        assert_eq!(mine.len(), 3);
        assert!(not.is_empty());
    }

    #[test]
    fn test_fnv1a_empty() {
        assert_eq!(fnv1a(b""), FNV_OFFSET);
    }
    #[test]
    fn test_fnv1a_nonzero() {
        assert_ne!(fnv1a(b"hello"), fnv1a(b"world"));
    }
    #[test]
    fn test_fnv1a_stable() {
        assert_eq!(fnv1a(b"PV:Test"), fnv1a(b"PV:Test"));
    }

    #[test]
    fn test_display_multi() {
        assert_eq!(ShardAssigner::new(2, 8).to_string(), "Shard[2/8]");
    }
    #[test]
    fn test_display_single() {
        assert_eq!(ShardAssigner::single().to_string(), "Shard[single]");
    }
}
