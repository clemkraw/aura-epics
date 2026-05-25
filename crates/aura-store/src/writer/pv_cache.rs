//! PV name → numeric ID resolution cache.
//!
//! Every sample row stores `pv_id` (4 bytes INTEGER) instead of `pv_name`.
//!
//! ## Hot path
//!
//! `resolve()` is called for every sample that passes the filter.
//! - Cache hit (99.9% of calls): `HashMap::get` → zero allocation.
//! - Cache miss (first sample for a new PV): upsert into `pv_lookup` table.
//!
//! ## Memory management
//!
//! PV IDs are immutable — once assigned, they never change. The cache
//! never evicts entries. A hard limit (`max_entries`) prevents unbounded
//! growth if a misconfigured IOC publishes millions of unique PV names.
//!
//! ## Consistency
//!
//! Multiple `aura-store` instances may race to insert the same PV name.
//! The `ON CONFLICT DO UPDATE ... RETURNING pv_id` ensures exactly one
//! ID is assigned, and all instances converge to the same mapping.
//!
//! ## Startup
//!
//! `warm()` loads all existing entries at startup to avoid a burst of
//! DB lookups when the first samples arrive.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use sqlx::postgres::PgPool;

use aura_core::error::{AuraError, AuraResult};

/// Default maximum cache entries (prevents unbounded growth).
const DEFAULT_MAX_ENTRIES: usize = 500_000;

/// Upsert SQL — race-safe across multiple aura-store instances.
/// `ON CONFLICT DO UPDATE` forces RETURNING to always return the pv_id
/// (unlike `DO NOTHING` which returns nothing on conflict).
const UPSERT_SQL: &str = r#"
    INSERT INTO pv_lookup (pv_name)
    VALUES ($1)
    ON CONFLICT (pv_name) DO UPDATE SET pv_name = EXCLUDED.pv_name
    RETURNING pv_id
"#;

/// Cached mapping from PV name to numeric PV ID.
///
/// One instance per `aura-store` process, shared across all writers.
pub struct PvCache {
    cache: HashMap<Arc<str>, i32>,
    max_entries: usize,

    hits: u64,
    misses: u64,
    /// Times a resolve was rejected because the cache is full.
    saturations: u64,
}

impl PvCache {
    /// Create an empty cache with default limits.
    pub fn new() -> Self {
        Self::with_limits(0, DEFAULT_MAX_ENTRIES)
    }

    /// Create with pre-allocated capacity and entry limit.
    pub fn with_limits(initial_capacity: usize, max_entries: usize) -> Self {
        let max_entries = max_entries.max(1);
        Self {
            cache: HashMap::with_capacity(initial_capacity.min(max_entries)),
            max_entries,
            hits: 0,
            misses: 0,
            saturations: 0,
        }
    }

    /// Create pre-sized for an expected number of PVs.
    pub fn with_capacity(expected_pvs: usize) -> Self {
        Self::with_limits(expected_pvs, DEFAULT_MAX_ENTRIES)
    }

    /// Resolve a PV name to its numeric ID.
    pub async fn resolve(&mut self, pv_name: &str, pool: &PgPool) -> AuraResult<i32> {
        // Fast path: cache hit (99.9% of calls).
        if let Some(&id) = self.cache.get(pv_name) {
            self.hits += 1;
            return Ok(id);
        }

        // Cache full — still resolve from DB but don't cache.
        if self.cache.len() >= self.max_entries {
            self.saturations += 1;
            self.misses += 1;
            return self.upsert_pv_lookup(pv_name, pool).await;
        }

        // Slow path: cache miss — upsert and cache.
        self.misses += 1;
        let id = self.upsert_pv_lookup(pv_name, pool).await?;
        self.cache.insert(Arc::from(pv_name), id);
        Ok(id)
    }

    /// Resolve from local cache only (no DB access).
    /// Returns `None` on cache miss.
    #[inline]
    pub fn resolve_cached(&self, pv_name: &str) -> Option<i32> {
        self.cache.get(pv_name).copied()
    }

    /// Resolve multiple PV names in bulk, returning (hits, misses).
    pub async fn resolve_bulk(&mut self, pv_names: &[&str], pool: &PgPool) -> AuraResult<Vec<i32>> {
        let mut ids = Vec::with_capacity(pv_names.len());

        for &pv_name in pv_names {
            let id = self.resolve(pv_name, pool).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    /// Pre-warm the cache by loading all entries from `pv_lookup`.
    ///
    /// Call at startup to avoid a burst of cache misses.
    /// Returns the number of entries loaded.
    pub async fn warm(&mut self, pool: &PgPool) -> AuraResult<usize> {
        let rows = sqlx::query_as::<_, (i32, String)>(
            "SELECT pv_id, pv_name FROM pv_lookup ORDER BY pv_id",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| AuraError::database(format!("pv_lookup warm failed: {e}")))?;

        let count = rows.len().min(self.max_entries);
        for (id, name) in rows.into_iter().take(self.max_entries) {
            self.cache.insert(Arc::from(&*name), id);
        }

        tracing::info!(entries = count, "PV cache warmed");
        Ok(count)
    }

    /// Insert a mapping directly (for testing or manual population).
    pub fn insert(&mut self, pv_name: impl Into<String>, pv_id: i32) -> bool {
        if self.cache.len() >= self.max_entries {
            return false;
        }
        let s: String = pv_name.into();
        self.cache.insert(Arc::from(&*s), pv_id);
        true
    }

    /// Number of PVs in the cache.
    #[inline]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Whether the cache is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Whether the cache has reached its maximum size.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.cache.len() >= self.max_entries
    }

    /// Maximum entries allowed.
    #[inline]
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Cache utilization (0.0 to 1.0).
    pub fn utilization(&self) -> f64 {
        if self.max_entries == 0 {
            return 0.0;
        }
        self.cache.len() as f64 / self.max_entries as f64
    }

    /// Approximate memory used by the cache in bytes.
    pub fn mem_bytes(&self) -> usize {
        // Each entry: String key (~24 + avg 30 chars) + i32 value(4) + HashMap overhead(~32)
        self.cache.len() * 90
    }

    /// Total cache hits.
    #[inline]
    pub fn hits(&self) -> u64 {
        self.hits
    }

    /// Total cache misses (DB lookups).
    #[inline]
    pub fn misses(&self) -> u64 {
        self.misses
    }

    /// Total lookups (hits + misses).
    #[inline]
    pub fn total_lookups(&self) -> u64 {
        self.hits + self.misses
    }

    /// Times resolve was called when the cache was full.
    #[inline]
    pub fn saturations(&self) -> u64 {
        self.saturations
    }

    /// Cache hit ratio (0.0 to 1.0). Returns 1.0 if no lookups.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            return 1.0;
        }
        self.hits as f64 / total as f64
    }

    /// Reset statistics counters (not the cache itself).
    pub fn reset_stats(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.saturations = 0;
    }

    /// Clear the entire cache and stats.
    pub fn clear(&mut self) {
        self.cache.clear();
        self.hits = 0;
        self.misses = 0;
        self.saturations = 0;
    }

    /// Iterate over all cached (pv_name, pv_id) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, i32)> {
        self.cache.iter().map(|(k, &v)| (&**k, v))
    }

    /// Insert the PV name into `pv_lookup` if it doesn't exist, then return the pv_id.
    /// Bulk-insert PV names into `pv_lookup` and warm the cache.
    /// Call at startup after monitors are connected, before processing events.
    /// Uses a single INSERT ... ON CONFLICT for all PVs → ~100ms for 30k PVs.
    pub async fn bulk_upsert(&mut self, pv_names: &[String], pool: &PgPool) -> AuraResult<usize> {
        if pv_names.is_empty() {
            return Ok(0);
        }

        // Single bulk INSERT with UNNEST — all PVs in one SQL round-trip.
        let names: Vec<&str> = pv_names.iter().map(|s| s.as_str()).collect();
        sqlx::query(
            "INSERT INTO pv_lookup (pv_name) \
             SELECT unnest($1::text[]) \
             ON CONFLICT (pv_name) DO NOTHING",
        )
        .bind(&names)
        .execute(pool)
        .await
        .map_err(|e| AuraError::database(format!("bulk pv_lookup insert: {e}")))?;

        // Now load all IDs into the cache.
        let rows = sqlx::query_as::<_, (i32, String)>(
            "SELECT id, pv_name FROM pv_lookup WHERE pv_name = ANY($1::text[])",
        )
        .bind(&names)
        .fetch_all(pool)
        .await
        .map_err(|e| AuraError::database(format!("bulk pv_lookup fetch: {e}")))?;

        let mut count = 0;
        for (id, name) in rows {
            if self.cache.len() < self.max_entries {
                self.cache.insert(Arc::from(&*name), id);
                count += 1;
            }
        }

        Ok(count)
    }

    async fn upsert_pv_lookup(&self, pv_name: &str, pool: &PgPool) -> AuraResult<i32> {
        sqlx::query_scalar::<_, i32>(UPSERT_SQL)
            .bind(pv_name)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                AuraError::database(format!("pv_lookup upsert failed for '{}': {}", pv_name, e))
            })
    }
}

impl Default for PvCache {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PvCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PvCache")
            .field(
                "entries",
                &format!("{}/{}", self.cache.len(), self.max_entries),
            )
            .field(
                "utilization",
                &format!("{:.1}%", self.utilization() * 100.0),
            )
            .field(
                "mem",
                &format!("{:.1} KB", self.mem_bytes() as f64 / 1024.0),
            )
            .field("hits", &self.hits)
            .field("misses", &self.misses)
            .field("hit_ratio", &format!("{:.2}%", self.hit_ratio() * 100.0))
            .field("saturations", &self.saturations)
            .finish()
    }
}

impl fmt::Display for PvCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PvCache: {}/{} entries ({:.1}%, {:.1} KB), \
             hit_ratio={:.2}% ({} hits, {} misses), {} saturations",
            self.cache.len(),
            self.max_entries,
            self.utilization() * 100.0,
            self.mem_bytes() as f64 / 1024.0,
            self.hit_ratio() * 100.0,
            self.hits,
            self.misses,
            self.saturations,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let c = PvCache::new();
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
        assert!(!c.is_full());
        assert_eq!(c.max_entries(), DEFAULT_MAX_ENTRIES);
        assert_eq!(c.hits(), 0);
        assert_eq!(c.misses(), 0);
        assert_eq!(c.saturations(), 0);
        assert_eq!(c.total_lookups(), 0);
        assert_eq!(c.hit_ratio(), 1.0);
        assert_eq!(c.utilization(), 0.0);
        assert_eq!(c.mem_bytes(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let c = PvCache::with_capacity(10_000);
        assert_eq!(c.len(), 0);
        assert_eq!(c.max_entries(), DEFAULT_MAX_ENTRIES);
    }

    #[test]
    fn test_with_limits() {
        let c = PvCache::with_limits(100, 1000);
        assert_eq!(c.max_entries(), 1000);
    }

    #[test]
    fn test_min_max_entries() {
        let c = PvCache::with_limits(0, 0);
        assert_eq!(c.max_entries(), 1);
    }

    #[test]
    fn test_default() {
        let c = PvCache::default();
        assert_eq!(c.len(), 0);
        assert_eq!(c.max_entries(), DEFAULT_MAX_ENTRIES);
    }

    #[test]
    fn test_resolve_cached_empty() {
        assert_eq!(PvCache::new().resolve_cached("PV:A"), None);
    }

    #[test]
    fn test_resolve_cached_hit() {
        let mut c = PvCache::new();
        c.insert("PV:A", 42);
        assert_eq!(c.resolve_cached("PV:A"), Some(42));
    }

    #[test]
    fn test_resolve_cached_miss() {
        let mut c = PvCache::new();
        c.insert("PV:A", 1);
        assert_eq!(c.resolve_cached("PV:B"), None);
    }

    #[test]
    fn test_resolve_cached_multiple() {
        let mut c = PvCache::new();
        c.insert("PV:A", 1);
        c.insert("PV:B", 2);
        c.insert("PV:C", 3);

        assert_eq!(c.resolve_cached("PV:A"), Some(1));
        assert_eq!(c.resolve_cached("PV:B"), Some(2));
        assert_eq!(c.resolve_cached("PV:C"), Some(3));
        assert_eq!(c.resolve_cached("PV:D"), None);
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn test_insert() {
        let mut c = PvCache::new();
        assert!(c.insert("PV:A", 1));
        assert_eq!(c.len(), 1);
        assert_eq!(c.resolve_cached("PV:A"), Some(1));
    }

    #[test]
    fn test_insert_overwrite() {
        let mut c = PvCache::new();
        c.insert("PV:A", 1);
        c.insert("PV:A", 99);
        assert_eq!(c.resolve_cached("PV:A"), Some(99));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn test_insert_rejected_when_full() {
        let mut c = PvCache::with_limits(0, 2);
        assert!(c.insert("PV:A", 1));
        assert!(c.insert("PV:B", 2));
        assert!(!c.insert("PV:C", 3)); // full
        assert_eq!(c.len(), 2);
        assert!(c.is_full());
    }

    #[test]
    fn test_hit_ratio_no_lookups() {
        assert_eq!(PvCache::new().hit_ratio(), 1.0);
    }

    #[test]
    fn test_hit_ratio_all_hits() {
        let mut c = PvCache::new();
        c.hits = 100;
        assert_eq!(c.hit_ratio(), 1.0);
    }

    #[test]
    fn test_hit_ratio_all_misses() {
        let mut c = PvCache::new();
        c.misses = 100;
        assert_eq!(c.hit_ratio(), 0.0);
    }

    #[test]
    fn test_hit_ratio_mixed() {
        let mut c = PvCache::new();
        c.hits = 90;
        c.misses = 10;
        assert!((c.hit_ratio() - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_total_lookups() {
        let mut c = PvCache::new();
        c.hits = 90;
        c.misses = 10;
        assert_eq!(c.total_lookups(), 100);
    }

    #[test]
    fn test_utilization_empty() {
        assert_eq!(PvCache::new().utilization(), 0.0);
    }

    #[test]
    fn test_utilization_half() {
        let mut c = PvCache::with_limits(0, 10);
        for i in 0..5 {
            c.insert(format!("PV:{i}"), i);
        }
        assert!((c.utilization() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_utilization_full() {
        let mut c = PvCache::with_limits(0, 3);
        c.insert("A", 1);
        c.insert("B", 2);
        c.insert("C", 3);
        assert!((c.utilization() - 1.0).abs() < 0.01);
        assert!(c.is_full());
    }

    #[test]
    fn test_mem_bytes() {
        let mut c = PvCache::new();
        assert_eq!(c.mem_bytes(), 0);
        c.insert("PV:A", 1);
        assert_eq!(c.mem_bytes(), 90); // ~90 bytes per entry
    }

    #[test]
    fn test_mem_bytes_scales() {
        let mut c = PvCache::new();
        for i in 0..100 {
            c.insert(format!("PV:{i}"), i);
        }
        assert_eq!(c.mem_bytes(), 100 * 90);
    }

    #[test]
    fn test_reset_stats() {
        let mut c = PvCache::new();
        c.insert("PV:A", 1);
        c.hits = 50;
        c.misses = 5;
        c.saturations = 1;

        c.reset_stats();

        assert_eq!(c.hits(), 0);
        assert_eq!(c.misses(), 0);
        assert_eq!(c.saturations(), 0);
        assert_eq!(c.len(), 1); // cache preserved
        assert_eq!(c.resolve_cached("PV:A"), Some(1));
    }

    #[test]
    fn test_clear() {
        let mut c = PvCache::new();
        c.insert("PV:A", 1);
        c.insert("PV:B", 2);
        c.hits = 50;
        c.misses = 5;
        c.saturations = 1;

        c.clear();

        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
        assert_eq!(c.hits(), 0);
        assert_eq!(c.misses(), 0);
        assert_eq!(c.saturations(), 0);
        assert_eq!(c.resolve_cached("PV:A"), None);
    }

    #[test]
    fn test_iter_empty() {
        assert_eq!(PvCache::new().iter().count(), 0);
    }

    #[test]
    fn test_iter() {
        let mut c = PvCache::new();
        c.insert("PV:A", 1);
        c.insert("PV:B", 2);

        let mut pairs: Vec<(&str, i32)> = c.iter().collect();
        pairs.sort_by_key(|p| p.0);
        assert_eq!(pairs, vec![("PV:A", 1), ("PV:B", 2)]);
    }

    #[test]
    fn test_upsert_sql_has_returning() {
        assert!(UPSERT_SQL.to_uppercase().contains("RETURNING PV_ID"));
    }

    #[test]
    fn test_upsert_sql_has_on_conflict() {
        assert!(UPSERT_SQL.to_uppercase().contains("ON CONFLICT"));
    }

    #[test]
    fn test_upsert_sql_race_safe() {
        // DO UPDATE (not DO NOTHING) ensures RETURNING always works.
        assert!(UPSERT_SQL.to_uppercase().contains("DO UPDATE"));
    }

    #[test]
    fn test_simulated_hot_path() {
        let mut c = PvCache::new();
        let pvs = ["PV:CRYO:TEMP", "PV:MAG:I", "PV:VAC:PRES"];

        // First sample per PV = miss.
        for (i, &pv) in pvs.iter().enumerate() {
            assert_eq!(c.resolve_cached(pv), None);
            c.misses += 1;
            c.insert(pv, i as i32 + 1);
        }

        for _ in 0..997 {
            for &pv in &pvs {
                assert!(c.resolve_cached(pv).is_some());
                c.hits += 1;
            }
        }

        assert_eq!(c.len(), 3);
        assert_eq!(c.misses(), 3);
        assert_eq!(c.hits(), 2991);
        assert!(c.hit_ratio() > 0.998);
    }

    #[test]
    fn test_simulated_saturation() {
        let mut c = PvCache::with_limits(0, 3);

        // Fill the cache.
        c.insert("PV:A", 1);
        c.insert("PV:B", 2);
        c.insert("PV:C", 3);
        assert!(c.is_full());

        // Simulate resolve for a new PV when full.
        // In real code this would go to DB but not cache.
        assert_eq!(c.resolve_cached("PV:D"), None);
        // The saturation counter would be incremented by resolve().
    }

    #[test]
    fn test_display() {
        let mut c = PvCache::with_limits(0, 1000);
        c.insert("PV:A", 1);
        c.hits = 90;
        c.misses = 10;
        c.saturations = 1;

        let s = c.to_string();
        assert!(s.contains("1/1000"));
        assert!(s.contains("90.00%")); // hit ratio
        assert!(s.contains("90 hits"));
        assert!(s.contains("10 misses"));
        assert!(s.contains("1 saturations"));
    }

    #[test]
    fn test_display_empty() {
        let s = PvCache::new().to_string();
        assert!(s.contains("0/500000"));
        assert!(s.contains("100.00%"));
    }

    #[test]
    fn test_debug() {
        let c = PvCache::new();
        let d = format!("{:?}", c);
        assert!(d.contains("PvCache"));
        assert!(d.contains("entries"));
        assert!(d.contains("utilization"));
        assert!(d.contains("mem"));
        assert!(d.contains("hit_ratio"));
        assert!(d.contains("saturations"));
    }
}