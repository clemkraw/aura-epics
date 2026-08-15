//! PV name → numeric ID resolution cache.
//!
//! Every sample row stores `pv_id` (4 bytes INTEGER) instead of `pv_name`.
//!
//! ## Hot path
//!
//! `resolve()` is called for every incoming sample.
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

const DEFAULT_MAX_ENTRIES: usize = 8_000_000;

/// Upsert SQL — race-safe across multiple aura-store instances.
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
    saturations: u64,
}

impl PvCache {
    pub fn new() -> Self {
        Self::with_limits(0, DEFAULT_MAX_ENTRIES)
    }

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

    pub fn with_capacity(expected_pvs: usize) -> Self {
        Self::with_limits(expected_pvs, DEFAULT_MAX_ENTRIES)
    }

    /// Resolve a PV name to its numeric ID.
    pub async fn resolve(&mut self, pv_name: &str, pool: &PgPool) -> AuraResult<i32> {
        if let Some(&id) = self.cache.get(pv_name) {
            self.hits += 1;
            return Ok(id);
        }

        if self.cache.len() >= self.max_entries {
            self.saturations += 1;
            self.misses += 1;
            // Saturation must be LOUD: past the cap, every event for an
            // uncached PV costs one SQL round-trip on the fallback path.
            if self.saturations == 1 || self.saturations.is_multiple_of(100_000) {
                tracing::warn!(
                    max_entries = self.max_entries,
                    saturations = self.saturations,
                    "PvCache saturated — uncached PVs now cost one SQL upsert per event"
                );
            }
            return self.upsert_pv_lookup(pv_name, pool).await;
        }

        self.misses += 1;
        let id = self.upsert_pv_lookup(pv_name, pool).await?;
        self.cache.insert(Arc::from(pv_name), id);
        Ok(id)
    }

    /// Resolve from local cache only (no DB access).
    #[inline]
    pub fn resolve_cached(&self, pv_name: &str) -> Option<i32> {
        self.cache.get(pv_name).copied()
    }

    /// Pre-warm the cache by loading entries from `pv_lookup`.
    ///
    /// Bounded fetch (LIMIT max_entries + 1: no point materialising rows
    /// the cache cannot hold), and truncation is an ERROR-level event —
    /// a silently incomplete cache means SQL-per-event on the fallback
    /// path for every PV that did not fit.
    pub async fn warm(&mut self, pool: &PgPool) -> AuraResult<usize> {
        let limit = self.max_entries as i64 + 1;
        let mut rows = sqlx::query_as::<_, (i32, String)>(
            "SELECT pv_id, pv_name FROM pv_lookup ORDER BY pv_id LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| AuraError::database(format!("pv_lookup warm failed: {e}")))?;

        if rows.len() > self.max_entries {
            rows.truncate(self.max_entries);
            tracing::error!(
                max_entries = self.max_entries,
                "pv_lookup holds MORE PVs than the cache cap — warm TRUNCATED; \
                 raise DEFAULT_MAX_ENTRIES (writer/pv_cache.rs) for this deployment"
            );
        }
        let count = rows.len();
        for (id, name) in rows {
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

    /// Bulk-insert PV names into `pv_lookup` and warm the cache.
    /// Uses a single INSERT ... ON CONFLICT for all PVs → ~100ms for 30k PVs.
    pub async fn bulk_upsert(&mut self, pv_names: &[String], pool: &PgPool) -> AuraResult<usize> {
        if pv_names.is_empty() {
            return Ok(0);
        }

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

        let rows = sqlx::query_as::<_, (i32, String)>(
            "SELECT pv_id, pv_name FROM pv_lookup WHERE pv_name = ANY($1::text[])",
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

    #[inline]
    pub fn len(&self) -> usize {
        self.cache.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
    #[inline]
    pub fn is_full(&self) -> bool {
        self.cache.len() >= self.max_entries
    }
    #[inline]
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }
    #[inline]
    pub fn hits(&self) -> u64 {
        self.hits
    }
    #[inline]
    pub fn misses(&self) -> u64 {
        self.misses
    }
    #[inline]
    pub fn total_lookups(&self) -> u64 {
        self.hits + self.misses
    }
    #[inline]
    pub fn saturations(&self) -> u64 {
        self.saturations
    }

    pub fn utilization(&self) -> f64 {
        if self.max_entries == 0 {
            return 0.0;
        }
        self.cache.len() as f64 / self.max_entries as f64
    }

    pub fn mem_bytes(&self) -> usize {
        self.cache.len() * 90
    }

    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            return 1.0;
        }
        self.hits as f64 / total as f64
    }

    pub fn snapshot(&self) -> HashMap<Arc<str>, i32> {
        self.cache.clone()
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.hits = 0;
        self.misses = 0;
        self.saturations = 0;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, i32)> {
        self.cache.iter().map(|(k, &v)| (&**k, v))
    }

    async fn upsert_pv_lookup(&self, pv_name: &str, pool: &PgPool) -> AuraResult<i32> {
        sqlx::query_scalar::<_, i32>(UPSERT_SQL)
            .bind(pv_name)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                AuraError::database(format!("pv_lookup upsert failed for '{pv_name}': {e}"))
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
            self.saturations
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let c = PvCache::new();
        assert!(c.is_empty());
        assert_eq!(c.max_entries(), DEFAULT_MAX_ENTRIES);
        assert_eq!(c.hit_ratio(), 1.0);
    }

    #[test]
    fn test_with_limits() {
        assert_eq!(PvCache::with_limits(100, 1000).max_entries(), 1000);
        assert_eq!(PvCache::with_limits(0, 0).max_entries(), 1); // min clamp
    }

    #[test]
    fn test_resolve_cached() {
        let mut c = PvCache::new();
        assert_eq!(c.resolve_cached("PV:A"), None);
        c.insert("PV:A", 42);
        assert_eq!(c.resolve_cached("PV:A"), Some(42));
        assert_eq!(c.resolve_cached("PV:B"), None);
    }

    #[test]
    fn test_insert() {
        let mut c = PvCache::new();
        assert!(c.insert("PV:A", 1));
        assert_eq!(c.len(), 1);
        // overwrite
        c.insert("PV:A", 99);
        assert_eq!(c.resolve_cached("PV:A"), Some(99));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn test_insert_rejected_when_full() {
        let mut c = PvCache::with_limits(0, 2);
        assert!(c.insert("PV:A", 1));
        assert!(c.insert("PV:B", 2));
        assert!(!c.insert("PV:C", 3));
        assert!(c.is_full());
    }

    #[test]
    fn test_hit_ratio() {
        let mut c = PvCache::new();
        assert_eq!(c.hit_ratio(), 1.0); // no lookups
        c.hits = 90;
        c.misses = 10;
        assert!((c.hit_ratio() - 0.9).abs() < 0.001);
        assert_eq!(c.total_lookups(), 100);
    }

    #[test]
    fn test_utilization() {
        let mut c = PvCache::with_limits(0, 10);
        assert_eq!(c.utilization(), 0.0);
        for i in 0..5 {
            c.insert(format!("PV:{i}"), i);
        }
        assert!((c.utilization() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_mem_bytes() {
        let mut c = PvCache::new();
        assert_eq!(c.mem_bytes(), 0);
        c.insert("PV:A", 1);
        assert_eq!(c.mem_bytes(), 90);
    }

    #[test]
    fn test_clear() {
        let mut c = PvCache::new();
        c.insert("PV:A", 1);
        c.hits = 50;
        c.misses = 5;
        c.saturations = 1;
        c.clear();
        assert!(c.is_empty());
        assert_eq!(c.hits(), 0);
        assert_eq!(c.resolve_cached("PV:A"), None);
    }

    #[test]
    fn test_iter() {
        let mut c = PvCache::new();
        c.insert("PV:A", 1);
        c.insert("PV:B", 2);
        let mut pairs: Vec<_> = c.iter().collect();
        pairs.sort_by_key(|p| p.0);
        assert_eq!(pairs, vec![("PV:A", 1), ("PV:B", 2)]);
    }

    #[test]
    fn test_upsert_sql() {
        let sql = UPSERT_SQL.to_uppercase();
        assert!(sql.contains("RETURNING PV_ID"));
        assert!(sql.contains("DO UPDATE")); // not DO NOTHING - ensures RETURNING works
    }

    #[test]
    fn test_simulated_hot_path() {
        let mut c = PvCache::new();
        for (i, pv) in ["CRYO:T", "MAG:I", "VAC:P"].iter().enumerate() {
            c.insert(*pv, i as i32 + 1);
            c.misses += 1;
        }
        for _ in 0..997 {
            for &pv in &["CRYO:T", "MAG:I", "VAC:P"] {
                assert!(c.resolve_cached(pv).is_some());
                c.hits += 1;
            }
        }
        assert!(c.hit_ratio() > 0.998);
    }

    #[test]
    fn test_display() {
        let mut c = PvCache::with_limits(0, 1000);
        c.insert("PV:A", 1);
        c.hits = 90;
        c.misses = 10;
        let s = c.to_string();
        assert!(s.contains("1/1000") && s.contains("90 hits"));
    }

    #[test]
    fn test_debug() {
        assert!(format!("{:?}", PvCache::new()).contains("PvCache"));
    }
}
