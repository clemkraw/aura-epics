//! Event handlers and helpers extracted from main select! loop.
//!
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

/// Seconds without data before a PV is considered timed out.
const PV_TIMEOUT_SECS: i64 = 60;

/// Result of a batch subscribe operation.
pub struct SubscribeResult {
    /// PVs successfully subscribed.
    pub ok_pvs: Vec<String>,
    /// Monitor handles for the new subscriptions.
    pub handles: Vec<aura_net::MonitorHandle>,
    /// Number of PVs that failed to subscribe.
    pub failed: usize,
}

/// Subscribe to PVs, register in pv_lookup, update pv_cache, log pv_status + pv_events.
///
/// This is the single source of truth for the subscribe flow.
/// Called from: startup, Redis cmd handler, pg_notify handler.
pub async fn subscribe_pvs(
    pool: &PgPool,
    driver: &mut aura_net::PvaDriver,
    engine: &mut aura_ingest::engine::IngestEngine,
    pv_cache: &Arc<arc_swap::ArcSwap<HashMap<Arc<str>, i32>>>,
    pvs: &[String],
    detail: &str,
) -> SubscribeResult {
    if pvs.is_empty() {
        return SubscribeResult {
            ok_pvs: vec![],
            handles: vec![],
            failed: 0,
        };
    }

    // Pre-register in pv_lookup so pv_id is resolvable when first events arrive.
    let names: Vec<&str> = pvs.iter().map(|s| s.as_str()).collect();
    if let Err(e) = sqlx::query(
        "INSERT INTO pv_lookup (pv_name) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING",
    )
    .bind(&names)
    .execute(pool)
    .await
    {
        tracing::debug!("pre-register pv_lookup: {e}");
    }

    // Update pv_cache (ArcSwap) so ingest threads can resolve pv_id immediately.
    if let Ok(rows) = sqlx::query_as::<_, (String, i32)>(
        "SELECT pv_name, pv_id FROM pv_lookup WHERE pv_name = ANY($1::text[])",
    )
    .bind(&names)
    .fetch_all(pool)
    .await
    {
        if !rows.is_empty() {
            let mut new_map = (**pv_cache.load()).clone();
            for (name, id) in &rows {
                new_map.insert(Arc::from(name.as_str()), *id);
            }
            pv_cache.store(Arc::new(new_map));
        }
    }

    // Subscribe via PVA driver.
    let results = driver.monitor_batch(pvs).await;
    let mut ok_pvs = Vec::new();
    let mut handles = Vec::new();
    let mut failed = 0usize;
    for (pv, res) in results {
        match res {
            Ok(handle) => {
                engine.register_pv(&pv);
                ok_pvs.push(pv);
                handles.push(handle);
            }
            Err(e) => {
                tracing::debug!(pv = %pv, error = %e, "subscribe failed");
                failed += 1;
            }
        }
    }

    if ok_pvs.is_empty() {
        return SubscribeResult {
            ok_pvs,
            handles,
            failed,
        };
    }

    // Final pv_cache refresh (picks up any newly auto-incremented pv_ids).
    let ok_names: Vec<&str> = ok_pvs.iter().map(|s| s.as_str()).collect();
    if let Ok(rows) = sqlx::query_as::<_, (String, i32)>(
        "SELECT pv_name, pv_id FROM pv_lookup WHERE pv_name = ANY($1::text[])",
    )
    .bind(&ok_names)
    .fetch_all(pool)
    .await
    {
        if !rows.is_empty() {
            let pv_ids: Vec<i32> = rows.iter().map(|(_, id)| *id).collect();
            let row_names: Vec<&str> = rows.iter().map(|(n, _)| n.as_str()).collect();
            {
                let mut new_map = (**pv_cache.load()).clone();
                for (name, id) in &rows {
                    new_map.insert(Arc::from(name.as_str()), *id);
                }
                pv_cache.store(Arc::new(new_map));
            }

            // Update pv_status + log pv_events.
            if let Err(e) = sqlx::query(
                "INSERT INTO pv_status (pv_name, pv_id, state, subscribed_at) \
                 SELECT t.pv_name, t.pv_id, 3, NOW() \
                 FROM UNNEST($1::text[], $2::int[]) AS t(pv_name, pv_id) \
                 ON CONFLICT (pv_name) DO UPDATE SET state = 3, pv_id = EXCLUDED.pv_id, subscribed_at = NOW()"
            ).bind(&row_names).bind(&pv_ids).execute(pool).await {
                tracing::debug!("pv_status subscribe: {e}");
            }

            if let Err(e) = sqlx::query(
                "INSERT INTO pv_events (pv_name, pv_id, event_type, detail) \
                 SELECT t.pv_name, t.pv_id, 0, $3 \
                 FROM UNNEST($1::text[], $2::int[]) AS t(pv_name, pv_id)",
            )
            .bind(&row_names)
            .bind(&pv_ids)
            .bind(detail)
            .execute(pool)
            .await
            {
                tracing::debug!("pv_events subscribe: {e}");
            }
        }
    }

    SubscribeResult {
        ok_pvs,
        handles,
        failed,
    }
}

/// Drain metadata from driver, store via MetadataDao.
/// Returns number of metadata entries stored.
pub async fn collect_initial_metadata(
    pool: &PgPool,
    driver: &aura_net::PvaDriver,
    engine: &mut aura_ingest::engine::IngestEngine,
    metadata_stored_pvs: &mut HashSet<String>,
    expected: usize,
    timeout_secs: u64,
) -> usize {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    while engine.pending_metadata.len() < expected && tokio::time::Instant::now() < deadline {
        let metas = driver.drain_metadata();
        for (pv_name, value) in metas {
            use aura_net::monitor::subscription::MonitorEvent;
            engine.process_event(&pv_name, MonitorEvent::Value(value));
        }
        if engine.pending_metadata.len() >= expected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let collected = engine.pending_metadata.len();
    if collected == 0 {
        return 0;
    }

    let metas = std::mem::take(&mut engine.pending_metadata);
    let stored: Vec<aura_store::metadata::StoredMetadata> = metas
        .iter()
        .map(|m| aura_store::metadata::StoredMetadata::from_core(m))
        .collect();

    match aura_store::metadata::MetadataDao::upsert_batch(pool, &stored).await {
        Ok(n) => {
            for m in &stored {
                metadata_stored_pvs.insert(m.pv_name.clone());
            }
            tracing::info!(count = n, "metadata stored");
            n
        }
        Err(e) => {
            tracing::error!("metadata upsert: {e}");
            0
        }
    }
}

/// Handle IOC disconnect: log pv_events, insert DISCONNECT samples, update pv_status.
pub async fn handle_disconnect(
    pool: &PgPool,
    pv_cache: &arc_swap::ArcSwap<HashMap<Arc<str>, i32>>,
    shared_buf: &aura_store::writer::shared_buf::SharedBuffer,
    addr: SocketAddr,
    pvs: &[String],
    reason: &str,
) {
    let names: Vec<&str> = pvs.iter().map(|s| s.as_str()).collect();
    let addr_str = addr.to_string();

    if let Err(e) = sqlx::query(
        "INSERT INTO pv_events (pv_name, pv_id, event_type, ioc_addr, detail) \
         SELECT t.pv_name, l.pv_id, 3, $2, $3 \
         FROM UNNEST($1::text[]) AS t(pv_name) \
         LEFT JOIN pv_lookup l ON l.pv_name = t.pv_name",
    )
    .bind(&names)
    .bind(&addr_str)
    .bind(reason)
    .execute(pool)
    .await
    {
        tracing::debug!("pv_events disconnect insert: {e}");
    }

    let cache = pv_cache.load();
    for pv in pvs {
        if let Some(&pv_id) = cache.get(pv.as_str()) {
            shared_buf.push_scalar(aura_store::writer::scalar::ScalarRow::new(
                chrono::Utc::now(),
                pv_id,
                f64::NAN,
                3,
                103,
                aura_core::sample::StoreReason::Disconnected,
            ));
        }
    }

    if let Err(e) = sqlx::query("UPDATE pv_status SET state = 4 WHERE pv_name = ANY($1::text[])")
        .bind(&names)
        .execute(pool)
        .await
    {
        tracing::debug!("pv_status disconnect update: {e}");
    }
}

/// Handle IOC reconnect: log pv_events, insert RECONNECT samples, update pv_status.
pub async fn handle_reconnect(
    pool: &PgPool,
    pv_cache: &arc_swap::ArcSwap<HashMap<Arc<str>, i32>>,
    shared_buf: &aura_store::writer::shared_buf::SharedBuffer,
    addr: SocketAddr,
    pvs: &[String],
) {
    let names: Vec<&str> = pvs.iter().map(|s| s.as_str()).collect();
    let addr_str = addr.to_string();

    if let Err(e) = sqlx::query(
        "INSERT INTO pv_events (pv_name, pv_id, event_type, ioc_addr) \
         SELECT t.pv_name, l.pv_id, 4, $2 \
         FROM UNNEST($1::text[]) AS t(pv_name) \
         LEFT JOIN pv_lookup l ON l.pv_name = t.pv_name",
    )
    .bind(&names)
    .bind(&addr_str)
    .execute(pool)
    .await
    {
        tracing::debug!("pv_events reconnect insert: {e}");
    }

    let cache = pv_cache.load();
    for pv in pvs {
        if let Some(&pv_id) = cache.get(pv.as_str()) {
            shared_buf.push_scalar(aura_store::writer::scalar::ScalarRow::new(
                chrono::Utc::now(),
                pv_id,
                f64::NAN,
                3,
                104,
                aura_core::sample::StoreReason::Reconnected,
            ));
        }
    }

    if let Err(e) = sqlx::query(
        "UPDATE pv_status SET state = 3, connected_at = NOW() WHERE pv_name = ANY($1::text[])",
    )
    .bind(&names)
    .execute(pool)
    .await
    {
        tracing::debug!("pv_status reconnect update: {e}");
    }
}

/// Drain metadata from sessions (metadata_buf) and ingest threads (inline_meta_store).
pub async fn drain_metadata(
    pool: &PgPool,
    driver: &aura_net::PvaDriver,
    inline_meta_store: &Mutex<Vec<aura_core::metadata::PvMetadata>>,
    metadata_stored_pvs: &mut HashSet<String>,
) {
    // Source 1: metadata_buf from sessions.
    let raw_metas = driver.drain_metadata();
    if !raw_metas.is_empty() {
        let mut stored = Vec::with_capacity(raw_metas.len());
        for (pv_name, value) in raw_metas {
            let mut conv = aura_ingest::converter::PvConverter::new(Arc::clone(&pv_name));
            use aura_net::monitor::subscription::MonitorEvent;
            if let aura_ingest::converter::ConvertResult::UpdateWithMetadata(_, meta) =
                conv.convert(MonitorEvent::Value(value))
            {
                stored.push(aura_store::metadata::StoredMetadata::from_core(&meta));
            }
        }
        stored.retain(|m| !metadata_stored_pvs.contains(&m.pv_name));
        if !stored.is_empty() {
            let new_names: Vec<&str> = stored.iter().map(|m| m.pv_name.as_str()).collect();
            let new_ids: Vec<Option<i32>> = stored.iter().map(|m| m.pv_id).collect();

            if let Err(e) = sqlx::query(
                "INSERT INTO pv_status (pv_name, pv_id, state, subscribed_at) \
                 SELECT t.pv_name, t.pv_id, 3, NOW() \
                 FROM UNNEST($1::text[], $2::int[]) AS t(pv_name, pv_id) \
                 ON CONFLICT (pv_name) DO UPDATE SET state = 3",
            )
            .bind(&new_names)
            .bind(&new_ids)
            .execute(pool)
            .await
            {
                tracing::debug!("pv_status insert: {e}");
            }

            if let Err(e) = sqlx::query(
                "INSERT INTO pv_events (pv_name, event_type, detail) \
                 SELECT unnest($1::text[]), 2, 'first value received'",
            )
            .bind(&new_names)
            .execute(pool)
            .await
            {
                tracing::debug!("pv_events first_value insert: {e}");
            }

            match aura_store::metadata::MetadataDao::upsert_batch(pool, &stored).await {
                Ok(n) => {
                    for m in &stored {
                        metadata_stored_pvs.insert(m.pv_name.clone());
                    }
                    tracing::info!(count = n, "metadata stored (background)");
                }
                Err(e) => tracing::warn!("metadata upsert: {e}"),
            }
        }
    }

    // Source 2: inline_meta_store from ingest threads.
    let metas: Vec<aura_core::metadata::PvMetadata> = {
        if let Ok(mut store) = inline_meta_store.lock() {
            std::mem::take(&mut *store)
        } else {
            Vec::new()
        }
    };
    if !metas.is_empty() {
        let stored: Vec<aura_store::metadata::StoredMetadata> = metas
            .iter()
            .map(|m| aura_store::metadata::StoredMetadata::from_core(m))
            .filter(|m| !metadata_stored_pvs.contains(&m.pv_name))
            .collect();
        if !stored.is_empty() {
            match aura_store::metadata::MetadataDao::upsert_batch(pool, &stored).await {
                Ok(n) => {
                    for m in &stored {
                        metadata_stored_pvs.insert(m.pv_name.clone());
                    }
                    tracing::info!(count = n, "metadata stored (inline)");
                }
                Err(e) => tracing::warn!("inline metadata upsert: {e}"),
            }
        }
    }
}

/// Drain per-PV stats, update pv_status, detect timeouts, compute health score.
pub async fn update_pv_status(
    pool: &PgPool,
    pv_stats_sink: &Mutex<Vec<HashMap<i32, (u64, f64, i16)>>>,
    pv_cache: &arc_swap::ArcSwap<HashMap<Arc<str>, i32>>,
    shared_buf: &aura_store::writer::shared_buf::SharedBuffer,
) {
    let shards: Vec<HashMap<i32, (u64, f64, i16)>> = {
        if let Ok(mut sink) = pv_stats_sink.lock() {
            std::mem::take(&mut *sink)
        } else {
            Vec::new()
        }
    };
    if !shards.is_empty() {
        let mut merged: HashMap<i32, (u64, f64, i16)> = HashMap::new();
        for shard in shards {
            for (pv_id, (count, value, severity)) in shard {
                merged
                    .entry(pv_id)
                    .and_modify(|e| {
                        e.0 += count;
                        e.1 = value;
                        e.2 = severity;
                    })
                    .or_insert((count, value, severity));
            }
        }
        let pv_ids: Vec<i32> = merged.keys().copied().collect();
        let counts: Vec<i64> = merged.values().map(|v| v.0 as i64).collect();
        let values: Vec<f64> = merged.values().map(|v| v.1).collect();
        let sevs: Vec<i16> = merged.values().map(|v| v.2).collect();
        let hzs: Vec<f32> = merged.values().map(|v| v.0 as f32 / 30.0).collect();

        if let Err(e) = sqlx::query(
            "UPDATE pv_status s SET \
               last_event_at = NOW(), events_total = s.events_total + t.cnt, \
               update_hz = t.hz, last_value = t.val, last_severity = t.sev \
             FROM UNNEST($1::int[], $2::bigint[], $3::real[], $4::float8[], $5::smallint[]) \
               AS t(pv_id, cnt, hz, val, sev) WHERE s.pv_id = t.pv_id",
        )
        .bind(&pv_ids)
        .bind(&counts)
        .bind(&hzs)
        .bind(&values)
        .bind(&sevs)
        .execute(pool)
        .await
        {
            tracing::debug!("pv_status stats update: {e}");
        }
    }

    // Timeout detection.
    let timeout_rows = sqlx::query_as::<_, (String,)>(
        "UPDATE pv_status SET state = 5 \
         WHERE state = 3 AND last_event_at IS NOT NULL \
           AND last_event_at < NOW() - make_interval(secs => $1) \
         RETURNING pv_name",
    )
    .bind(PV_TIMEOUT_SECS as f64)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    if !timeout_rows.is_empty() {
        let timeout_pvs: Vec<&str> = timeout_rows.iter().map(|(n,)| n.as_str()).collect();

        if let Err(e) = sqlx::query(
            "INSERT INTO pv_events (pv_name, event_type, detail) \
             SELECT unnest($1::text[]), 5, 'no data for 60s'",
        )
        .bind(&timeout_pvs)
        .execute(pool)
        .await
        {
            tracing::debug!("pv_events timeout insert: {e}");
        }

        let cache = pv_cache.load();
        for pv in &timeout_pvs {
            if let Some(&pv_id) = cache.get(*pv) {
                shared_buf.push_scalar(aura_store::writer::scalar::ScalarRow::new(
                    chrono::Utc::now(),
                    pv_id,
                    f64::NAN,
                    3,
                    105,
                    aura_core::sample::StoreReason::Timeout,
                ));
            }
        }
        tracing::warn!(
            count = timeout_pvs.len(),
            "PVs timed out (no data {PV_TIMEOUT_SECS}s)"
        );
    }

    // Health score.
    if let Err(e) = sqlx::query(
        "UPDATE pv_status SET health_score = \
           CASE WHEN state = 3 THEN \
             LEAST(1.0, EXTRACT(EPOCH FROM (NOW() - COALESCE(connected_at, subscribed_at))) \
               / GREATEST(1, EXTRACT(EPOCH FROM (NOW() - subscribed_at)))) \
             * CASE WHEN last_event_at > NOW() - INTERVAL '60 seconds' THEN 1.0 \
                    WHEN last_event_at > NOW() - INTERVAL '300 seconds' THEN 0.5 \
                    ELSE 0.1 END \
           WHEN state = 4 THEN 0.0 \
           WHEN state = 5 THEN 0.1 \
           ELSE 0.5 END",
    )
    .execute(pool)
    .await
    {
        tracing::debug!("health score update: {e}");
    }
}
