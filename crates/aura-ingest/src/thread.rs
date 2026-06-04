//! Ingest thread body — runs on dedicated std::thread (not tokio) for zero-async overhead.
//! Each shard drains events from the MonitorBus, decodes via ScalarDelta fast path,
//! and pushes WriterRows to the SharedBuffer.

use crate::engine::IngestEngine;
use aura_core::config::AuraConfig;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

/// Run one ingest shard: drain events from bus, decode, push to SharedBuffer.
pub fn run_ingest_shard(
    cfg: AuraConfig,
    pv_names: Vec<String>,
    mut shard_rx: aura_net::monitor::bus::MonitorBusRx,
    shared_buf: Arc<aura_store::writer::shared_buf::SharedBuffer>,
    pv_cache: Arc<arc_swap::ArcSwap<HashMap<Arc<str>, i32>>>,
    meta_store: Arc<Mutex<Vec<aura_core::metadata::PvMetadata>>>,
    stats_sink: Arc<Mutex<Vec<HashMap<i32, (u64, f64, i16)>>>>,
    stat_events: Arc<AtomicU64>,
    stat_published: Arc<AtomicU64>,
    stat_fast_path: Arc<AtomicU64>,
    stat_full_decode: Arc<AtomicU64>,
    stat_skipped: Arc<AtomicU64>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    heartbeat_config: Arc<arc_swap::ArcSwap<Vec<(i32, f32)>>>,
) {
    let default_heartbeat_s = cfg.ingest.default_heartbeat_s;
    let mut engine = IngestEngine::new();
    for name in &pv_names {
        engine.register_pv(name);
    }

    let mut event_buf: Vec<aura_net::TaggedEvent> = Vec::with_capacity(4096);
    let mut local_pv_stats: Vec<(u64, f64, i16)> = Vec::new();
    let mut last_stats_flush = std::time::Instant::now();
    let mut idle_count = 0u32;

    let mut heartbeat =
        crate::heartbeat::HeartbeatTracker::new(default_heartbeat_s, heartbeat_config);
    let mut last_heartbeat_scan = std::time::Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        let mut local_se = 0u64;
        let mut local_sp = 0u64;
        let mut local_sfp = 0u64;
        let mut local_sfd = 0u64;

        event_buf.clear();
        shard_rx.drain_into(&mut event_buf, 65536);

        let batch_now = std::time::Instant::now();
        let cache = pv_cache.load();

        for tagged in event_buf.drain(..) {
            // Fast path: ScalarDelta bypasses converter entirely.
            if let aura_net::monitor::subscription::MonitorEvent::ScalarDelta {
                value,
                seconds,
                nanos,
                severity,
                status,
            } = &tagged.event
            {
                let pv_id = if tagged.pv_id > 0 {
                    tagged.pv_id
                } else if let Some(&id) = cache.get(&*tagged.pv_name) {
                    id
                } else {
                    continue;
                };
                shared_buf.push_scalar(aura_store::writer::scalar::ScalarRow::from_epoch(
                    *seconds,
                    *nanos,
                    pv_id,
                    *value,
                    *severity as i16,
                    *status as i16,
                    aura_core::sample::StoreReason::EpsilonExceeded,
                ));
                let idx = pv_id as usize;
                if idx <= 1_000_000 {
                    if idx >= local_pv_stats.len() {
                        local_pv_stats.resize(idx + 1, (0, 0.0, 0));
                    }
                    let e = &mut local_pv_stats[idx];
                    e.0 += 1;
                    e.1 = *value;
                    e.2 = *severity as i16;
                }
                heartbeat.record_store(pv_id, *value, *severity as i16, *status as i16, batch_now);
                local_se += 1;
                local_sp += 1;
                local_sfp += 1;
                continue;
            }

            // Fast path: StringDelta
            if let aura_net::monitor::subscription::MonitorEvent::StringDelta {
                value,
                seconds,
                nanos,
                severity,
                status,
            } = &tagged.event
            {
                let pv_id = if tagged.pv_id > 0 {
                    tagged.pv_id
                } else if let Some(&id) = cache.get(&*tagged.pv_name) {
                    id
                } else {
                    continue;
                };
                shared_buf.push_other(aura_store::writer::shared_buf::WriterRow::String(
                    aura_store::writer::string::StringRow::from_epoch(
                        *seconds,
                        *nanos,
                        pv_id,
                        value.clone(),
                        *severity as i16,
                        *status as i16,
                    ),
                ));
                local_se += 1;
                local_sp += 1;
                local_sfp += 1;
                continue;
            }

            // Destructure to take ownership (ArrayDelta moves Vec<f64> without clone).
            let aura_net::TaggedEvent {
                pv_name: tag_pv,
                pv_id: tag_pid,
                event: tag_ev,
            } = tagged;

            match tag_ev {
                // Fast path: ArrayDelta
                aura_net::monitor::subscription::MonitorEvent::ArrayDelta {
                    values,
                    seconds,
                    nanos,
                    severity,
                    status,
                } => {
                    let pv_id = if tag_pid > 0 {
                        tag_pid
                    } else if let Some(&id) = cache.get(&*tag_pv) {
                        id
                    } else {
                        continue;
                    };
                    let time =
                        chrono::DateTime::from_timestamp(seconds, nanos as u32).unwrap_or_default();
                    shared_buf.push_other(aura_store::writer::shared_buf::WriterRow::Array(
                        Box::new(aura_store::writer::array::ArrayCapture {
                            time,
                            pv_id,
                            severity: severity as i16,
                            status: status as i16,
                            data: aura_store::writer::array::ArrayData::Numeric(values),
                        }),
                    ));
                    local_se += 1;
                    local_sp += 1;
                    local_sfp += 1;
                }

                // Slow path - all other types go through converter.
                slow_ev => {
                    local_sfd += 1;
                    if let crate::engine::ProcessResult::Sample(mut update) =
                        engine.process_event(&tag_pv, slow_ev)
                    {
                        let pv_id = if tag_pid > 0 {
                            tag_pid
                        } else if let Some(&id) = cache.get(&*update.pv_name) {
                            id
                        } else {
                            continue;
                        };

                        use aura_core::pva::PvDataType;
                        use aura_core::pva::normative::NormativeType;
                        use aura_store::writer::shared_buf::WriterRow;
                        let time = update.timestamp();
                        let severity = update.severity();
                        let status = update.status();

                        match &mut update.data {
                            NormativeType::NTScalar(nt) => {
                                shared_buf.push_scalar(aura_store::writer::scalar::ScalarRow::new(
                                    nt.timestamp.to_datetime(),
                                    pv_id,
                                    nt.value.as_f64().unwrap_or(0.0),
                                    nt.alarm.severity as i16,
                                    nt.alarm.status as i16,
                                    aura_core::sample::StoreReason::EpsilonExceeded,
                                ));
                            }
                            NormativeType::NTEnum(nt) => {
                                shared_buf.push_scalar(aura_store::writer::scalar::ScalarRow::new(
                                    nt.timestamp.to_datetime(),
                                    pv_id,
                                    nt.value.as_f64(),
                                    nt.alarm.severity as i16,
                                    nt.alarm.status as i16,
                                    aura_core::sample::StoreReason::EpsilonExceeded,
                                ));
                            }
                            NormativeType::NTScalarArray(a) => {
                                let values = a.value.as_f64_vec().unwrap_or_default();
                                shared_buf.push_other(WriterRow::Array(Box::new(
                                    aura_store::writer::array::ArrayCapture {
                                        time,
                                        pv_id,
                                        severity,
                                        status,
                                        data: aura_store::writer::array::ArrayData::Numeric(values),
                                    },
                                )));
                            }
                            NormativeType::NTMatrix(m) => {
                                let values = m.value.clone();
                                shared_buf.push_other(WriterRow::Array(Box::new(
                                    aura_store::writer::array::ArrayCapture {
                                        time,
                                        pv_id,
                                        severity,
                                        status,
                                        data: aura_store::writer::array::ArrayData::Numeric(values),
                                    },
                                )));
                            }
                            NormativeType::NTNDArray(img) => {
                                let data = match &mut img.value {
                                    aura_core::pva::ArrayValue::UByteArray(v) => std::mem::take(v),
                                    _ => Vec::new(),
                                };
                                let dims: Vec<i32> = img.dimension.iter().map(|d| d.size).collect();
                                let codec = std::mem::take(&mut img.codec.name);
                                shared_buf.push_other(WriterRow::Image(
                                    aura_store::writer::image::ImageRow::new(
                                        time,
                                        pv_id,
                                        data,
                                        codec,
                                        img.compressed_size,
                                        img.uncompressed_size,
                                        dims,
                                        img.unique_id,
                                        severity,
                                        status,
                                    ),
                                ));
                            }
                            _ => {
                                let data_type = PvDataType::from_nt(&update.data);
                                match data_type {
                                    PvDataType::Table => {
                                        if let Ok(bytes) = serde_json::to_vec(&update.data) {
                                            shared_buf.push_other(WriterRow::Json(
                                                aura_store::writer::json::JsonRow::table(
                                                    time, pv_id, bytes, severity, status,
                                                ),
                                            ));
                                        }
                                    }
                                    PvDataType::Custom | PvDataType::Union => {
                                        if let Ok(bytes) = serde_json::to_vec(&update.data) {
                                            let nt = update.data.type_name();
                                            shared_buf.push_other(WriterRow::Json(
                                                aura_store::writer::json::JsonRow::custom(
                                                    time, pv_id, nt, bytes, severity, status,
                                                ),
                                            ));
                                        }
                                    }
                                    _ => {
                                        if let Ok(data) = serde_json::to_value(&update.data) {
                                            let row = match data_type {
                                                PvDataType::Histogram => {
                                                    aura_store::writer::json::JsonRow::histogram(
                                                        time, pv_id, data, severity, status,
                                                    )
                                                }
                                                PvDataType::Continuum => {
                                                    aura_store::writer::json::JsonRow::continuum(
                                                        time, pv_id, data, severity, status,
                                                    )
                                                }
                                                PvDataType::NameValue => {
                                                    aura_store::writer::json::JsonRow::namevalue(
                                                        time, pv_id, data, severity, status,
                                                    )
                                                }
                                                PvDataType::MultiChannel => {
                                                    aura_store::writer::json::JsonRow::multi(
                                                        time, pv_id, data, severity, status,
                                                    )
                                                }
                                                _ => continue,
                                            };
                                            shared_buf.push_other(WriterRow::Json(row));
                                        }
                                    }
                                }
                            }
                        }
                        local_sp += 1;
                    }
                }
            }
        }

        // Batch atomic flush.
        stat_events.fetch_add(engine.total_events + local_se, Ordering::Relaxed);
        stat_published.fetch_add(engine.total_published + local_sp, Ordering::Relaxed);
        stat_fast_path.fetch_add(local_sfp, Ordering::Relaxed);
        stat_full_decode.fetch_add(local_sfd, Ordering::Relaxed);
        stat_skipped.fetch_add(engine.total_skipped, Ordering::Relaxed);
        engine.total_events = 0;
        engine.total_published = 0;
        engine.total_skipped = 0;

        if !engine.pending_metadata.is_empty() {
            if let Ok(mut store) = meta_store.lock() {
                store.append(&mut engine.pending_metadata);
            }
        }

        if last_stats_flush.elapsed() > std::time::Duration::from_secs(30) {
            let mut snapshot: HashMap<i32, (u64, f64, i16)> = HashMap::new();
            for (pv_id, entry) in local_pv_stats.iter().enumerate() {
                if entry.0 > 0 {
                    snapshot.insert(pv_id as i32, *entry);
                }
            }
            if !snapshot.is_empty() {
                if let Ok(mut sink) = stats_sink.lock() {
                    sink.push(snapshot);
                }
                for e in local_pv_stats.iter_mut() {
                    e.0 = 0;
                }
            }
            last_stats_flush = std::time::Instant::now();
        }

        // Heartbeat scan.
        if !heartbeat.is_disabled()
            && last_heartbeat_scan.elapsed()
                >= std::time::Duration::from_secs(crate::heartbeat::SCAN_INTERVAL_SECS)
        {
            let active_ids: std::collections::HashSet<i32> =
                pv_cache.load().values().copied().collect();
            let hb_count = heartbeat.emit_heartbeats(&shared_buf, &active_ids);
            if hb_count > 0 {
                local_sp += hb_count as u64;
            }
            last_heartbeat_scan = std::time::Instant::now();
        }

        if local_se == 0 && local_sfd == 0 {
            idle_count += 1;
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
            if idle_count > 100 {
                std::thread::sleep(std::time::Duration::from_micros(500));
            } else {
                std::thread::yield_now();
            }
        } else {
            idle_count = 0;
        }
    }
}

/// Shared state for spawning ingest threads.
pub struct IngestContext {
    pub ingest_threads: usize,
    pub config: AuraConfig,
    pub shared_bufs: Vec<Arc<aura_store::writer::shared_buf::SharedBuffer>>,
    pub shared_pv_cache: Arc<arc_swap::ArcSwap<HashMap<Arc<str>, i32>>>,
    pub inline_meta_store: Arc<Mutex<Vec<aura_core::metadata::PvMetadata>>>,
    pub pv_stats_sink: Arc<Mutex<Vec<HashMap<i32, (u64, f64, i16)>>>>,
    pub stat_events: Arc<AtomicU64>,
    pub stat_published: Arc<AtomicU64>,
    pub stat_fast_path: Arc<AtomicU64>,
    pub stat_full_decode: Arc<AtomicU64>,
    pub stat_skipped: Arc<AtomicU64>,
    pub shutdown: Arc<std::sync::atomic::AtomicBool>,
    pub heartbeat_config: Arc<arc_swap::ArcSwap<Vec<(i32, f32)>>>,
}

/// Spawn N ingest threads from shard assignments.
pub fn spawn_ingest_threads(
    ctx: &IngestContext,
    shard_pvs: &mut Vec<Vec<String>>,
    bus_rxs: &mut Vec<aura_net::monitor::bus::MonitorBusRx>,
) -> Vec<std::thread::JoinHandle<()>> {
    let mut handles = Vec::new();
    for shard_id in 0..ctx.ingest_threads {
        let cfg = ctx.config.clone();
        let pv_names = shard_pvs.remove(0);
        let n_pvs = pv_names.len();
        let shard_rx = bus_rxs.remove(0);
        let shared_buf = Arc::clone(&ctx.shared_bufs[shard_id]);
        let pv_cache = ctx.shared_pv_cache.clone();
        let meta_store = ctx.inline_meta_store.clone();
        let stats_sink = ctx.pv_stats_sink.clone();
        let se = ctx.stat_events.clone();
        let sp = ctx.stat_published.clone();
        let sfp = ctx.stat_fast_path.clone();
        let sfd = ctx.stat_full_decode.clone();
        let ss = ctx.stat_skipped.clone();
        let shutdown = ctx.shutdown.clone();
        let heartbeat_config = ctx.heartbeat_config.clone();

        let handle = std::thread::Builder::new()
            .name(format!("ingest-{shard_id}"))
            .spawn(move || {
                run_ingest_shard(
                    cfg,
                    pv_names,
                    shard_rx,
                    shared_buf,
                    pv_cache,
                    meta_store,
                    stats_sink,
                    se,
                    sp,
                    sfp,
                    sfd,
                    ss,
                    shutdown,
                    heartbeat_config,
                )
            })
            .expect("failed to spawn ingest thread");
        handles.push(handle);
        eprintln!("  [thr]  Ingest shard {shard_id} started ({n_pvs} PVs)");
    }
    handles
}

/// Compute shard assignments and spawn ingest threads.
pub fn setup_shards_and_spawn(
    monitors: &mut Vec<aura_net::monitor::MonitorHandle>,
    ingest_threads: usize,
    bus_rxs: &mut Vec<aura_net::monitor::bus::MonitorBusRx>,
    ctx: &IngestContext,
) -> (
    Vec<std::thread::JoinHandle<()>>,
    Vec<tokio_util::sync::CancellationToken>,
) {
    let all_pv_names: Vec<String> = monitors.iter().map(|h| h.pv_name().to_string()).collect();
    let mut shard_pvs: Vec<Vec<String>> = (0..ingest_threads).map(|_| Vec::new()).collect();
    for name in &all_pv_names {
        let s = aura_net::monitor::bus::shard_for_pv(name, ingest_threads);
        shard_pvs[s].push(name.clone());
    }
    drop(all_pv_names);

    let cancel_tokens: Vec<tokio_util::sync::CancellationToken> = std::mem::take(monitors)
        .into_iter()
        .map(|h| h.cancel_token())
        .collect();

    let handles = spawn_ingest_threads(ctx, &mut shard_pvs, bus_rxs);
    (handles, cancel_tokens)
}