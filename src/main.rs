mod banner;
mod handlers;

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;

use aura_core::config::AuraConfig;
use aura_core::sample::PvUpdate;
use aura_ingest::engine::IngestEngine;
use aura_net::PvaDriver;
use aura_net::client::PvaClientConfig;
use aura_store::pipeline::{Pipeline, PipelineConfig};

/// Format a chunk interval in seconds into a human-readable PostgreSQL INTERVAL string.
fn format_chunk_interval(secs: u64) -> String {
    if secs >= 7200 {
        format!("{} hours", secs / 3600)
    } else if secs >= 3600 {
        "1 hour".to_string()
    } else if secs >= 120 {
        format!("{} minutes", secs / 60)
    } else {
        "1 minute".to_string()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    banner::print_banner();

    // Init tracing - without this, all tracing::info! are silent.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "aura=info,aura_discover=info,aura_ingest=info,aura_store=warn,aura_net=info".into()
            }),
        )
        .init();

    let c_met = "\x1b[38;2;140;140;140m";
    let c_id = "\x1b[38;2;80;160;255m";
    let c_ok = "\x1b[38;2;0;255;150m";
    let c_err = "\x1b[38;2;255;80;80m";
    let c_warn = "\x1b[38;2;255;165;0m";
    let c_val = "\x1b[38;2;180;140;255m";
    let c_rst = "\x1b[0m";

    let t0 = Instant::now();
    let ts = || format!("{:.6}", t0.elapsed().as_secs_f64());

    println!(
        "{c_met}[{}]{c_rst} {c_id}cfg{c_rst}  Loading configuration...",
        ts()
    );

    let config_path = std::env::args()
        .skip_while(|a| a != "--config")
        .nth(1)
        .unwrap_or_else(|| "config/aura.toml".to_string());

    let config_str = match std::fs::read_to_string(&config_path) {
        Ok(s) => s,
        Err(e) => {
            println!(
                "{c_met}[{}]{c_rst} {c_err}cfg  FATAL: cannot read {config_path}: {e}{c_rst}",
                ts()
            );
            std::process::exit(1);
        }
    };
    let config: AuraConfig = match toml::from_str(&config_str) {
        Ok(c) => c,
        Err(e) => {
            println!(
                "{c_met}[{}]{c_rst} {c_err}cfg  FATAL: parse error: {e}{c_rst}",
                ts()
            );
            std::process::exit(1);
        }
    };

    println!(
        "{c_met}[{}]{c_rst} {c_id}cfg{c_rst}  Loaded {c_val}{config_path}{c_rst}",
        ts()
    );

    println!(
        "{c_met}[{}]{c_rst} {c_id}rds{c_rst}  Connecting to Redis {c_val}{}{c_rst}...",
        ts(),
        config.redis.url
    );

    let redis_client = redis::Client::open(config.redis.url.as_str())?;

    println!(
        "{c_met}[{}]{c_rst} {c_id}db {c_rst}  Connecting to TimescaleDB...",
        ts()
    );

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(config.database.max_connections)
        .connect(&config.database.url)
        .await?;

    println!(
        "{c_met}[{}]{c_rst} {c_ok}db   Connected{c_rst} — pool={c_val}{}{c_rst} connections",
        ts(),
        config.database.max_connections
    );

    println!(
        "{c_met}[{}]{c_rst} {c_id}db {c_rst}  Running migrations...",
        ts()
    );
    let report = aura_store::Migrations::run(&pool).await?;
    println!(
        "{c_met}[{}]{c_rst} {c_ok}db   Migrations OK{c_rst} — applied={c_val}{}{c_rst} skipped={c_val}{}{c_rst}",
        ts(),
        report.applied_count(),
        report.skipped_count()
    );

    println!(
        "{c_met}[{}]{c_rst} {c_id}sto{c_rst}  Building storage pipeline...",
        ts()
    );

    let pipeline_config = PipelineConfig::from_aura_config(&config);
    let mut pipeline = Pipeline::new(pipeline_config);

    // Dynamic core detection - adapt to the hardware.
    let num_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let ingest_threads = (num_cores / 4).max(2).min(16); // 8C → 4 threads au lieu de 8

    // Dynamic buffer sizing: 5% of system RAM, min 2M, max 20M total.
    let total_ram_mb = {
        let info = sys_info::mem_info().unwrap_or(sys_info::MemInfo {
            total: 8_000_000,
            free: 4_000_000,
            avail: 4_000_000,
            buffers: 0,
            cached: 0,
            swap_total: 0,
            swap_free: 0,
        });
        (info.total / 1024) as usize // KB → MB
    };
    let buf_total = (total_ram_mb * 1024 * 1024 / 50 / 32) // 2% of RAM / 32 bytes/row
        .max(2_000_000)
        .min(20_000_000);
    let buf_initial = (buf_total / 4).max(100_000);
    println!(
        "{c_met}[{}]{c_rst} {c_ok}sto  RAM: {} MB — buffer: {}M rows ({} MB per shard × {} shards){c_rst}",
        ts(),
        total_ram_mb,
        buf_total / 1_000_000,
        buf_total / ingest_threads * 32 / 1024 / 1024,
        ingest_threads
    );
    let copy_connections = (num_cores / 2).max(4).min(16);

    println!(
        "{c_met}[{}]{c_rst} {c_id}sto{c_rst}  Detected {c_val}{num_cores}{c_rst} logical CPUs              - {c_val}{ingest_threads}{c_rst} ingest threads,              {c_val}{copy_connections}{c_rst} COPY connections",
        ts()
    );

    for i in 0..copy_connections {
        let (client, connection) =
            tokio_postgres::connect(&config.database.url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("tokio-postgres COPY connection {i} lost: {e}");
            }
        });
        pipeline.add_copy_connection(Arc::new(client));
    }
    pipeline.finalize_copy_pool();

    // Startup the main pipeline (warm cache, claim pending).
    pipeline.startup(&pool).await?;

    println!(
        "{c_met}[{}]{c_rst} {c_id}sto{c_rst}  Architecture: \
             {c_val}{ingest_threads}{c_rst} ingest -> 1 store -> \
             {c_val}{copy_connections}{c_rst} COPY",
        ts()
    );

    let (sample_tx, sample_rx) = tokio::sync::mpsc::channel::<PvUpdate>(100_000);
    let shard_senders: Vec<tokio::sync::mpsc::Sender<PvUpdate>> =
        (0..ingest_threads).map(|_| sample_tx.clone()).collect();
    drop(sample_tx); // Only shard_senders keep the channel alive.

    // Single Notify shared across all shards — any push wakes the store loop.
    let store_notify = Arc::new(tokio::sync::Notify::new());

    // Per-shard SharedBuffers — zero contention between ingest threads.
    let shared_writer_bufs: Vec<Arc<aura_store::writer::shared_buf::SharedBuffer>> = (0
        ..ingest_threads)
        .map(|_| {
            Arc::new(aura_store::writer::shared_buf::SharedBuffer::new(
                buf_initial / ingest_threads,
                buf_total / ingest_threads,
                store_notify.clone(),
            ))
        })
        .collect();
    let shared_writer_bufs_store: Vec<Arc<aura_store::writer::shared_buf::SharedBuffer>> =
        shared_writer_bufs.iter().map(|b| Arc::clone(b)).collect();
    // Single reference for handlers (disconnect/reconnect/timeout samples — rare events).
    let shared_writer_buf = Arc::clone(&shared_writer_bufs[0]);

    let store_pool = pool.clone();
    let redis_url_store = config.redis.url.clone();
    let spill_counter = Arc::new(AtomicU64::new(0));
    let ingest_shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let heartbeat_config: Arc<arc_swap::ArcSwap<Vec<(i32, f32)>>> =
        Arc::new(arc_swap::ArcSwap::from_pointee(Vec::new()));
    let spill_counter_store = spill_counter.clone();
    let store_cancel = tokio_util::sync::CancellationToken::new();
    let store_cancel_trigger = store_cancel.clone();

    let store_handle = tokio::spawn(aura_store::store_loop::run(
        pipeline,
        shared_writer_bufs_store,
        sample_rx,
        store_pool,
        redis_url_store,
        spill_counter_store,
        store_cancel,
        store_notify.clone(),
    ));

    println!(
        "{c_met}[{}]{c_rst} {c_ok}sto  Pipeline running{c_rst} — batch={c_val}{}{c_rst} flush={c_val}{}ms{c_rst}",
        ts(),
        config.store.batch_size,
        config.store.flush_interval_ms
    );

    // START DISCOVER (DB -> search -> subscribe)
    println!(
        "{c_met}[{}]{c_rst} {c_id}dsc{c_rst}  Starting PV discovery orchestrator...",
        ts()
    );

    let mut discover = aura_discover::Orchestrator::new(config.clone());
    let discover_pool = pool.clone();
    let mut redis_discover = redis_client.get_multiplexed_async_connection().await?;

    let discover_handle = tokio::spawn(async move {
        if let Err(e) = discover.run(&discover_pool, &mut redis_discover).await {
            tracing::error!("discover: {e}");
        }
    });

    println!(
        "{c_met}[{}]{c_rst} {c_ok}dsc  Orchestrator running{c_rst} — polling pv_config every {c_val}{}s{c_rst}",
        ts(),
        config.discover.config_poll_interval_s
    );

    // START INGEST (listens for subscribe commands from discover)
    println!(
        "{c_met}[{}]{c_rst} {c_id}net{c_rst}  Initializing PVAccess ingest engine...",
        ts()
    );

    let mut pva_config = PvaClientConfig::from_env();

    {
        let ioc_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM ioc_config WHERE enabled = TRUE")
                .fetch_one(&pool)
                .await
                .unwrap_or((0,));
        if ioc_count.0 == 0 && !config.discover.name_servers.is_empty() {
            println!(
                "{c_met}[{}]{c_rst} {c_id}ioc{c_rst}  Seeding ioc_config from aura.toml ({} entries)...",
                ts(),
                config.discover.name_servers.len()
            );
            for s in &config.discover.name_servers {
                if s.parse::<std::net::SocketAddr>().is_ok() {
                    let _ = sqlx::query(
                        "INSERT INTO ioc_config (address) VALUES ($1) ON CONFLICT DO NOTHING",
                    )
                    .bind(s)
                    .execute(&pool)
                    .await;
                } else {
                    eprintln!(
                        "\x1b[31m[config]\x1b[0m invalid IOC '{}' — must be IP:PORT",
                        s
                    );
                }
            }
        }
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT address FROM ioc_config WHERE enabled = TRUE")
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
        let ns_addrs: Vec<std::net::SocketAddr> = rows
            .iter()
            .filter_map(|(a,)| a.parse::<std::net::SocketAddr>().ok())
            .collect();
        if ns_addrs.is_empty() {
            eprintln!(
                "\x1b[31m[config]\x1b[0m no IOCs — INSERT INTO ioc_config (address) VALUES ('IP:PORT')"
            );
        } else {
            pva_config.name_servers = ns_addrs.clone();
            let labels: Vec<&str> = rows.iter().map(|(a,)| a.as_str()).collect();
            println!(
                "{c_met}[{}]{c_rst} {c_id}ioc{c_rst}  {} IOCs: {c_val}{:?}{c_rst}",
                ts(),
                labels.len(),
                labels
            );
        }
    }

    let mut driver = PvaDriver::new(pva_config);

    // Create the aggregated monitor bus - 1 channel per ingest shard.
    // Events flow: session -> bus_tx -> shard_rx -> ingest thread.
    // This replaces 16k+ per-PV channels with N shard channels.
    let (bus_tx, mut bus_rxs) = aura_net::create_bus(ingest_threads, 50_000);
    driver.set_bus_tx(bus_tx);

    // Shared atomic counters for multi-threaded ingest stats.
    let ingest_metrics = Arc::new(aura_ingest::metrics::IngestMetrics::new());

    // Single engine for startup (metadata collection). Will be replaced
    // by per-shard engines after monitors are distributed.
    let mut engine = IngestEngine::new();

    // Subscribe to the command channel from discover.
    let mut redis_sub = redis_client.get_async_pubsub().await?;
    redis_sub.subscribe(aura_discover::COMMAND_CHANNEL).await?;

    let mut notify_rx = aura_discover::pg_notify::spawn_listener(config.database.url.clone());

    let mut monitors: Vec<aura_net::MonitorHandle> = Vec::new();

    println!(
        "{c_met}[{}]{c_rst} {c_ok}net  Ingest ready{c_rst} — listening for commands on {c_val}{}{c_rst}",
        ts(),
        aura_discover::COMMAND_CHANNEL
    );

    let mut ingest_handles: Vec<std::thread::JoinHandle<()>> = Vec::new();

    let mut stats_timer = tokio::time::interval(Duration::from_secs(10));

    let mut cmd_stream = redis_sub.on_message();

    println!(
        "\n{c_ok}● CORE_ACTIVE{c_rst} :: Engine is polling data. Terminate with {c_warn}Ctrl+C{c_rst}\n"
    );

    // Load pv_name → pv_id mapping from pv_lookup into ArcSwap.
    let pv_id_map: std::collections::HashMap<Arc<str>, i32> = {
        let rows: Vec<(String, i32)> = sqlx::query_as("SELECT pv_name, pv_id FROM pv_lookup")
            .fetch_all(&pool)
            .await
            .unwrap_or_default();
        let n = rows.len();
        let map: std::collections::HashMap<Arc<str>, i32> = rows
            .into_iter()
            .map(|(name, id)| (Arc::from(name.as_str()), id))
            .collect();
        if n > 0 {
            println!(
                "{c_met}[{}]{c_rst} {c_ok}db   pv_id snapshot: {n} entries loaded from pv_lookup{c_rst}",
                ts()
            );
        }
        map
    };
    let shared_pv_cache: Arc<arc_swap::ArcSwap<std::collections::HashMap<Arc<str>, i32>>> =
        Arc::new(arc_swap::ArcSwap::from_pointee(pv_id_map));
    println!(
        "{c_met}[{}]{c_rst} {c_ok}db   pv_cache: {} entries (ArcSwap lock-free){c_rst}",
        ts(),
        shared_pv_cache.load().len()
    );

    let mut metadata_stored_pvs: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    // No Redis pub/sub dependency at startup - DB is the source of truth.
    {
        let pv_rows: Vec<(String,)> =
            sqlx::query_as("SELECT pv_name FROM pv_config WHERE enabled = TRUE")
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
        if !pv_rows.is_empty() {
            let subscribe_pvs: Vec<String> = pv_rows.into_iter().map(|(pv,)| pv).collect();
            let count = subscribe_pvs.len();
            println!(
                "{c_met}[{}]{c_rst} {c_id}net{c_rst}  subscribing to {count} PVs...",
                ts()
            );

            let sub = handlers::subscribe_pvs(
                &pool,
                &mut driver,
                &mut engine,
                &shared_pv_cache,
                &subscribe_pvs,
                "startup subscribe",
            )
            .await;
            monitors.extend(sub.handles);
            let ok_pvs = sub.ok_pvs;
            let fail = sub.failed;

            if !ok_pvs.is_empty() {
                let expected = ok_pvs.len();
                let n = handlers::collect_initial_metadata(
                    &pool,
                    &driver,
                    &mut engine,
                    &mut metadata_stored_pvs,
                    expected,
                    5,
                )
                .await;
                println!(
                    "{c_met}[{}]{c_rst} {c_ok}db   ✓ {n}/{expected} metadata stored{c_rst}",
                    ts()
                );
            }

            if fail > 0 {
                println!(
                    "{c_met}[{}]{c_rst} {c_id}net{c_rst}  ✓ {}/{count} PVs ready to archive ({fail} failed)",
                    ts(),
                    ok_pvs.len()
                );
            } else {
                println!(
                    "{c_met}[{}]{c_rst} {c_id}net{c_rst}  ✓ {count}/{count} PVs ready to archive",
                    ts()
                );
            }
        }
    }

    // Load per-PV heartbeat overrides from pv_config.
    {
        let hb_rows: Vec<(i32, f64)> = sqlx::query_as(
            "SELECT l.pv_id, c.heartbeat_s FROM pv_config c JOIN pv_lookup l ON l.pv_name = c.pv_name WHERE c.heartbeat_s IS NOT NULL AND c.enabled = TRUE"
        ).fetch_all(&pool).await.unwrap_or_default();
        if !hb_rows.is_empty() {
            let overrides: Vec<(i32, f32)> =
                hb_rows.iter().map(|&(id, hs)| (id, hs as f32)).collect();
            heartbeat_config.store(Arc::new(overrides));
        }
    }

    // Auto-tune TimescaleDB chunk interval
    // Target: active chunk index fits in shared_buffers/2 (~RAM/8).
    // Re-tuned every 5 minutes from actual observed throughput.
    let chunk_max_rows: usize = total_ram_mb * 1024 * 1024 / 8 / 100;
    let mut last_chunk_tune = Instant::now();
    let mut current_chunk_secs: u64;
    let mut chunk_tune_last_events: u64 = 0;
    {
        let n_pvs = shared_pv_cache.load().len().max(1);
        let est_rate = n_pvs * 10;
        let secs = (chunk_max_rows / est_rate).max(60).min(86400) as u64;
        let interval = format_chunk_interval(secs);
        if let Err(e) = sqlx::query("SELECT set_chunk_time_interval('samples', $1::interval)")
            .bind(&interval)
            .execute(&pool)
            .await
        {
            tracing::warn!("set_chunk_time_interval failed: {e}");
        }
        current_chunk_secs = secs;
        println!(
            "{c_met}[{}]{c_rst} {c_ok}db   chunk interval: {interval} (initial, {n_pvs} PVs × 10 Hz){c_rst}",
            ts()
        );
    }

    let pv_stats_sink: Arc<std::sync::Mutex<Vec<std::collections::HashMap<i32, (u64, f64, i16)>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let inline_meta_store: Arc<std::sync::Mutex<Vec<aura_core::metadata::PvMetadata>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    if !monitors.is_empty() {
        let ctx = aura_ingest::thread::IngestContext {
            ingest_threads,
            config: config.clone(),
            shared_bufs: shared_writer_bufs.clone(),
            shared_pv_cache: shared_pv_cache.clone(),
            inline_meta_store: inline_meta_store.clone(),
            pv_stats_sink: pv_stats_sink.clone(),
            metrics: ingest_metrics.clone(),
            shutdown: ingest_shutdown.clone(),
            heartbeat_config: heartbeat_config.clone(),
        };
        let mut bus_rxs_vec = std::mem::take(&mut bus_rxs);
        let (handles, _tokens) = aura_ingest::thread::setup_shards_and_spawn(
            &mut monitors,
            ingest_threads,
            &mut bus_rxs_vec,
            &ctx,
        );
        ingest_handles = handles;
        println!(
            "{c_met}[{}]{c_rst} {c_ok}thr  {ingest_threads} ingest threads active{c_rst}",
            ts()
        );
    }

    driver.set_pv_cache(shared_pv_cache.clone());
    let mut reconnect_rx = driver.take_reconnect_rx();
    let mut lifecycle_rx = driver.take_lifecycle_rx();

    {
        let pv_cache = shared_pv_cache.load();
        if !pv_cache.is_empty() {
            let names: Vec<&str> = pv_cache.keys().map(|k| k.as_ref()).collect();
            let ids: Vec<i32> = pv_cache.values().copied().collect();
            let _ = sqlx::query(
                "INSERT INTO pv_status (pv_name, pv_id, state, subscribed_at, last_event_at) \
                 SELECT t.pv_name, t.pv_id, 3, NOW(), NOW() \
                 FROM UNNEST($1::text[], $2::int[]) AS t(pv_name, pv_id) \
                 ON CONFLICT (pv_name) DO UPDATE SET state = 3, pv_id = EXCLUDED.pv_id, subscribed_at = NOW(), last_event_at = NOW()"
            ).bind(&names).bind(&ids).execute(&pool).await;
            println!(
                "{c_met}[{}]{c_rst} {c_ok}db   pv_status: {} entries initialized{c_rst}",
                ts(),
                names.len()
            );
            // Log SUBSCRIBE events for all startup PVs.
            let _ = sqlx::query(
                "INSERT INTO pv_events (pv_name, pv_id, event_type, detail)                  SELECT t.pv_name, t.pv_id, 0, 'startup subscribe'                  FROM UNNEST($1::text[], $2::int[]) AS t(pv_name, pv_id)"
            ).bind(&names).bind(&ids).execute(&pool).await;
        }
    }

    loop {
        tokio::select! {
            Some((addr, new_cmd_tx)) = reconnect_rx.recv() => {
                driver.session_commands_mut().insert(addr, new_cmd_tx);
                println!("{c_met}[{}]{c_rst} {c_ok}net  IOC {} reconnected — commands routed{c_rst}", ts(), addr);
            }

            Some(event) = lifecycle_rx.recv() => {
                use aura_net::runtime::driver::LifecycleEvent;
                match event {
                    LifecycleEvent::Disconnected { addr, pvs, reason } => {
                        ingest_metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                        let n = pvs.len();
                        println!("{c_met}[{}]{c_rst} {c_warn}net  IOC {addr} disconnected ({n} PVs) — {reason}{c_rst}", ts());
                        handlers::handle_disconnect(&pool, &shared_pv_cache, &shared_writer_buf, addr, &pvs, &reason).await;
                    }
                    LifecycleEvent::Reconnected { addr, pvs } => {
                        ingest_metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                        let n = pvs.len();
                        println!("{c_met}[{}]{c_rst} {c_ok}net  IOC {addr} reconnected ({n} PVs){c_rst}", ts());
                        handlers::handle_reconnect(&pool, &shared_pv_cache, &shared_writer_buf, addr, &pvs).await;
                    }
                }
            }

            _ = stats_timer.tick() => {
                handlers::drain_metadata(&pool, &driver, &inline_meta_store, &mut metadata_stored_pvs).await;
                handlers::update_pv_status(&pool, &pv_stats_sink, &shared_pv_cache, &shared_writer_buf).await;

                // Reload per-PV heartbeat config every 30s (every 3rd stats tick).
                {
                    static TICK_COUNT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
                    if TICK_COUNT.fetch_add(1, Ordering::Relaxed) % 3 == 0 {
                        if let Ok(hb_rows) = sqlx::query_as::<_, (i32, f64)>(
                            "SELECT l.pv_id, c.heartbeat_s FROM pv_config c \
                             JOIN pv_lookup l ON l.pv_name = c.pv_name \
                             WHERE c.heartbeat_s IS NOT NULL AND c.enabled = TRUE"
                        ).fetch_all(&pool).await {
                            let overrides: Vec<(i32, f32)> = hb_rows.iter().map(|&(id, hs)| (id, hs as f32)).collect();
                            heartbeat_config.store(Arc::new(overrides));
                        }
                    }
                }

                let snap = ingest_metrics.snapshot();
                let ev = snap.events_received;
                if ev > 0 {
                    let buf_drops: u64 = shared_writer_bufs.iter().map(|b| b.total_dropped()).sum();
                    let spills = spill_counter.load(Ordering::Relaxed);
                    println!("{c_met}[{}]{c_rst} {c_id}sta{c_rst}  events={c_val}{}{c_rst} pub={c_val}{}{c_rst} skip={c_val}{}{c_rst} dc={c_val}{}{c_rst} spill={c_val}{}{c_rst} buf_drop={c_val}{}{c_rst} sessions={c_val}{}{c_rst} fast={c_val}{}%{c_rst}",
                        ts(), snap.events_received, snap.events_published,
                        snap.events_skipped, snap.disconnects,
                        spills, buf_drops, driver.session_count(),
                        snap.fast_path_pct);
                }

                // Re-tune chunk interval every 5 min from actual throughput
                if last_chunk_tune.elapsed() > Duration::from_secs(300) {
                    let elapsed = last_chunk_tune.elapsed().as_secs().max(1);
                    let delta = ev.saturating_sub(chunk_tune_last_events);
                    let rate = (delta / elapsed) as usize;
                    if rate > 0 {
                        let new_secs = (chunk_max_rows / rate).max(60).min(86400) as u64;
                        let ratio = if new_secs > current_chunk_secs {
                            new_secs as f64 / current_chunk_secs.max(1) as f64
                        } else {
                            current_chunk_secs as f64 / new_secs.max(1) as f64
                        };
                        if ratio > 1.3 {
                            let interval = format_chunk_interval(new_secs);
                            if let Err(e) = sqlx::query("SELECT set_chunk_time_interval('samples', $1::interval)")
                                .bind(&interval).execute(&pool).await {
                                tracing::warn!("chunk re-tune failed: {e}");
                            } else {
                                current_chunk_secs = new_secs;
                                println!("{c_met}[{}]{c_rst} {c_ok}db   chunk re-tuned: {interval} (measured {}k events/s){c_rst}",
                                    ts(), rate / 1000);
                            }
                        }
                    }
                    chunk_tune_last_events = ev;
                    last_chunk_tune = Instant::now();
                }
            }

            Some(msg) = cmd_stream.next() => {
                let payload: String = match msg.get_payload() {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                // Collect this command and drain any additional pending commands.
                let mut subscribe_pvs: Vec<String> = Vec::new();
                let mut other_cmds: Vec<aura_discover::IngestCommand> = Vec::new();

                if let Some(cmd) = aura_discover::IngestCommand::from_json(&payload) {
                    match cmd {
                        aura_discover::IngestCommand::Subscribe { pv, .. } => subscribe_pvs.push(pv),
                        aura_discover::IngestCommand::SubscribeBatch { pvs, .. } => {
                            // Filter out PVs already subscribed (prevents double-subscribe at startup).
                            let new_pvs: Vec<String> = pvs.into_iter()
                                .filter(|pv| !driver.pv_server_cache().contains_key(pv))
                                .collect();
                            if !new_pvs.is_empty() {
                                subscribe_pvs.extend(new_pvs);
                            }
                        }
                        other => other_cmds.push(other),
                    }
                }

                // Drain all pending messages from the stream (non-blocking).
                loop {
                    match tokio::time::timeout(
                        Duration::from_millis(1),
                        cmd_stream.next()
                    ).await {
                        Ok(Some(msg)) => {
                            let p: String = match msg.get_payload() {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if let Some(cmd) = aura_discover::IngestCommand::from_json(&p) {
                                match cmd {
                                    aura_discover::IngestCommand::Subscribe { pv, .. } => subscribe_pvs.push(pv),
                                    aura_discover::IngestCommand::SubscribeBatch { pvs, .. } => subscribe_pvs.extend(pvs),
                                    other => other_cmds.push(other),
                                }
                            }
                        }
                        _ => break,
                    }
                }

                // Process batch subscribes.
                if !subscribe_pvs.is_empty() {
                    let count = subscribe_pvs.len();
                    println!("{c_met}[{}]{c_rst} {c_id}net{c_rst}  subscribing to {count} PVs...", ts());

                    let sub = handlers::subscribe_pvs(
                        &pool, &mut driver, &mut engine, &shared_pv_cache,
                        &subscribe_pvs, "subscribed (orchestrator)",
                    ).await;
                    monitors.extend(sub.handles);
                    let ok_pvs = sub.ok_pvs;
                    let fail = sub.failed;

                    if !ok_pvs.is_empty() {
                        let expected = ok_pvs.len();
                        let timeout_s = 3 + (expected as u64 / 10000).max(1);
                        let n = handlers::collect_initial_metadata(
                            &pool, &driver, &mut engine, &mut metadata_stored_pvs, expected, timeout_s,
                        ).await;
                        println!("{c_met}[{}]{c_rst} {c_ok}db   ✓ {n}/{expected} metadata stored{c_rst}", ts());
                        engine.total_published = 0;
                        engine.total_events = 0;
                        engine.total_skipped = 0;
                    }

                    let ok = ok_pvs.len();
                    println!("{c_met}[{}]{c_rst} {c_ok}net  ✓ {ok}/{count} PVs ready to archive{c_rst}{}", ts(),
                        if fail > 0 { format!(" ({fail} failed)") } else { String::new() });



                    if !monitors.is_empty() && ingest_handles.is_empty() {
                        let ctx = aura_ingest::thread::IngestContext {
                            ingest_threads, config: config.clone(),
                            shared_bufs: shared_writer_bufs.clone(),
                            shared_pv_cache: shared_pv_cache.clone(),
                            inline_meta_store: inline_meta_store.clone(),
                            pv_stats_sink: pv_stats_sink.clone(),
                            metrics: ingest_metrics.clone(),
                            shutdown: ingest_shutdown.clone(),
                            heartbeat_config: heartbeat_config.clone(),
                        };
                        let mut bus_rxs_vec = std::mem::take(&mut bus_rxs);
                        let (handles, _tokens) = aura_ingest::thread::setup_shards_and_spawn(
                            &mut monitors, ingest_threads, &mut bus_rxs_vec, &ctx,
                        );
                        ingest_handles = handles;
                        println!("{c_met}[{}]{c_rst} {c_ok}thr  {ingest_threads} ingest threads active{c_rst}", ts());
                    }
                }

                    for cmd in other_cmds {
                    match cmd {
                        aura_discover::IngestCommand::Unsubscribe { pv } => {
                            driver.unsubscribe(&[pv.clone()]).await;
                            if let Some(idx) = monitors.iter().position(|h| h.pv_name() == pv) {
                                monitors[idx].cancel();
                                monitors.swap_remove(idx);
                            }
                            engine.unregister_pv(&pv);
                            {
                                let mut new_map = (**shared_pv_cache.load()).clone();
                                new_map.remove(pv.as_str());
                                shared_pv_cache.store(Arc::new(new_map));
                            }
                            let _ = sqlx::query("UPDATE pv_status SET state = 6 WHERE pv_name = $1").bind(&pv).execute(&pool).await;
                            let _ = sqlx::query("INSERT INTO pv_events (pv_name, event_type, detail) VALUES ($1, 6, 'unsubscribed')")
                                .bind(&pv).execute(&pool).await;
                            println!("{c_met}[{}]{c_rst} {c_id}net{c_rst}  ✕ {pv} unsubscribed", ts());
                        }
                        aura_discover::IngestCommand::UnsubscribeBatch { pvs } => {
                            let count = pvs.len();
                            driver.unsubscribe(&pvs).await;
                            for pv in &pvs {
                                if let Some(idx) = monitors.iter().position(|h| h.pv_name() == *pv) {
                                    monitors[idx].cancel();
                                    monitors.swap_remove(idx);
                                }
                                engine.unregister_pv(pv);
                            }
                            {
                                let mut new_map = (**shared_pv_cache.load()).clone();
                                for pv in &pvs { new_map.remove(pv.as_str()); }
                                shared_pv_cache.store(Arc::new(new_map));
                            }
                            let names: Vec<&str> = pvs.iter().map(|s| s.as_str()).collect();
                            let _ = sqlx::query("UPDATE pv_status SET state = 6 WHERE pv_name = ANY($1::text[])").bind(&names).execute(&pool).await;
                            let _ = sqlx::query(
                                "INSERT INTO pv_events (pv_name, event_type, detail) SELECT unnest($1::text[]), 6, 'batch unsubscribed'"
                            ).bind(&names).execute(&pool).await;
                            println!("{c_met}[{}]{c_rst} {c_id}net{c_rst}  ✕ {count} PVs unsubscribed (batch)", ts());
                        }
                        aura_discover::IngestCommand::Reload => {
                            println!("{c_met}[{}]{c_rst} {c_id}net{c_rst}  Reload requested", ts());
                        }
                        _ => {}
                    }
                }
            }

            Some(cmd_json) = notify_rx.recv() => {
                // IOC change is not an IngestCommand variant - handle raw JSON first.
                if cmd_json.contains("\"ioc_change\"") {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&cmd_json) {
                        if v.get("cmd").and_then(|c| c.as_str()) == Some("ioc_change") {
                            // IOC config changed — reload + hot-add new IOCs.
                            println!("{c_met}[{}]{c_rst} {c_id}ioc{c_rst}  IOC config changed (DB NOTIFY)", ts());
                            let rows: Vec<(String,)> = sqlx::query_as("SELECT address FROM ioc_config WHERE enabled = TRUE")
                                .fetch_all(&pool).await.unwrap_or_default();
                            let new_addrs: Vec<std::net::SocketAddr> = rows.iter()
                                .filter_map(|(a,)| a.parse::<std::net::SocketAddr>().ok())
                                .collect();
                            let old_addrs = driver.ioc_addresses();
                            let added: Vec<std::net::SocketAddr> = new_addrs.iter()
                                .filter(|a| !old_addrs.contains(a))
                                .copied()
                                .collect();
                            let removed: Vec<std::net::SocketAddr> = old_addrs.iter()
                                .filter(|a| !new_addrs.contains(a))
                                .copied()
                                .collect();
                            driver.set_name_servers(new_addrs);
                            for addr in &removed {
                                if let Some(pvs) = driver.ioc_pvs().get(addr) {
                                    let names: Vec<&str> = pvs.iter().map(|s| s.as_str()).collect();
                                    let addr_str = addr.to_string();
                                    let _ = sqlx::query(
                                        "INSERT INTO pv_events (pv_name, event_type, ioc_addr, detail) \
                                         SELECT unnest($1::text[]), 7, $2, 'IOC removed via NOTIFY'"
                                    ).bind(&names).bind(&addr_str).execute(&pool).await;
                                }
                                driver.remove_session(addr);
                                println!("{c_met}[{}]{c_rst} {c_warn}ioc  {} removed{c_rst}", ts(), addr);
                            }
                            for addr in added {
                                let all_configured: Vec<String> = sqlx::query_as::<_, (String,)>(
                                    "SELECT pv_name FROM pv_config WHERE enabled = TRUE"
                                ).fetch_all(&pool).await.unwrap_or_default()
                                    .into_iter().map(|(n,)| n).collect();
                                let recovered = driver.hot_add_ioc(addr, &all_configured).await;
                                if !recovered.is_empty() {
                                    let count = recovered.len();
                                    let names: Vec<&str> = recovered.iter().map(|s| s.as_str()).collect();
                                    let _ = sqlx::query("INSERT INTO pv_lookup (pv_name) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING")
                                        .bind(&names).execute(&pool).await;
                                    let rows: Vec<(String, i32)> = sqlx::query_as(
                                        "SELECT pv_name, pv_id FROM pv_lookup WHERE pv_name = ANY($1::text[])"
                                    ).bind(&names).fetch_all(&pool).await.unwrap_or_default();
                                    {
                                        let mut new_map = (**shared_pv_cache.load()).clone();
                                        for (name, id) in rows {
                                            new_map.insert(Arc::from(name.as_str()), id);
                                        }
                                        shared_pv_cache.store(Arc::new(new_map));
                                    }
                                    println!("{c_met}[{}]{c_rst} {c_ok}ioc  {} added — {count} PVs recovered{c_rst}", ts(), addr);
                                } else {
                                    println!("{c_met}[{}]{c_rst} {c_ok}ioc  {} added (0 PVs recovered){c_rst}", ts(), addr);
                                }
                            }
                            continue;
                        }
                    }
                }
                if let Some(cmd) = aura_discover::IngestCommand::from_json(&cmd_json) {
                    match cmd {
                        cmd @ aura_discover::IngestCommand::Subscribe { .. }
                        | cmd @ aura_discover::IngestCommand::SubscribeBatch { .. }
                        => {
                            // Collect PVs from this message.
                            let mut subscribe_pvs: Vec<String> = match cmd {
                                aura_discover::IngestCommand::Subscribe { pv, .. } => vec![pv],
                                aura_discover::IngestCommand::SubscribeBatch { pvs, .. } => pvs,
                                _ => unreachable!(),
                            };
                            // Drain pending NOTIFY messages (100ms window) to aggregate batches.
                            // PostgreSQL splits large inserts into 578-PV chunks — we recombine them.
                            loop {
                                match tokio::time::timeout(
                                    Duration::from_millis(100),
                                    notify_rx.recv()
                                ).await {
                                    Ok(Some(next_json)) => {
                                        if next_json.contains("\"ioc_change\"") {
                                            // IOC change - process after this batch.
                                            // Re-inject by handling inline (rare).
                                            break;
                                        }
                                        if let Some(next_cmd) = aura_discover::IngestCommand::from_json(&next_json) {
                                            match next_cmd {
                                                aura_discover::IngestCommand::Subscribe { pv, .. } => subscribe_pvs.push(pv),
                                                aura_discover::IngestCommand::SubscribeBatch { pvs, .. } => subscribe_pvs.extend(pvs),
                                                _ => break,
                                            }
                                        }
                                    }
                                    _ => break,
                                }
                            }
                            let new_pvs: Vec<String> = subscribe_pvs.into_iter()
                                .filter(|pv| !driver.pv_server_cache().contains_key(pv))
                                .collect();
                            if new_pvs.is_empty() { continue; }
                            let count = new_pvs.len();
                            if count == 1 {
                                println!("{c_met}[{}]{c_rst} {c_id}pv {c_rst}  +{c_val}{}{c_rst} (DB NOTIFY)", ts(), &new_pvs[0]);
                            } else {
                                println!("{c_met}[{}]{c_rst} {c_id}pv {c_rst}  +{c_val}{count} PVs{c_rst} (DB NOTIFY)", ts());
                            }
                            // Pre-register in pv_lookup + pv_cache BEFORE subscribing.
                            // This ensures pv_id is resolvable when the first events arrive.
                            let sub = handlers::subscribe_pvs(
                                &pool, &mut driver, &mut engine, &shared_pv_cache,
                                &new_pvs, "dynamic subscribe via NOTIFY",
                            ).await;
                            let ok_pvs = sub.ok_pvs;
                            if sub.failed > 0 {
                                let failed: Vec<&str> = new_pvs.iter()
                                    .filter(|pv| !ok_pvs.iter().any(|ok| ok == *pv))
                                    .map(|s| s.as_str()).take(5).collect();
                                println!("{c_met}[{}]{c_rst} {c_warn}pv   {} PVs not found on any IOC (e.g. {:?}){c_rst}", ts(), sub.failed, failed);
                            }
                            if !ok_pvs.is_empty() {
                                println!("{c_met}[{}]{c_rst} {c_ok}pv   {} PVs added to hot path{c_rst}", ts(), ok_pvs.len());
                            }
                        }
                        aura_discover::IngestCommand::Unsubscribe { pv } => {
                            println!("{c_met}[{}]{c_rst} {c_id}pv {c_rst}  -{c_val}{pv}{c_rst} (DB NOTIFY)", ts());
                            driver.unsubscribe(&[pv.clone()]).await;
                            {
                                let mut new_map = (**shared_pv_cache.load()).clone();
                                new_map.remove(pv.as_str());
                                shared_pv_cache.store(Arc::new(new_map));
                            }
                            let _ = sqlx::query("UPDATE pv_status SET state = 6 WHERE pv_name = $1").bind(&pv).execute(&pool).await;
                            let _ = sqlx::query("INSERT INTO pv_events (pv_name, event_type, detail) VALUES ($1, 6, 'unsubscribed')").bind(&pv).execute(&pool).await;
                        }
                        aura_discover::IngestCommand::UnsubscribeBatch { pvs } => {
                            let count = pvs.len();
                            println!("{c_met}[{}]{c_rst} {c_id}pv {c_rst}  -{c_val}{count} PVs{c_rst} (DB NOTIFY batch)", ts());
                            driver.unsubscribe(&pvs).await;
                                            {
                                let mut new_map = (**shared_pv_cache.load()).clone();
                                for pv in &pvs { new_map.remove(pv.as_str()); }
                                shared_pv_cache.store(Arc::new(new_map));
                            }
                            let names: Vec<&str> = pvs.iter().map(|s| s.as_str()).collect();
                            let _ = sqlx::query("UPDATE pv_status SET state = 6 WHERE pv_name = ANY($1::text[])").bind(&names).execute(&pool).await;
                            let _ = sqlx::query(
                                "INSERT INTO pv_events (pv_name, event_type, detail) SELECT unnest($1::text[]), 6, 'batch unsubscribed'"
                            ).bind(&names).execute(&pool).await;
                        }
                        _ => {}
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => { break; }
        }
    }

    println!("\n{c_met}[shutdown]{c_rst} Signal SIGINT received.");

    // Close PVA sessions cleanly (TCP FIN, not RST) so IOCs don't log errors.
    driver.shutdown_sessions().await;

    store_cancel_trigger.cancel();

    drop(shard_senders);
    ingest_shutdown.store(true, Ordering::Relaxed);

    let _ = tokio::time::timeout(Duration::from_secs(10), store_handle).await;

    for h in ingest_handles {
        let _ = h.join();
    }

    discover_handle.abort();

    let snap = ingest_metrics.snapshot();
    println!(
        "{c_met}[shutdown]{c_rst} Events: {c_val}{}{c_rst} received, {c_val}{}{c_rst} published, {c_val}{}{c_rst} skipped",
        snap.events_received, snap.events_published, snap.events_skipped
    );
    println!("{c_met}[shutdown]{c_rst} {c_ok}AURA-EPICS stopped safely.{c_rst}");

    Ok(())
}