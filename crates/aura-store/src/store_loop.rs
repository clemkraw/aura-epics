//! Store loop: drains SharedBuffer + fallback channel, flushes via COPY.
//! Runs as a single tokio task with background COPY (double-buffered).

use crate::pipeline::Pipeline;
use crate::writer::shared_buf::SharedBuffer;
use aura_core::sample::PvUpdate;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Run the store loop. Consumes rows from SharedBuffer and fallback mpsc channel,
/// flushes to TimescaleDB via parallel COPY connections.
pub async fn run(
    mut pipeline: Pipeline,
    shared_bufs: Vec<Arc<SharedBuffer>>,
    mut rx: tokio::sync::mpsc::Receiver<PvUpdate>,
    store_pool: sqlx::PgPool,
    redis_url: String,
    spill_counter: Arc<AtomicU64>,
    cancel: tokio_util::sync::CancellationToken,
    store_notify: Arc<tokio::sync::Notify>,
) {
    let mut total_written: u64 = 0;
    let mut last_written: u64 = 0;
    let mut total_received: u64 = 0;
    let mut last_received: u64 = 0;
    let mut last_time = std::time::Instant::now();
    let mut last_stats = std::time::Instant::now();
    let mut total_spill_drained: u64 = 0;

    let mut redis_drain: Option<redis::aio::MultiplexedConnection> = None;

    // Background flush - store loop continues while COPY runs.
    let mut bg_flush: Option<tokio::task::JoinHandle<Result<crate::writer::FlushReport, String>>> =
        None;

    loop {
        // Flush: background COPY with double-buffering
        if pipeline.writer().has_pending()
            && (pipeline.any_writer_full_pub() || pipeline.should_time_flush_pub())
        {
            if let Some(handle) = bg_flush.take() {
                match tokio::time::timeout(Duration::from_secs(60), handle).await {
                    Ok(Ok(Ok(r))) => {
                        total_written += r.total() as u64;
                    }
                    Ok(Ok(Err(e))) => tracing::error!("bg flush error: {e}"),
                    Ok(Err(e)) => tracing::error!("bg flush panic: {e}"),
                    Err(_) => {
                        tracing::error!("bg flush timeout 60s — PostgreSQL may be unresponsive")
                    }
                }
                pipeline.reset_last_flush();
            }
            let ft = std::time::Instant::now();
            if let Some(bundle) = pipeline.writer_mut().take_flush_bundle() {
                bg_flush = Some(tokio::spawn(async move { bundle.flush().await }));
                pipeline.add_flush_us(ft.elapsed().as_micros() as u64);
            }
        }

        // Ingest from per-thread SharedBuffers
        let pt = std::time::Instant::now();
        let mut n_shared = 0usize;
        let mut stored = 0usize;
        for buf in shared_bufs.iter() {
            let scalars = buf.take_scalars();
            if !scalars.is_empty() {
                let n = scalars.len();
                pipeline.writer_mut().ingest_scalars(scalars);
                n_shared += n;
                stored += n;
            }
            let others = buf.take_others();
            if !others.is_empty() {
                let n = others.len();
                pipeline.writer_mut().ingest_other_rows(others);
                n_shared += n;
                stored += n;
            }
        }

        // Drain fallback channel (dispatch_sync - no await)
        let mut n_fallback = 0usize;
        loop {
            match rx.try_recv() {
                Ok(mut update) => {
                    match pipeline
                        .writer_mut()
                        .dispatch_sync(&mut update, aura_core::sample::StoreReason::ValueChanged)
                    {
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            tracing::error!("dispatch: {e}");
                        }
                        None => {
                            let _ = pipeline
                                .writer_mut()
                                .dispatch(
                                    &mut update,
                                    aura_core::sample::StoreReason::ValueChanged,
                                    &store_pool,
                                )
                                .await;
                        }
                    }
                    n_fallback += 1;
                    if n_fallback >= 10_000 {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        // Drain Redis spill (safety valve for SharedBuffer backpressure)
        let has_spills = spill_counter.load(Ordering::Relaxed) > total_spill_drained;
        if has_spills {
            if redis_drain.is_none() {
                if let Ok(c) = redis::Client::open(redis_url.as_str()) {
                    if let Ok(conn) = c.get_multiplexed_async_connection().await {
                        redis_drain = Some(conn);
                    }
                }
            }
            if let Some(ref mut conn) = redis_drain {
                let batch: Result<redis::Value, _> = redis::cmd("XRANGE")
                    .arg("aura:spill")
                    .arg("-")
                    .arg("+")
                    .arg("COUNT")
                    .arg(100)
                    .query_async(conn)
                    .await;
                if let Ok(redis::Value::Array(entries)) = batch {
                    if !entries.is_empty() {
                        let mut ids: Vec<String> = Vec::new();
                        let mut drained = 0usize;
                        for entry in &entries {
                            if let redis::Value::Array(parts) = entry {
                                if parts.len() >= 2 {
                                    let id = match &parts[0] {
                                        redis::Value::BulkString(b) => {
                                            String::from_utf8_lossy(b).to_string()
                                        }
                                        _ => continue,
                                    };
                                    if let redis::Value::Array(fields) = &parts[1] {
                                        for chunk in fields.chunks(2) {
                                            if let [
                                                redis::Value::BulkString(k),
                                                redis::Value::BulkString(v),
                                            ] = chunk
                                            {
                                                if k == b"d" {
                                                    if let Ok(mut u) =
                                                        serde_json::from_slice::<PvUpdate>(v)
                                                    {
                                                        let _ = pipeline.writer_mut().dispatch(
                                                            &mut u, aura_core::sample::StoreReason::ValueChanged, &store_pool,
                                                        ).await;
                                                        drained += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    ids.push(id);
                                }
                            }
                        }
                        if !ids.is_empty() {
                            let _: Result<i64, _> = redis::cmd("XDEL")
                                .arg("aura:spill")
                                .arg(&ids)
                                .query_async(conn)
                                .await;
                            total_spill_drained += drained as u64;
                        }
                    }
                }
            }
        }

        // Metrics
        let batch_total = n_shared + n_fallback;
        if batch_total > 0 {
            pipeline.add_process_us(pt.elapsed().as_micros() as u64);
            total_received += batch_total as u64;
            pipeline.add_received(batch_total as u64);
            pipeline.add_stored((stored + n_fallback) as u64);
        }
        pipeline.inc_iterations();

        // Wait for data via Notify - no polling, no sleep delay
        if batch_total == 0 && !pipeline.writer().has_pending() {
            tokio::select! {
                _ = store_notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                _ = cancel.cancelled() => break,
            }
        }
        if cancel.is_cancelled() {
            break;
        }

        // Stats (suppress during init)
        if total_received > 0 && last_stats.elapsed() >= Duration::from_secs(5) {
            let elapsed = last_time.elapsed().as_secs_f64();
            let in_rate = (total_received - last_received) as f64 / elapsed;
            let db_rate = (total_written - last_written) as f64 / elapsed;
            let buf = pipeline.scalar_buffer_len();
            let shared_len: usize = shared_bufs.iter().map(|b| b.len()).sum();

            eprintln!(
                "\x1b[36m[store]\x1b[0m in->{:.0}/s db->{:.0}/s | \
                 buf={buf} shared={shared_len} cpu={}ms build={}ms send={}ms flush={}ms spill_recovered={}",
                in_rate,
                db_rate,
                pipeline.total_process_us() / 1000,
                pipeline.total_build_us() / 1000,
                pipeline.total_send_us() / 1000,
                pipeline.total_flush_us() / 1000,
                total_spill_drained,
            );

            last_received = total_received;
            last_written = total_written;
            last_time = std::time::Instant::now();
            last_stats = std::time::Instant::now();
        }
    }

    // Wait for background flush before shutdown.
    if let Some(handle) = bg_flush {
        let _ = handle.await;
    }
    // Final drain + flush.
    for buf in &shared_bufs {
        let s = buf.take_scalars();
        if !s.is_empty() {
            pipeline.writer_mut().ingest_scalars(s);
        }
        let o = buf.take_others();
        if !o.is_empty() {
            pipeline.writer_mut().ingest_other_rows(o);
        }
    }
    if pipeline.writer().has_pending() {
        let _ = pipeline.writer_mut().flush_all().await;
    }
}