//! Store loop draining `SharedBuffer` and fallback channels to TimescaleDB.
//! Executes as a single Tokio task using non-blocking background COPY tasks.

use crate::pipeline::Pipeline;
use crate::writer::shared_buf::SharedBuffer;
use aura_core::sample::PvUpdate;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Executes the primary database ingestion loop.
///
/// Consumes metrics from `SharedBuffer` instances and an MPSC fallback channel,
/// flushing them to TimescaleDB over parallel COPY connections.
///
/// ### Backpressure & Overload Management
/// `SharedBuffer` instances represent bounded staging memory (`max_len` per shard).
/// When full, or while ingest is paused during database retry backoffs, excess incoming
/// items are dropped and counted at the producer level (`total_dropped`).
pub async fn run(
    mut pipeline: Pipeline,
    shared_bufs: Vec<Arc<SharedBuffer>>,
    mut rx: tokio::sync::mpsc::Receiver<PvUpdate>,
    store_pool: sqlx::PgPool,
    cancel: tokio_util::sync::CancellationToken,
    store_notify: Arc<tokio::sync::Notify>,
) {
    let mut total_written: u64 = 0;
    let mut last_written: u64 = 0;
    let mut total_received: u64 = 0;
    let mut last_received: u64 = 0;
    let mut last_time = std::time::Instant::now();
    let mut last_stats = std::time::Instant::now();

    // Configuration & State for COPY flush resilience
    const FLUSH_BACKOFF_SECS: [u64; 5] = [1, 4, 15, 30, 60];
    const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

    /// Holds a single bundle retained for retry following a transient failure.
    struct PendingRetry {
        bundle: Box<crate::writer::FlushBundle>,
        attempts: u32,
        next_try: std::time::Instant,
    }

    let mut bg_flush: Option<tokio::task::JoinHandle<crate::writer::FlushOutcome>> = None;
    let mut inflight_attempts: u32 = 0;
    let mut inflight_rows: usize = 0;

    let mut inflight_progress: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let mut pending_retry: Option<PendingRetry> = None;
    let mut flush_errors: u64 = 0;

    let rows_lost = pipeline.rows_lost_counter();

    loop {
        let mut did_flush_work = false;

        // 1. Non-blockingly reap the in-flight background flush task.
        if bg_flush.as_ref().is_some_and(|h| h.is_finished()) {
            let handle = bg_flush.take().expect("checked is_some above");
            let attempts = inflight_attempts;
            let rows_at_risk = inflight_rows;
            inflight_attempts = 0;
            inflight_rows = 0;
            did_flush_work = true;

            match handle.await {
                Ok(outcome) => {
                    total_written += outcome.report.total() as u64;

                    // Non-retryable errors (e.g., schema or constraint violations)
                    if outcome.lost_rows > 0 {
                        flush_errors += 1;
                        rows_lost.fetch_add(outcome.lost_rows as u64, Ordering::Relaxed);
                        tracing::error!(
                            rows = outcome.lost_rows,
                            errors = ?outcome.errors,
                            "COPY failed permanently — data dropped (non-retryable)"
                        );
                    }

                    // Handle transient failure retries
                    if let Some(bundle) = outcome.retry {
                        flush_errors += 1;
                        let failed = attempts + 1;
                        let rows = bundle.total_rows();

                        if (failed as usize) < FLUSH_BACKOFF_SECS.len() + 1 {
                            let delay = FLUSH_BACKOFF_SECS
                                [(failed as usize - 1).min(FLUSH_BACKOFF_SECS.len() - 1)];
                            tracing::warn!(
                                rows,
                                attempt = failed,
                                max_attempts = FLUSH_BACKOFF_SECS.len() + 1,
                                retry_in_s = delay,
                                errors = ?outcome.errors,
                                "COPY failed transiently — retaining bundle for retry"
                            );
                            pending_retry = Some(PendingRetry {
                                bundle,
                                attempts: failed,
                                next_try: std::time::Instant::now() + Duration::from_secs(delay),
                            });
                        } else {
                            rows_lost.fetch_add(rows as u64, Ordering::Relaxed);
                            tracing::error!(
                                rows,
                                attempts = failed,
                                errors = ?outcome.errors,
                                "COPY retries exhausted — data lost"
                            );
                        }
                    } else if attempts > 0 && outcome.lost_rows == 0 {
                        tracing::info!(
                            rows = outcome.report.total(),
                            attempts = attempts + 1,
                            "COPY retry succeeded — no data lost"
                        );
                    }
                }
                Err(join_err) => {
                    flush_errors += 1;
                    let committed = inflight_progress.load(Ordering::Relaxed) as usize;
                    let lost = rows_at_risk.saturating_sub(committed);
                    total_written += committed as u64;
                    rows_lost.fetch_add(lost as u64, Ordering::Relaxed);
                    tracing::error!(
                        rows = lost,
                        committed,
                        "Flush task panicked: {join_err} — uncommitted rows lost"
                    );
                }
            }
        }

        // 2. Schedule next flush: prioritized pending retries over fresh data.
        if bg_flush.is_none() {
            if let Some(pr) = pending_retry.take() {
                if std::time::Instant::now() >= pr.next_try {
                    // Attempt to re-establish dropped pool connections prior to retry
                    let (healed, still_dead) = pipeline.heal_copy_connections().await;
                    if healed > 0 {
                        tracing::info!(
                            healed,
                            still_dead,
                            "COPY connections re-established before retry"
                        );
                    }
                    inflight_attempts = pr.attempts;
                    inflight_rows = pr.bundle.total_rows();
                    inflight_progress = Arc::new(AtomicU64::new(0));
                    bg_flush = Some(tokio::spawn(
                        pr.bundle
                            .flush_with_progress(Arc::clone(&inflight_progress)),
                    ));
                    did_flush_work = true;
                } else {
                    pending_retry = Some(pr); // Delay not elapsed
                }
            }
        }

        // Trigger a new flush if threshold conditions or timers are met.
        if bg_flush.is_none()
            && pending_retry.is_none()
            && pipeline.writer().has_pending()
            && (pipeline.any_writer_full_pub() || pipeline.should_time_flush_pub())
        {
            let ft = std::time::Instant::now();
            if let Some(bundle) = pipeline.writer_mut().take_flush_bundle() {
                pipeline.reset_last_flush();
                inflight_attempts = 0;
                inflight_rows = bundle.total_rows();
                inflight_progress = Arc::new(AtomicU64::new(0));
                bg_flush = Some(tokio::spawn(
                    bundle.flush_with_progress(Arc::clone(&inflight_progress)),
                ));
                pipeline.add_flush_us(ft.elapsed().as_micros() as u64);
                did_flush_work = true;
            }
        }

        // 3. Ingest rows from SharedBuffers and fallback channel (paused during retries).
        let pt = std::time::Instant::now();
        let mut n_shared = 0usize;
        let mut stored = 0usize;

        if pending_retry.is_none() {
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
        }

        // Drain fallback channel
        let mut n_fallback = 0usize;
        if pending_retry.is_none() {
            loop {
                match rx.try_recv() {
                    Ok(mut update) => {
                        match pipeline.writer_mut().dispatch_sync(
                            &mut update,
                            aura_core::sample::StoreReason::ValueChanged,
                        ) {
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                tracing::error!("dispatch error: {e}");
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
        }

        // Metrics update
        let batch_total = n_shared + n_fallback;
        if batch_total > 0 {
            pipeline.add_process_us(pt.elapsed().as_micros() as u64);
            total_received += batch_total as u64;
            pipeline.add_received(batch_total as u64);
            pipeline.add_stored((stored + n_fallback) as u64);
        }
        pipeline.inc_iterations();

        // 4. Sleep until the next action (event notification, retry deadline, or safety tick).
        if batch_total == 0 && !did_flush_work {
            let mut nap = Duration::from_millis(100);
            if bg_flush.is_none() {
                if let Some(ref pr) = pending_retry {
                    nap = nap.min(
                        pr.next_try
                            .saturating_duration_since(std::time::Instant::now()),
                    );
                } else if pipeline.writer().has_pending() {
                    nap = nap.min(pipeline.time_to_next_flush());
                }
            }
            if !nap.is_zero() {
                tokio::select! {
                    _ = store_notify.notified() => {}
                    _ = tokio::time::sleep(nap) => {}
                    _ = cancel.cancelled() => break,
                }
            }
        }

        if cancel.is_cancelled() {
            break;
        }

        // Periodic metrics logging (every 5 seconds)
        if total_received > 0 && last_stats.elapsed() >= Duration::from_secs(5) {
            let elapsed = last_time.elapsed().as_secs_f64();
            let in_rate = (total_received - last_received) as f64 / elapsed;
            let db_rate = (total_written - last_written) as f64 / elapsed;
            let buf = pipeline.scalar_buffer_len();
            let shared_len: usize = shared_bufs.iter().map(|b| b.len()).sum();

            eprintln!(
                "\x1b[36m[store]\x1b[0m in->{:.0}/s db->{:.0}/s | \
                 buf={buf} shared={shared_len} cpu={}ms build={}ms send={}ms flush={}ms flush_err={} rows_lost={}{}",
                in_rate,
                db_rate,
                pipeline.total_process_us() / 1000,
                pipeline.total_build_us() / 1000,
                pipeline.total_send_us() / 1000,
                pipeline.total_flush_us() / 1000,
                flush_errors,
                rows_lost.load(Ordering::Relaxed),
                if pending_retry.is_some() {
                    " [DEGRADED: retry pending, ingest paused]"
                } else {
                    ""
                },
            );

            last_received = total_received;
            last_written = total_written;
            last_time = std::time::Instant::now();
            last_stats = std::time::Instant::now();
        }
    }

    // =========================================================================
    // Graceful Shutdown Sequence
    // =========================================================================

    // 1. Wait for active in-flight background flush (abort if timeout reached).
    if let Some(mut handle) = bg_flush.take() {
        let joined = match tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT, &mut handle).await {
            Ok(joined) => joined,
            Err(_) => {
                tracing::warn!(
                    timeout_s = SHUTDOWN_FLUSH_TIMEOUT.as_secs(),
                    "In-flight flush timed out during shutdown — aborting task"
                );
                handle.abort();
                handle.await
            }
        };

        match joined {
            Ok(outcome) => {
                total_written += outcome.report.total() as u64;
                if outcome.lost_rows > 0 {
                    rows_lost.fetch_add(outcome.lost_rows as u64, Ordering::Relaxed);
                }
                if let Some(bundle) = outcome.retry {
                    pending_retry = Some(PendingRetry {
                        bundle,
                        attempts: inflight_attempts + 1,
                        next_try: std::time::Instant::now(),
                    });
                }
            }
            Err(join_err) => {
                let committed = inflight_progress.load(Ordering::Relaxed) as usize;
                let lost = inflight_rows.saturating_sub(committed);
                total_written += committed as u64;
                rows_lost.fetch_add(lost as u64, Ordering::Relaxed);

                if join_err.is_cancelled() {
                    tracing::error!(
                        rows = lost,
                        committed,
                        timeout_s = SHUTDOWN_FLUSH_TIMEOUT.as_secs(),
                        "In-flight flush aborted during shutdown — uncommitted rows lost"
                    );
                } else {
                    tracing::error!(
                        rows = lost,
                        committed,
                        "Background flush task panicked during shutdown: {join_err}"
                    );
                }
            }
        }
    }

    // 2. Perform final flush attempt on retained retry bundle.
    if let Some(pr) = pending_retry.take() {
        let rows = pr.bundle.total_rows();
        let (healed, _) = pipeline.heal_copy_connections().await;
        if healed > 0 {
            tracing::info!(healed, "COPY connections re-established for final retry");
        }

        let progress = Arc::new(AtomicU64::new(0));
        let mut handle = tokio::spawn(pr.bundle.flush_with_progress(Arc::clone(&progress)));
        let joined = match tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT, &mut handle).await {
            Ok(joined) => joined,
            Err(_) => {
                handle.abort();
                handle.await
            }
        };

        match joined {
            Ok(outcome) => {
                total_written += outcome.report.total() as u64;
                let lost = outcome.lost_rows + outcome.retry.as_ref().map_or(0, |b| b.total_rows());
                if lost > 0 {
                    rows_lost.fetch_add(lost as u64, Ordering::Relaxed);
                    tracing::error!(
                        rows = lost,
                        errors = ?outcome.errors,
                        "Final retry failed at shutdown — rows lost"
                    );
                }
            }
            Err(join_err) => {
                let committed = progress.load(Ordering::Relaxed) as usize;
                let lost = rows.saturating_sub(committed);
                total_written += committed as u64;
                rows_lost.fetch_add(lost as u64, Ordering::Relaxed);
                tracing::error!(
                    rows = lost,
                    committed,
                    timeout_s = SHUTDOWN_FLUSH_TIMEOUT.as_secs(),
                    cancelled = join_err.is_cancelled(),
                    "Final retry timed out during shutdown — uncommitted rows lost"
                );
            }
        }
    }

    // 3. Final drain of remaining SharedBuffers and remaining writer queues.
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
        let (healed, _) = pipeline.heal_copy_connections().await;
        if healed > 0 {
            tracing::info!(healed, "COPY connections re-established for final flush");
        }

        if let Some(bundle) = pipeline.writer_mut().take_flush_bundle() {
            let rows = bundle.total_rows();
            let progress = Arc::new(AtomicU64::new(0));
            let mut handle = tokio::spawn(bundle.flush_with_progress(Arc::clone(&progress)));
            let joined = match tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT, &mut handle).await {
                Ok(joined) => joined,
                Err(_) => {
                    handle.abort();
                    handle.await
                }
            };

            match joined {
                Ok(outcome) => {
                    total_written += outcome.report.total() as u64;
                    let lost = outcome.lost_rows + outcome.retry.as_ref().map_or(0, |b| b.total_rows());
                    if lost > 0 {
                        rows_lost.fetch_add(lost as u64, Ordering::Relaxed);
                        tracing::error!(
                            rows = lost,
                            committed = outcome.report.total(),
                            errors = ?outcome.errors,
                            "Final flush incomplete — uncommitted rows lost"
                        );
                    }
                }
                Err(join_err) => {
                    let committed = progress.load(Ordering::Relaxed) as usize;
                    let lost = rows.saturating_sub(committed);
                    total_written += committed as u64;
                    rows_lost.fetch_add(lost as u64, Ordering::Relaxed);
                    tracing::error!(
                        rows = lost,
                        committed,
                        timeout_s = SHUTDOWN_FLUSH_TIMEOUT.as_secs(),
                        cancelled = join_err.is_cancelled(),
                        "Final flush timed out — uncommitted rows lost"
                    );
                }
            }
        }
    }

    // Summary logging
    if rows_lost.load(Ordering::Relaxed) > 0 {
        tracing::error!(
            rows_lost = rows_lost.load(Ordering::Relaxed),
            flush_errors,
            total_written,
            "Store loop terminated with data loss — verify database health"
        );
    } else if flush_errors > 0 {
        tracing::warn!(
            flush_errors,
            total_written,
            "Store loop terminated cleanly with recovered transient errors"
        );
    } else {
        tracing::info!(total_written, "Store loop terminated cleanly with no data loss");
    }
}