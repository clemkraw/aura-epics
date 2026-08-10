//! Shared pool of `tokio-postgres` connections for all COPY operations.
//!
//! # Design
//!
//! A fixed set of N connections serves all writer types.
//! Each writer receives the full pool and decides how many connections to use based on its batch size:
//!
//! - **Scalar** (hot path): uses ALL N connections for N-way parallel COPY.
//!
//! - **Secondary writers** (string, array, json, image): each assigned a
//!   preferred connection index to spread load across the pool.
//!
//! # Concurrency Model
//!
//! Concurrent `copy_in` calls on the same `Arc<Client>` are SAFE (no mutex,
//! no checkout — tokio-postgres pipelines requests internally) but NOT
//! parallel: the PostgreSQL wire protocol allows ONE active COPY per
//! connection, so a second copy_in on the same client is queued until the
//! first completes. True parallelism comes from using N distinct
//! connections; a secondary writer whose preferred connection is busy with
//! a scalar chunk simply waits its turn on that connection (a few ms).

use arc_swap::ArcSwap;
use aura_core::error::{AuraError, AuraResult};
use std::sync::Arc;

/// A classified COPY error: carries whether the failure is transient
/// (connection lost, server restarting, resource pressure — worth retrying)
/// or permanent (schema mismatch, bad data — retrying cannot succeed).
#[derive(Debug, Clone)]
pub struct CopyError {
    pub message: String,
    pub transient: bool,
}

impl CopyError {
    fn pool_empty() -> Self {
        Self {
            message: "CopyPool is empty — no COPY connections established".to_string(),
            transient: false,
        }
    }

    fn from_pg(stage: &str, conn_idx: usize, row_count: usize, e: &tokio_postgres::Error) -> Self {
        Self {
            message: format!("{stage} ({row_count} rows, conn {conn_idx}): {e}"),
            transient: is_transient_pg_error(e),
        }
    }
}

impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} [{}]",
            self.message,
            if self.transient {
                "transient"
            } else {
                "permanent"
            }
        )
    }
}

impl std::error::Error for CopyError {}

/// Classify a tokio-postgres error as transient (retryable) or permanent.
///
/// Transient: the connection died, the server is restarting/shutting down,
/// resources are exhausted, or a deadlock/serialization conflict occurred.
/// Permanent: data/schema errors — resending identical bytes cannot succeed.
pub fn is_transient_pg_error(e: &tokio_postgres::Error) -> bool {
    if e.is_closed() {
        return true;
    }
    match e.code() {
        // No SQLSTATE → IO/protocol-level failure (network blip, server gone).
        None => true,
        Some(state) => {
            let c = state.code();
            // 08xxx connection exception, 40xxx transaction rollback (deadlock),
            // 53xxx insufficient resources, 57xxx operator intervention
            // (includes 57P01 admin_shutdown sent on `pg_ctl restart`).
            c.starts_with("08") || c.starts_with("40") || c.starts_with("53") || c.starts_with("57")
        }
    }
}

/// Pool of tokio-postgres connections for COPY operations.
///
/// The client list lives behind a shared `ArcSwap`: every clone of the pool
/// (the master in `BatchWriter`, the five per-writer copies, and the copy
/// embedded in any in-flight/retained `FlushBundle`) observes the SAME list.
/// This is what makes [`CopyPool::reconnect_dead`] work: `tokio_postgres`
/// clients NEVER reconnect on their own, so after a PostgreSQL restart the
/// pool must be healed in one place and the repair must be visible to every
/// holder at once — including a bundle that is already waiting for retry.
#[derive(Clone)]
pub struct CopyPool {
    clients: Arc<ArcSwap<Vec<Arc<tokio_postgres::Client>>>>,
}

impl CopyPool {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }

    pub fn with_capacity(_n: usize) -> Self {
        // Capacity hint kept for API compatibility; the list is rebuilt on
        // each mutation (startup + rare reconnects), so it buys nothing.
        Self::new()
    }

    /// Register a connection. Call N times at startup (single-threaded).
    pub fn add(&mut self, client: Arc<tokio_postgres::Client>) {
        let mut next: Vec<Arc<tokio_postgres::Client>> = (**self.clients.load()).clone();
        next.push(client);
        self.clients.store(Arc::new(next));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.clients.load().len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.clients.load().is_empty()
    }

    /// Get connection `i` (wraps around if i >= len).
    ///
    /// Precondition: the pool is non-empty. All public send/execute entry
    /// points check `is_empty()` first and return an error instead of
    /// reaching this (which would otherwise divide by zero under
    /// `panic = "abort"` and kill the whole archiver).
    #[inline]
    pub fn get(&self, i: usize) -> Arc<tokio_postgres::Client> {
        let clients = self.clients.load();
        debug_assert!(!clients.is_empty(), "CopyPool::get on empty pool");
        Arc::clone(&clients[i % clients.len()])
    }

    /// Snapshot of the current client list (owned).
    pub fn all(&self) -> Vec<Arc<tokio_postgres::Client>> {
        (**self.clients.load()).clone()
    }

    /// Send a COPY payload on connection `i`, with error classification.
    ///
    /// This is the core implementation. Returns [`CopyError`] carrying a
    /// `transient` flag so callers can decide whether a retry makes sense.
    pub async fn send_copy_classified(
        &self,
        conn_idx: usize,
        sql: &str,
        payload: bytes::Bytes,
        row_count: usize,
    ) -> Result<(), CopyError> {
        use futures_util::SinkExt;
        if self.is_empty() {
            return Err(CopyError::pool_empty());
        }
        let client = self.get(conn_idx);
        let sink = client
            .copy_in::<_, bytes::Bytes>(sql)
            .await
            .map_err(|e| CopyError::from_pg("COPY begin", conn_idx, row_count, &e))?;
        let mut sink = std::pin::pin!(sink);
        sink.send(payload)
            .await
            .map_err(|e| CopyError::from_pg("COPY send", conn_idx, row_count, &e))?;
        sink.close()
            .await
            .map_err(|e| CopyError::from_pg("COPY close", conn_idx, row_count, &e))?;
        Ok(())
    }

    /// Send a COPY payload on connection `i` (legacy interface).
    pub async fn send_copy(
        &self,
        conn_idx: usize,
        sql: &str,
        payload: Vec<u8>,
        row_count: usize,
    ) -> AuraResult<()> {
        self.send_copy_classified(conn_idx, sql, bytes::Bytes::from(payload), row_count)
            .await
            .map_err(|e| AuraError::database(e.message))
    }

    /// Send a COPY payload as Bytes (caller keeps buffer capacity for reuse).
    pub async fn send_copy_bytes(
        &self,
        conn_idx: usize,
        sql: &str,
        payload: bytes::Bytes,
        row_count: usize,
    ) -> AuraResult<()> {
        self.send_copy_classified(conn_idx, sql, payload, row_count)
            .await
            .map_err(|e| AuraError::database(e.message))
    }

    /// Send N COPY payloads in parallel, one per connection (round-robin),
    /// returning a per-payload result **in input order**.
    ///
    /// Each COPY is its own implicit transaction: some chunks may commit
    /// while others fail. The caller maps failed indices back to row ranges
    /// to retry only what actually failed (no duplicates for committed chunks).
    pub async fn send_parallel_classified(
        &self,
        sql: &'static str,
        payloads: Vec<(Vec<u8>, usize)>,
    ) -> Vec<Result<usize, CopyError>> {
        if self.is_empty() {
            return payloads
                .iter()
                .map(|_| Err(CopyError::pool_empty()))
                .collect();
        }
        let futs: Vec<_> = payloads
            .into_iter()
            .enumerate()
            .map(|(i, (payload, row_count))| {
                let client = self.get(i);
                async move {
                    use futures_util::SinkExt;
                    let sink = client
                        .copy_in::<_, bytes::Bytes>(sql)
                        .await
                        .map_err(|e| CopyError::from_pg("COPY begin", i, row_count, &e))?;
                    let mut sink = std::pin::pin!(sink);
                    sink.send(bytes::Bytes::from(payload))
                        .await
                        .map_err(|e| CopyError::from_pg("COPY send", i, row_count, &e))?;
                    sink.close()
                        .await
                        .map_err(|e| CopyError::from_pg("COPY close", i, row_count, &e))?;
                    Ok::<usize, CopyError>(row_count)
                }
            })
            .collect();

        futures_util::future::join_all(futs).await
    }

    /// Send N COPY payloads in parallel (legacy interface: first error wins).
    pub async fn send_parallel(
        &self,
        sql: &'static str,
        payloads: Vec<(Vec<u8>, usize)>,
    ) -> AuraResult<()> {
        for r in self.send_parallel_classified(sql, payloads).await {
            if let Err(e) = r {
                return Err(AuraError::database(e.message));
            }
        }
        Ok(())
    }

    /// Execute a parameterized query on connection `i`.
    /// Used by pv_cache for metadata lookups/inserts.
    pub async fn execute(
        &self,
        conn_idx: usize,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> AuraResult<u64> {
        if self.is_empty() {
            return Err(AuraError::database(
                "CopyPool is empty — no COPY connections established".to_string(),
            ));
        }
        let client = self.get(conn_idx);
        client
            .execute(sql, params)
            .await
            .map_err(|e| AuraError::database(format!("execute (conn {conn_idx}): {e}")))
    }

    /// Re-establish every dead connection in place.
    ///
    /// `tokio_postgres::Client` has no auto-reconnect: once PostgreSQL goes
    /// away (restart, SIGKILL, failover) the client is closed forever and
    /// every COPY on it fails with "connection closed". Found by the
    /// aura-bench durability scenario: without healing, the store loop's
    /// bounded retry faithfully re-sends bundles onto dead clients until
    /// attempts are exhausted — and the data is lost even though the server
    /// came back within seconds.
    pub async fn reconnect_dead(&self, url: &str) -> (usize, usize) {
        let current = self.clients.load_full();
        if !current.iter().any(|c| c.is_closed()) {
            return (0, 0);
        }
        let mut next: Vec<Arc<tokio_postgres::Client>> = (*current).clone();
        let (mut reconnected, mut still_dead) = (0usize, 0usize);
        for (i, slot) in next.iter_mut().enumerate() {
            if !slot.is_closed() {
                continue;
            }
            let dial = tokio_postgres::connect(url, tokio_postgres::NoTls);
            match tokio::time::timeout(std::time::Duration::from_secs(5), dial).await {
                Ok(Ok((client, connection))) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            tracing::error!(conn = i, "tokio-postgres COPY connection lost: {e}");
                        }
                    });
                    *slot = Arc::new(client);
                    reconnected += 1;
                }
                Ok(Err(e)) => {
                    tracing::warn!(conn = i, "COPY reconnect failed: {e}");
                    still_dead += 1;
                }
                Err(_) => {
                    tracing::warn!(conn = i, "COPY reconnect timed out (5 s)");
                    still_dead += 1;
                }
            }
        }
        if reconnected > 0 {
            self.clients.store(Arc::new(next));
        }
        (reconnected, still_dead)
    }
}

impl Default for CopyPool {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CopyPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyPool")
            .field("connections", &self.len())
            .finish()
    }
}

/// Binary COPY header: signature(11) + flags(4) + extension(4) = 19 bytes.
pub const PGCOPY_HEADER: [u8; 19] = [
    b'P', b'G', b'C', b'O', b'P', b'Y', b'\n', 0xff, b'\r', b'\n', 0x00, 0, 0, 0,
    0, // flags i32
    0, 0, 0, 0, // header extension length i32
];

/// Binary COPY trailer: -1 as i16 big-endian.
pub const PGCOPY_TRAILER: [u8; 2] = [0xff, 0xff];

/// PostgreSQL epoch: 2000-01-01 00:00:00 UTC in microseconds since Unix epoch.
pub const PG_EPOCH_OFFSET_US: i64 = 946_684_800 * 1_000_000;

/// Result of pushing a row into any writer's buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    Accepted,
    Full,
    BackpressureExceeded,
}

impl PushResult {
    #[inline]
    pub fn needs_flush(&self) -> bool {
        !matches!(self, Self::Accepted)
    }
    #[inline]
    pub fn is_accepted(&self) -> bool {
        !matches!(self, Self::BackpressureExceeded)
    }
}

impl std::fmt::Display for PushResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Accepted => "accepted",
            Self::Full => "full",
            Self::BackpressureExceeded => "backpressure",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_accepted() {
        assert!(!PushResult::Accepted.needs_flush());
        assert!(PushResult::Accepted.is_accepted());
        assert_eq!(PushResult::Accepted.to_string(), "accepted");
    }

    #[test]
    fn push_full() {
        assert!(PushResult::Full.needs_flush());
        assert!(PushResult::Full.is_accepted());
    }

    #[test]
    fn push_backpressure() {
        assert!(PushResult::BackpressureExceeded.needs_flush());
        assert!(!PushResult::BackpressureExceeded.is_accepted());
    }

    #[test]
    fn constants() {
        assert_eq!(PGCOPY_HEADER.len(), 19);
        assert_eq!(PGCOPY_TRAILER, [0xff, 0xff]);
        assert_eq!(i16::from_be_bytes(PGCOPY_TRAILER), -1);
        assert_eq!(PG_EPOCH_OFFSET_US, 946_684_800_000_000);
    }

    #[test]
    fn pool_empty() {
        let p = CopyPool::new();
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn pool_default() {
        assert!(CopyPool::default().is_empty());
    }

    #[test]
    fn pool_clone() {
        let p = CopyPool::with_capacity(4);
        assert_eq!(p.len(), p.clone().len());
    }

    #[test]
    fn pool_debug() {
        assert!(format!("{:?}", CopyPool::new()).contains("CopyPool"));
    }
}
