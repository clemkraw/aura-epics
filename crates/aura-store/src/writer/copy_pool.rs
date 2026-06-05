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
//!   preferred connection index to avoid contention. If their preferred
//!   connection is busy, tokio-postgres multiplexes automatically — zero blocking.
//!
//! # Concurrency Model
//!
//! `tokio-postgres::Client` is internally multiplexed: multiple concurrent
//! `copy_in` calls on the same `Arc<Client>` are safe. Each gets its own
//! server-side COPY session. No mutex, no checkout, no blocking.

use aura_core::error::{AuraError, AuraResult};
use std::sync::Arc;

/// Pool of tokio-postgres connections for COPY operations.
///
/// All connections are `Arc<Client>` — clone is an atomic increment.
/// Writers receive the full pool and pick connections by index.
#[derive(Clone)]
pub struct CopyPool {
    clients: Vec<Arc<tokio_postgres::Client>>,
}

impl CopyPool {
    pub fn new() -> Self {
        Self {
            clients: Vec::new(),
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            clients: Vec::with_capacity(n),
        }
    }

    /// Register a connection. Call N times at startup.
    pub fn add(&mut self, client: Arc<tokio_postgres::Client>) {
        self.clients.push(client);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.clients.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// Get connection `i` (wraps around if i >= len).
    #[inline]
    pub fn get(&self, i: usize) -> &Arc<tokio_postgres::Client> {
        &self.clients[i % self.clients.len()]
    }

    #[inline]
    pub fn all(&self) -> &[Arc<tokio_postgres::Client>] {
        &self.clients
    }

    /// Send a COPY payload on connection `i`.
    pub async fn send_copy(
        &self,
        conn_idx: usize,
        sql: &str,
        payload: Vec<u8>,
        row_count: usize,
    ) -> AuraResult<()> {
        use futures_util::SinkExt;
        let client = self.get(conn_idx);
        let sink = client
            .copy_in::<_, bytes::Bytes>(sql)
            .await
            .map_err(|e| AuraError::database(format!("COPY begin (conn {conn_idx}): {e}")))?;
        let mut sink = std::pin::pin!(sink);
        sink.send(bytes::Bytes::from(payload)).await.map_err(|e| {
            AuraError::database(format!(
                "COPY send ({row_count} rows, conn {conn_idx}): {e}"
            ))
        })?;
        sink.close()
            .await
            .map_err(|e| AuraError::database(format!("COPY close (conn {conn_idx}): {e}")))?;
        Ok(())
    }

    /// Send a COPY payload as Bytes (caller keeps buffer capacity for reuse).
    pub async fn send_copy_bytes(
        &self,
        conn_idx: usize,
        sql: &str,
        payload: bytes::Bytes,
        row_count: usize,
    ) -> AuraResult<()> {
        use futures_util::SinkExt;
        let client = self.get(conn_idx);
        let sink = client
            .copy_in::<_, bytes::Bytes>(sql)
            .await
            .map_err(|e| AuraError::database(format!("COPY begin (conn {conn_idx}): {e}")))?;
        let mut sink = std::pin::pin!(sink);
        sink.send(payload).await.map_err(|e| {
            AuraError::database(format!(
                "COPY send ({row_count} rows, conn {conn_idx}): {e}"
            ))
        })?;
        sink.close()
            .await
            .map_err(|e| AuraError::database(format!("COPY close (conn {conn_idx}): {e}")))?;
        Ok(())
    }

    /// Send N COPY payloads in parallel, one per connection (round-robin).
    pub async fn send_parallel(
        &self,
        sql: &'static str,
        payloads: Vec<(Vec<u8>, usize)>,
    ) -> AuraResult<()> {
        let futs: Vec<_> = payloads
            .into_iter()
            .enumerate()
            .map(|(i, (payload, row_count))| {
                let client = self.get(i).clone();
                async move {
                    use futures_util::SinkExt;
                    let sink = client
                        .copy_in::<_, bytes::Bytes>(sql)
                        .await
                        .map_err(|e| AuraError::database(format!("COPY begin (conn {i}): {e}")))?;
                    let mut sink = std::pin::pin!(sink);
                    sink.send(bytes::Bytes::from(payload)).await.map_err(|e| {
                        AuraError::database(format!("COPY send ({row_count} rows, conn {i}): {e}"))
                    })?;
                    sink.close()
                        .await
                        .map_err(|e| AuraError::database(format!("COPY close (conn {i}): {e}")))?;
                    Ok::<(), AuraError>(())
                }
            })
            .collect();

        let results = futures_util::future::join_all(futs).await;
        for r in results {
            r?;
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
        let client = self.get(conn_idx);
        client
            .execute(sql, params)
            .await
            .map_err(|e| AuraError::database(format!("execute (conn {conn_idx}): {e}")))
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
            .field("connections", &self.clients.len())
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