//! Shared pool of `tokio-postgres` connections for all COPY operations.
//!
//! # Design
//!
//! A fixed set of N connections (default 8) serves all writer types.
//! Each writer receives the full pool and decides how many connections to use based on its batch size:
//!
//! - **Scalar** (hot path): uses ALL N connections for N-way parallel COPY.
//!
//! - **Secondary writers** (string, array, json, image): each assigned a
//!   preferred connection index to avoid contention. If their preferred
//!   connection is busy with scalar COPY, tokio-postgres multiplexes automatically — zero blocking.
//!
//! # Concurrency Model
//!
//! `tokio-postgres::Client` is internally multiplexed: multiple concurrent
//! `copy_in` calls on the same `Arc<Client>` are safe. Each gets its own
//! server-side COPY session. No mutex, no checkout, no blocking.

use aura_core::error::{AuraError, AuraResult};
use std::sync::Arc;

/// Default number of connections.
pub const DEFAULT_POOL_SIZE: usize = 8;

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

    /// Get all connections as a slice.
    #[inline]
    pub fn all(&self) -> &[Arc<tokio_postgres::Client>] {
        &self.clients
    }

    /// Send a COPY payload (binary or text) on connection `i`.
    ///
    /// Zero-copy: `Bytes::from(payload)` transfers ownership to the
    /// TCP socket without memcpy.
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

    /// Send N COPY payloads in parallel, one per connection (round-robin).
    ///
    /// Used by scalar writer for N-way parallel flush.
    /// Each payload goes to connection `i % pool_size`.
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
    ///
    /// Used by image writer for individual INSERTs (BYTEA columns don't benefit from COPY).
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

/// Escape a string for PostgreSQL COPY text format.
///
/// Fast path: if no special chars, appends directly without char scan.
#[inline]
pub fn escape_copy_text(value: &str, out: &mut String) {
    if !value
        .bytes()
        .any(|b| b == b'\\' || b == b'\t' || b == b'\n' || b == b'\r')
    {
        out.push_str(value);
        return;
    }
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── PushResult ───────────────────────────────────────────────

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
    fn push_eq() {
        assert_eq!(PushResult::Full, PushResult::Full);
        assert_ne!(PushResult::Full, PushResult::Accepted);
    }

    // ── Constants ────────────────────────────────────────────────

    #[test]
    fn pgcopy_header_size() {
        assert_eq!(PGCOPY_HEADER.len(), 19);
    }
    #[test]
    fn pgcopy_trailer_size() {
        assert_eq!(PGCOPY_TRAILER.len(), 2);
    }
    #[test]
    fn pgcopy_trailer_is_minus_one() {
        let v = i16::from_be_bytes(PGCOPY_TRAILER);
        assert_eq!(v, -1);
    }
    #[test]
    fn pg_epoch() {
        assert_eq!(PG_EPOCH_OFFSET_US, 946_684_800_000_000);
    }
    #[test]
    fn default_pool_size() {
        assert!(DEFAULT_POOL_SIZE >= 4);
    }

    // ── CopyPool ─────────────────────────────────────────────────

    #[test]
    fn pool_empty() {
        let p = CopyPool::new();
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn pool_default() {
        let p = CopyPool::default();
        assert!(p.is_empty());
    }

    #[test]
    fn pool_clone() {
        let p = CopyPool::with_capacity(4);
        let p2 = p.clone();
        assert_eq!(p.len(), p2.len());
    }

    #[test]
    fn pool_debug() {
        let d = format!("{:?}", CopyPool::new());
        assert!(d.contains("CopyPool") && d.contains("0"));
    }

    // ── Escape ───────────────────────────────────────────────────

    #[test]
    fn escape_plain() {
        let mut o = String::new();
        escape_copy_text("hello world", &mut o);
        assert_eq!(o, "hello world");
    }

    #[test]
    fn escape_tab() {
        let mut o = String::new();
        escape_copy_text("a\tb", &mut o);
        assert_eq!(o, "a\\tb");
    }

    #[test]
    fn escape_newline() {
        let mut o = String::new();
        escape_copy_text("a\nb", &mut o);
        assert_eq!(o, "a\\nb");
    }

    #[test]
    fn escape_cr() {
        let mut o = String::new();
        escape_copy_text("a\rb", &mut o);
        assert_eq!(o, "a\\rb");
    }

    #[test]
    fn escape_backslash() {
        let mut o = String::new();
        escape_copy_text("a\\b", &mut o);
        assert_eq!(o, "a\\\\b");
    }

    #[test]
    fn escape_combined() {
        let mut o = String::new();
        escape_copy_text("a\t\n\r\\b", &mut o);
        assert_eq!(o, "a\\t\\n\\r\\\\b");
    }

    #[test]
    fn escape_empty() {
        let mut o = String::new();
        escape_copy_text("", &mut o);
        assert_eq!(o, "");
    }

    #[test]
    fn escape_unicode() {
        let mut o = String::new();
        escape_copy_text("température: 4.2°K", &mut o);
        assert_eq!(o, "température: 4.2°K");
    }

    #[test]
    fn escape_fast_path_no_scan() {
        let mut o = String::new();
        let big = "a".repeat(10_000);
        escape_copy_text(&big, &mut o);
        assert_eq!(o.len(), 10_000);
    }
}
