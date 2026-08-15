//! Shared writer buffer — per-thread, with dedicated scalar queue.
//!
//! Scalars (99% of traffic) go into a dedicated `Vec<ScalarRow>` (32 bytes/row).
//! parking_lot::Mutex (8ns uncontended vs 25ns std::sync::Mutex).

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::array::ArrayCapture;
use super::image::ImageRow;
use super::json::JsonRow;
use super::scalar::ScalarRow;
use super::string::StringRow;

/// Non-scalar writer row. Bypasses mpsc channel for all types when pv_id is known.
pub enum WriterRow {
    String(StringRow),
    Array(Box<ArrayCapture>),
    Json(JsonRow),
    Image(ImageRow),
}

/// Thread-safe shared buffer with dedicated scalar queue.
pub struct SharedBuffer {
    scalars: Mutex<Vec<ScalarRow>>,
    others: Mutex<Vec<WriterRow>>,
    max_len: usize,
    dropped: AtomicU64,
    /// Shared store-loop wakeup - single Notify across all ingest threads.
    store_notify: std::sync::Arc<tokio::sync::Notify>,
}

impl SharedBuffer {
    pub fn new(
        capacity: usize,
        max_len: usize,
        store_notify: std::sync::Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            scalars: Mutex::new(Vec::with_capacity(capacity.min(max_len))),
            others: Mutex::new(Vec::with_capacity(128)),
            max_len,
            dropped: AtomicU64::new(0),
            store_notify,
        }
    }

    #[inline]
    pub fn push_scalar(&self, row: ScalarRow) -> bool {
        let mut buf = self.scalars.lock();
        if buf.len() < self.max_len {
            let was_empty = buf.is_empty();
            buf.push(row);
            if was_empty {
                self.store_notify.notify_one();
            }
            true
        } else {
            drop(buf);
            self.dropped.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    #[inline]
    pub fn push_other(&self, row: WriterRow) -> bool {
        let mut buf = self.others.lock();
        if buf.len() < self.max_len {
            let was_empty = buf.is_empty();
            buf.push(row);
            if was_empty {
                self.store_notify.notify_one();
            }
            true
        } else {
            drop(buf);
            self.dropped.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    #[inline]
    pub fn take_scalars(&self) -> Vec<ScalarRow> {
        let mut buf = self.scalars.lock();
        let cap = buf.capacity().min(self.max_len);
        std::mem::replace(&mut *buf, Vec::with_capacity(cap))
    }

    #[inline]
    pub fn take_others(&self) -> Vec<WriterRow> {
        let mut buf = self.others.lock();
        std::mem::replace(&mut *buf, Vec::with_capacity(128))
    }

    pub fn len(&self) -> usize {
        self.scalars.lock().len() + self.others.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.scalars.lock().is_empty() && self.others.lock().is_empty()
    }

    pub fn total_dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

// Send + Sync auto-derived: parking_lot::Mutex<Vec<T>> is Send+Sync when T: Send.

#[cfg(test)]
mod tests {
    use super::*;
    use aura_core::sample::StoreReason;

    fn scalar(pv_id: i32, value: f64) -> ScalarRow {
        ScalarRow::new(
            chrono::Utc::now(),
            pv_id,
            value,
            0,
            0,
            StoreReason::ValueChanged,
        )
    }

    #[test]
    fn push_scalar_and_take() {
        let n = std::sync::Arc::new(tokio::sync::Notify::new());
        let buf = SharedBuffer::new(100, 1000, n.clone());
        assert!(buf.push_scalar(scalar(1, 42.0)));
        assert!(buf.push_scalar(scalar(2, 43.0)));
        assert_eq!(buf.len(), 2);
        let rows = buf.take_scalars();
        assert_eq!(rows.len(), 2);
        assert!(buf.is_empty());
    }

    #[test]
    fn take_returns_empty_after_drain() {
        let n = std::sync::Arc::new(tokio::sync::Notify::new());
        let buf = SharedBuffer::new(100, 1000, n.clone());
        buf.push_scalar(scalar(1, 1.0));
        let _ = buf.take_scalars();
        let rows = buf.take_scalars();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn backpressure_when_full() {
        let n = std::sync::Arc::new(tokio::sync::Notify::new());
        let buf = SharedBuffer::new(2, 2, n.clone());
        assert!(buf.push_scalar(scalar(1, 1.0)));
        assert!(buf.push_scalar(scalar(2, 2.0)));
        assert!(!buf.push_scalar(scalar(3, 3.0))); // full
        assert_eq!(buf.total_dropped(), 1);
    }

    #[test]
    fn others_path() {
        let n = std::sync::Arc::new(tokio::sync::Notify::new());
        let buf = SharedBuffer::new(100, 1000, n.clone());
        buf.push_other(WriterRow::String(StringRow::new(
            chrono::Utc::now(),
            1,
            "hello".to_string(),
            0,
            0,
        )));
        assert_eq!(buf.len(), 1);
        let others = buf.take_others();
        assert_eq!(others.len(), 1);
    }

    #[test]
    fn concurrent_push_scalar() {
        use std::sync::Arc;
        let n = std::sync::Arc::new(tokio::sync::Notify::new());
        let buf = Arc::new(SharedBuffer::new(100_000, 100_000, n.clone()));
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let buf = Arc::clone(&buf);
                std::thread::spawn(move || {
                    for i in 0..1000 {
                        buf.push_scalar(scalar(t * 1000 + i, i as f64));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let rows = buf.take_scalars();
        assert_eq!(rows.len(), 4000);
    }
}
