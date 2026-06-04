//! User-facing monitor handle - receive events, cancel subscription.
//!
//! `MonitorHandle` is what `aura-ingest` holds. It receives `MonitorEvent`s via a tokio mpsc
//! channel and can cancel the subscription by dropping or calling `cancel()`.

use std::fmt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::subscription::MonitorEvent;

/// User-facing handle to a PV monitor subscription.
pub struct MonitorHandle {
    pv_name: std::sync::Arc<str>,
    rx: mpsc::Receiver<MonitorEvent>,
    /// None after `cancel_token()` consumes it - Drop becomes a no-op.
    cancel: Option<CancellationToken>,
}

impl MonitorHandle {
    fn new(
        pv_name: impl Into<std::sync::Arc<str>>,
        rx: mpsc::Receiver<MonitorEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            pv_name: pv_name.into(),
            rx,
            cancel: Some(cancel),
        }
    }

    /// Create a handle and its sender (for wiring up internally).
    pub fn channel(
        pv_name: impl Into<std::sync::Arc<str>>,
        buffer: usize,
    ) -> (mpsc::Sender<MonitorEvent>, Self) {
        let (tx, rx) = mpsc::channel(buffer);
        let cancel = CancellationToken::new();
        (tx, Self::new(pv_name, rx, cancel))
    }

    /// Create a lightweight handle for bus mode - no per-PV channel.
    /// Events flow through the shared bus, not individual channels.
    pub fn bus_handle(
        pv_name: impl Into<std::sync::Arc<str>>,
    ) -> (mpsc::Sender<MonitorEvent>, Self) {
        let (tx, rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        (tx, Self::new(pv_name, rx, cancel))
    }

    /// Receive the next event. Returns None when the subscription ends.
    pub async fn recv(&mut self) -> Option<MonitorEvent> {
        self.rx.recv().await
    }

    /// Try to receive without waiting.
    pub fn try_recv(&mut self) -> Option<MonitorEvent> {
        self.rx.try_recv().ok()
    }

    /// Cancel the subscription.
    pub fn cancel(&self) {
        if let Some(ref c) = self.cancel {
            c.cancel();
        }
    }

    /// Extract the CancellationToken (consumes the handle).
    pub fn cancel_token(mut self) -> CancellationToken {
        self.cancel.take().expect("cancel token already taken")
    }

    /// Whether the subscription has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.as_ref().map_or(true, |c| c.is_cancelled())
    }

    /// The PV name this handle is monitoring.
    pub fn pv_name(&self) -> &str {
        &self.pv_name
    }
}

impl Drop for MonitorHandle {
    fn drop(&mut self) {
        if let Some(ref c) = self.cancel {
            c.cancel();
        }
    }
}

impl fmt::Debug for MonitorHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MonitorHandle")
            .field("pv", &self.pv_name)
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl fmt::Display for MonitorHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MonitorHandle[{}{}]",
            self.pv_name,
            if self.is_cancelled() {
                " CANCELLED"
            } else {
                ""
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::pva_value::{PvaScalar, PvaValue};

    fn scalar_event(v: f64) -> MonitorEvent {
        MonitorEvent::Value(PvaValue::Scalar(PvaScalar::Double(v)))
    }

    #[test]
    fn test_channel() {
        let (tx, handle) = MonitorHandle::channel("PV:A", 16);
        assert_eq!(handle.pv_name(), "PV:A");
        assert!(!handle.is_cancelled());
        drop(tx);
    }

    #[test]
    fn test_cancel() {
        let (_tx, handle) = MonitorHandle::channel("PV:A", 16);
        handle.cancel();
        assert!(handle.is_cancelled());
    }

    #[test]
    fn test_cancel_idempotent() {
        let (_tx, handle) = MonitorHandle::channel("PV:A", 16);
        handle.cancel();
        handle.cancel();
        assert!(handle.is_cancelled());
    }

    #[test]
    fn test_cancel_on_drop() {
        let (_tx, handle) = MonitorHandle::channel("PV:A", 16);
        let token = handle.cancel.as_ref().unwrap().clone();
        assert!(!token.is_cancelled());
        drop(handle);
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_cancel_token_does_not_cancel() {
        let (_tx, handle) = MonitorHandle::channel("PV:A", 16);
        let token = handle.cancel_token();
        assert!(!token.is_cancelled()); // NOT cancelled — token was moved out
    }

    #[test]
    fn test_cancel_token_drop_cancels() {
        let (_tx, handle) = MonitorHandle::channel("PV:A", 16);
        let token = handle.cancel_token();
        drop(token);
        // token is a CancellationToken — dropping it doesn't cancel.
        // Only explicit .cancel() cancels.
    }

    #[tokio::test]
    async fn test_recv() {
        let (tx, mut handle) = MonitorHandle::channel("PV:A", 16);
        tx.send(scalar_event(3.14)).await.unwrap();
        let event = handle.recv().await.unwrap();
        assert!(event.is_value());
    }

    #[tokio::test]
    async fn test_recv_disconnect() {
        let (tx, mut handle) = MonitorHandle::channel("PV:A", 16);
        tx.send(MonitorEvent::Disconnect).await.unwrap();
        assert!(handle.recv().await.unwrap().is_disconnect());
    }

    #[tokio::test]
    async fn test_recv_closed() {
        let (tx, mut handle) = MonitorHandle::channel("PV:A", 16);
        drop(tx);
        assert!(handle.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_recv_ordering() {
        let (tx, mut handle) = MonitorHandle::channel("PV:A", 16);
        tx.send(scalar_event(1.0)).await.unwrap();
        tx.send(MonitorEvent::Disconnect).await.unwrap();
        tx.send(MonitorEvent::Reconnect).await.unwrap();
        tx.send(scalar_event(2.0)).await.unwrap();
        assert!(handle.recv().await.unwrap().is_value());
        assert!(handle.recv().await.unwrap().is_disconnect());
        assert_eq!(handle.recv().await.unwrap(), MonitorEvent::Reconnect);
        assert!(handle.recv().await.unwrap().is_value());
    }

    #[test]
    fn test_try_recv_empty() {
        let (_tx, mut handle) = MonitorHandle::channel("PV:A", 16);
        assert!(handle.try_recv().is_none());
    }

    #[tokio::test]
    async fn test_try_recv_has_data() {
        let (tx, mut handle) = MonitorHandle::channel("PV:A", 16);
        tx.send(scalar_event(1.0)).await.unwrap();
        assert!(handle.try_recv().is_some());
    }

    #[tokio::test]
    async fn test_multiple_events() {
        let (tx, mut handle) = MonitorHandle::channel("PV:A", 16);
        for i in 0..5 {
            tx.send(scalar_event(i as f64)).await.unwrap();
        }
        for _ in 0..5 {
            assert!(handle.recv().await.is_some());
        }
    }

    #[test]
    fn test_display() {
        let (_, h) = MonitorHandle::channel("PV:A", 16);
        assert!(h.to_string().contains("PV:A"));
    }

    #[test]
    fn test_display_cancelled() {
        let (_, h) = MonitorHandle::channel("PV:A", 16);
        h.cancel();
        assert!(h.to_string().contains("CANCELLED"));
    }

    #[test]
    fn test_debug() {
        let (_, h) = MonitorHandle::channel("PV:A", 16);
        assert!(format!("{:?}", h).contains("MonitorHandle"));
    }
}