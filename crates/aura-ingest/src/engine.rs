//! Ingest engine - slow-path MonitorEvent processing and metadata extraction.
//!
//! The fast path (99% of traffic) in `thread.rs` handles ScalarDelta,
//! StringDelta, and ArrayDelta directly without touching the engine.
//!
//! The engine handles:
//! - First Value event per PV -> metadata extraction via PvConverter
//! - Non-standard types that don't match the fast path
//! - PV registration/unregistration lifecycle

use std::collections::HashMap;
use std::sync::Arc;

use crate::converter::{ConvertResult, PvConverter};
use aura_core::sample::PvUpdate;

/// Result of processing a single MonitorEvent.
#[allow(clippy::large_enum_variant)]
pub enum ProcessResult {
    /// A sample was produced - send it to the caller.
    Sample(PvUpdate),
    /// No sample produced (skip, unconvertible, etc.).
    Skip,
}

/// Ingest engine - manages per-PV converters and metadata extraction.
#[derive(Default)]
pub struct IngestEngine {
    pub converters: HashMap<Arc<str>, PvConverter>,
    /// Metadata extracted from first updates, pending DB write.
    pub pending_metadata: Vec<aura_core::metadata::PvMetadata>,
    pub total_events: u64,
    pub total_published: u64,
    pub total_skipped: u64,
}

impl IngestEngine {
    pub fn new() -> Self {
        Self {
            converters: HashMap::new(),
            pending_metadata: Vec::new(),
            total_events: 0,
            total_published: 0,
            total_skipped: 0,
        }
    }

    /// Create a converter for a PV (called before subscribing).
    pub fn register_pv(&mut self, pv_name: &str) {
        if !self.converters.contains_key(pv_name) {
            self.converters
                .insert(Arc::from(pv_name), PvConverter::new(pv_name));
        }
    }

    /// Remove a PV (unsubscribe).
    pub fn unregister_pv(&mut self, pv_name: &str) {
        self.converters.remove(pv_name);
    }

    /// Process a MonitorEvent for a PV (slow path only).
    ///
    /// Called for Value events that bypass the fast path (first event per PV, non-NTScalar
    /// types, etc.). Returns the produced PvUpdate if converted.
    /// Metadata is accumulated in `pending_metadata` for later bulk insert.
    pub fn process_event(
        &mut self,
        pv_name: &str,
        event: aura_net::monitor::subscription::MonitorEvent,
    ) -> ProcessResult {
        self.total_events += 1;

        let converter = match self.converters.get_mut(pv_name) {
            Some(c) => c,
            None => {
                self.total_skipped += 1;
                return ProcessResult::Skip;
            }
        };

        match converter.convert(event) {
            ConvertResult::Update(update) => {
                self.total_published += 1;
                ProcessResult::Sample(update)
            }
            ConvertResult::UpdateWithMetadata(update, meta) => {
                self.pending_metadata.push(meta);
                self.total_published += 1;
                ProcessResult::Sample(update)
            }
            ConvertResult::Skip => {
                self.total_skipped += 1;
                ProcessResult::Skip
            }
        }
    }

    pub fn active_pv_count(&self) -> usize {
        self.converters.len()
    }
}

impl std::fmt::Debug for IngestEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestEngine")
            .field("pvs", &self.converters.len())
            .field("events", &self.total_events)
            .field("published", &self.total_published)
            .field("skipped", &self.total_skipped)
            .finish()
    }
}

impl std::fmt::Display for IngestEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IngestEngine[pvs={} events={} pub={} skip={}]",
            self.converters.len(),
            self.total_events,
            self.total_published,
            self.total_skipped
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aura_core::pva::scalars::ScalarValue;
    use aura_net::monitor::subscription::MonitorEvent;
    use aura_net::types::pva_value::PvaValue;

    fn nt_scalar_event(v: f64) -> MonitorEvent {
        MonitorEvent::Value(PvaValue::Structure(vec![
            ("value".into(), PvaValue::Scalar(ScalarValue::Double(v))),
            (
                "alarm".into(),
                PvaValue::Structure(vec![
                    ("severity".into(), PvaValue::Scalar(ScalarValue::Int(0))),
                    ("status".into(), PvaValue::Scalar(ScalarValue::Int(0))),
                    (
                        "message".into(),
                        PvaValue::Scalar(ScalarValue::String(String::new())),
                    ),
                ]),
            ),
            (
                "timeStamp".into(),
                PvaValue::Structure(vec![
                    (
                        "secondsPastEpoch".into(),
                        PvaValue::Scalar(ScalarValue::Long(1700000000)),
                    ),
                    ("nanoseconds".into(), PvaValue::Scalar(ScalarValue::Int(0))),
                    ("userTag".into(), PvaValue::Scalar(ScalarValue::Int(0))),
                ]),
            ),
        ]))
    }

    #[test]
    fn test_new() {
        let e = IngestEngine::new();
        assert_eq!(e.active_pv_count(), 0);
        assert_eq!(e.total_events, 0);
    }

    #[test]
    fn test_register_pv() {
        let mut e = IngestEngine::new();
        e.register_pv("PV:A");
        assert_eq!(e.active_pv_count(), 1);
    }

    #[test]
    fn test_register_dedup() {
        let mut e = IngestEngine::new();
        e.register_pv("PV:A");
        e.register_pv("PV:A");
        assert_eq!(e.active_pv_count(), 1);
    }

    #[test]
    fn test_unregister() {
        let mut e = IngestEngine::new();
        e.register_pv("PV:A");
        e.unregister_pv("PV:A");
        assert_eq!(e.active_pv_count(), 0);
    }

    #[test]
    fn test_process_unknown_pv() {
        let mut e = IngestEngine::new();
        let r = e.process_event("PV:UNKNOWN", MonitorEvent::Value(PvaValue::Null));
        assert!(matches!(r, ProcessResult::Skip));
        assert_eq!(e.total_skipped, 1);
    }

    #[test]
    fn test_process_value() {
        let mut e = IngestEngine::new();
        e.register_pv("PV:A");
        let result = e.process_event("PV:A", nt_scalar_event(4.217));
        assert!(matches!(result, ProcessResult::Sample(_)));
        assert_eq!(e.total_published, 1);
    }

    #[test]
    fn test_first_value_captures_metadata() {
        let mut e = IngestEngine::new();
        e.register_pv("PV:A");
        e.process_event("PV:A", nt_scalar_event(1.0));
        assert_eq!(e.pending_metadata.len(), 1);
        e.process_event("PV:A", nt_scalar_event(2.0));
        assert_eq!(e.pending_metadata.len(), 1);
    }

    #[test]
    fn test_process_null_skips() {
        let mut e = IngestEngine::new();
        e.register_pv("PV:A");
        let r = e.process_event("PV:A", MonitorEvent::Value(PvaValue::Null));
        assert!(matches!(r, ProcessResult::Skip));
    }

    #[test]
    fn test_scalar_delta_skips() {
        let mut e = IngestEngine::new();
        e.register_pv("PV:A");
        let r = e.process_event(
            "PV:A",
            MonitorEvent::ScalarDelta {
                value: 1.0,
                seconds: 0,
                nanos: 0,
                severity: 0,
                status: 0,
            },
        );
        assert!(matches!(r, ProcessResult::Skip));
    }

    #[test]
    fn test_display() {
        assert!(IngestEngine::new().to_string().contains("IngestEngine"));
    }
    #[test]
    fn test_debug() {
        assert!(format!("{:?}", IngestEngine::new()).contains("IngestEngine"));
    }
}
