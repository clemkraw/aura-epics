//! MonitorEvent -> PvUpdate conversion (slow path only).
//!
//! Handles the first full-update per PV (1 event per PV lifetime).
//! The fast path in `thread.rs` bypasses this entirely for ScalarDelta, StringDelta,
//! and ArrayDelta — 99% of traffic never touches this code.
//!
//! Primary role: extract `PvMetadata` from the initial NTScalar structure
//! (display limits, units, alarm thresholds) on the first Value event.

use std::sync::Arc;

use aura_core::pva::normative::NormativeType;
use aura_core::sample::PvUpdate;
use aura_net::monitor::subscription::MonitorEvent;
use aura_net::types::pva_value::PvaValue;
use aura_net::types::to_normative::to_normative_from_value;

/// Result of converting a MonitorEvent.
#[derive(Debug)]
pub enum ConvertResult {
    /// Value update ready for storage.
    Update(PvUpdate),
    /// First value - includes extracted metadata for pv_metadata table.
    UpdateWithMetadata(PvUpdate, aura_core::metadata::PvMetadata),
    /// Null, unconvertible, or fast-path event - skip.
    Skip,
}

/// Per-PV converter state.
///
/// Tracks whether metadata has been extracted. After the first successful
/// conversion, all subsequent conversions skip metadata extraction.
pub struct PvConverter {
    pv_name: Arc<str>,
    /// Set to true after first successful conversion. Reset on IOC reconnect.
    metadata_extracted: bool,
}

impl PvConverter {
    pub fn new(pv_name: impl Into<Arc<str>>) -> Self {
        Self {
            pv_name: pv_name.into(),
            metadata_extracted: false,
        }
    }

    /// Convert a MonitorEvent into a ConvertResult.
    ///
    /// Only `Value` events are converted - all other variants (ScalarDelta,
    /// StringDelta, ArrayDelta, Disconnect, Reconnect, Error) are handled
    /// elsewhere and return `Skip`.
    pub fn convert(&mut self, event: MonitorEvent) -> ConvertResult {
        match event {
            MonitorEvent::Value(value) => self.convert_value(value),
            _ => ConvertResult::Skip,
        }
    }

    /// Detect NTScalar/NTEnum/NTScalarArray from structure shape and convert.
    fn convert_value(&mut self, value: PvaValue) -> ConvertResult {
        if value.field("value").is_none() {
            return ConvertResult::Skip;
        }
        match to_normative_from_value(&value) {
            Some(nt) => self.maybe_attach_metadata(nt),
            None => ConvertResult::Skip,
        }
    }

    /// On first conversion, extract metadata. Subsequent calls skip (one bool check).
    #[inline]
    fn maybe_attach_metadata(&mut self, nt: NormativeType) -> ConvertResult {
        let update = PvUpdate::new(Arc::clone(&self.pv_name), nt);
        if !self.metadata_extracted {
            self.metadata_extracted = true;
            let meta =
                aura_core::metadata::PvMetadata::from_initial_update(&*self.pv_name, &update.data);
            ConvertResult::UpdateWithMetadata(update, meta)
        } else {
            ConvertResult::Update(update)
        }
    }

    pub fn pv_name(&self) -> &str {
        &*self.pv_name
    }

    /// Reset after IOC reconnect - re-capture metadata on next value in case the PV type or limits changed.
    pub fn reset(&mut self) {
        self.metadata_extracted = false;
    }
}

impl std::fmt::Display for PvConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Converter[{}]", self.pv_name)
    }
}

impl std::fmt::Debug for PvConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PvConverter")
            .field("pv", &self.pv_name)
            .field("metadata_extracted", &self.metadata_extracted)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aura_core::pva::scalars::ScalarValue;

    fn nt_scalar_value(v: f64) -> PvaValue {
        PvaValue::Structure(vec![
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
        ])
    }

    #[test]
    fn test_new() {
        let c = PvConverter::new("PV:A");
        assert_eq!(c.pv_name(), "PV:A");
        assert!(!c.metadata_extracted);
    }

    #[test]
    fn test_first_value_returns_metadata() {
        let mut c = PvConverter::new("PV:A");
        let r = c.convert(MonitorEvent::Value(nt_scalar_value(4.217)));
        assert!(matches!(r, ConvertResult::UpdateWithMetadata(_, _)));
        assert!(c.metadata_extracted);
    }

    #[test]
    fn test_second_value_no_metadata() {
        let mut c = PvConverter::new("PV:A");
        c.convert(MonitorEvent::Value(nt_scalar_value(1.0)));
        let r = c.convert(MonitorEvent::Value(nt_scalar_value(2.0)));
        assert!(matches!(r, ConvertResult::Update(_)));
    }

    #[test]
    fn test_null_value_skips() {
        let mut c = PvConverter::new("PV:A");
        let r = c.convert(MonitorEvent::Value(PvaValue::Null));
        assert!(matches!(r, ConvertResult::Skip));
    }

    #[test]
    fn test_no_value_field_skips() {
        let mut c = PvConverter::new("PV:A");
        let r = c.convert(MonitorEvent::Value(PvaValue::Structure(vec![(
            "other".into(),
            PvaValue::Scalar(ScalarValue::Double(1.0)),
        )])));
        assert!(matches!(r, ConvertResult::Skip));
    }

    #[test]
    fn test_scalar_delta_skips() {
        let mut c = PvConverter::new("PV:A");
        let r = c.convert(MonitorEvent::ScalarDelta {
            value: 1.0,
            seconds: 0,
            nanos: 0,
            severity: 0,
            status: 0,
        });
        assert!(matches!(r, ConvertResult::Skip));
    }

    #[test]
    fn test_reset_allows_metadata_reextraction() {
        let mut c = PvConverter::new("PV:A");
        c.convert(MonitorEvent::Value(nt_scalar_value(1.0)));
        assert!(c.metadata_extracted);

        c.reset();
        assert!(!c.metadata_extracted);

        let r = c.convert(MonitorEvent::Value(nt_scalar_value(2.0)));
        assert!(matches!(r, ConvertResult::UpdateWithMetadata(_, _)));
    }

    #[test]
    fn test_update_has_correct_pv_name() {
        let mut c = PvConverter::new("PERLE:BPM:01:X");
        match c.convert(MonitorEvent::Value(nt_scalar_value(42.0))) {
            ConvertResult::UpdateWithMetadata(u, _) => {
                assert_eq!(&*u.pv_name, "PERLE:BPM:01:X");
            }
            other => panic!("expected UpdateWithMetadata, got {other:?}"),
        }
    }

    #[test]
    fn test_display() {
        assert!(PvConverter::new("PV:A").to_string().contains("PV:A"));
    }

    #[test]
    fn test_debug() {
        assert!(format!("{:?}", PvConverter::new("PV:A")).contains("PvConverter"));
    }
}