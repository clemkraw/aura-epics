//! Per-PV filter state machine.
//!
//! Each PV has its own [`PvFilter`] instance that tracks the last
//! stored value, timestamp, and alarm severity. On each incoming
//! sample, [`decide`] returns whether to store or drop.
//!
//! ## Decision priority
//!
//! ```text
//! 1. Initial         — first sample ever → STORE
//! 2. AlarmChange     — severity transition → STORE
//! 3. EpsilonExceeded — |value - last| > ε → STORE (scalars)
//! 4. ArrayChanged    — L2(array, last) > ε → STORE (arrays/matrices)
//! 5. ImageFrame      — always → STORE (NTNDArray)
//! 6. CustomChanged   — always → STORE (JSON-stored types)
//! 7. ArrayChanged    — always → STORE (other bulk: histogram, continuum…)
//! 8. Heartbeat       — elapsed > T → STORE
//! 9. Drop            — nothing changed
//! ```

use chrono::{DateTime, Duration, Utc};
use std::fmt;

use aura_core::pva::{
    AlarmSeverity, ArrayValue, NormativeType, PvDataType,
};
use aura_core::sample::{FilterDecision, StoreReason};
use aura_core::PvUpdate;

use super::calibrator::{Calibrator, CalibratorConfig};

/// Configuration for a PV filter.
#[derive(Debug, Clone)]
pub struct PvFilterConfig {
    /// Fixed epsilon. `None` = auto-calibrate.
    pub epsilon: Option<f64>,
    /// Heartbeat interval.
    pub heartbeat: Duration,
    /// Calibrator config (only used if epsilon is None).
    pub calibrator: CalibratorConfig,
}

impl Default for PvFilterConfig {
    fn default() -> Self {
        Self {
            epsilon: None,
            heartbeat: Duration::seconds(60),
            calibrator: CalibratorConfig::default(),
        }
    }
}

impl PvFilterConfig {
    /// Create a config with fixed epsilon and heartbeat.
    pub fn fixed(epsilon: f64, heartbeat_s: f64) -> Self {
        Self {
            epsilon: Some(epsilon),
            heartbeat: Duration::milliseconds((heartbeat_s * 1000.0) as i64),
            calibrator: CalibratorConfig::default(),
        }
    }

    /// Create a config with auto-calibrated epsilon.
    pub fn auto(heartbeat_s: f64, calibrator: CalibratorConfig) -> Self {
        Self {
            epsilon: None,
            heartbeat: Duration::milliseconds((heartbeat_s * 1000.0) as i64),
            calibrator,
        }
    }
}

/// Per-PV filter state machine.
///
/// One instance per PV, owned by [`super::FilterEngine`].
/// Tracks the last stored value/array, timestamp, and alarm severity.
pub struct PvFilter {
    pv_name: String,
    config: PvFilterConfig,

    // ── State ────────────────────────────────────────────────────────
    last_value: Option<f64>,
    last_array: Option<Vec<f64>>,
    last_time: Option<DateTime<Utc>>,
    last_severity: AlarmSeverity,

    // ── Calibration ──────────────────────────────────────────────────
    calibrator: Option<Calibrator>,

    // ── Counters ─────────────────────────────────────────────────────
    received: u64,
    stored: u64,
}

impl PvFilter {
    /// Create a new filter for a PV.
    pub fn new(pv_name: impl Into<String>, config: PvFilterConfig) -> Self {
        let calibrator = if config.epsilon.is_none() {
            Some(Calibrator::new(config.calibrator.clone()))
        } else {
            None
        };

        Self {
            pv_name: pv_name.into(),
            config,
            last_value: None,
            last_array: None,
            last_time: None,
            last_severity: AlarmSeverity::None,
            calibrator,
            received: 0,
            stored: 0,
        }
    }

    /// Process an incoming PV update and decide whether to store or drop.
    #[inline]
    pub fn decide(&mut self, update: &PvUpdate) -> FilterDecision {
        self.received += 1;

        let now = update.timestamp();
        let severity = update.data.alarm().severity;

        // Feed value to calibrator BEFORE decision — calibration
        // must use ALL samples, not just stored ones, for accurate σ.
        if let Some(ref mut cal) = self.calibrator {
            if let Some(v) = update.as_f64() {
                cal.push(v, now);
            }
        }

        // Compute data type ONCE, reuse for all checks.
        let data_type = PvDataType::from_nt(&update.data);
        let decision = self.decide_inner(update, now, severity, data_type);

        if decision.is_stored() {
            self.commit_store(update, now, severity);
        }

        decision
    }

    // ── Accessors ────────────────────────────────────────────────────

    /// The current effective epsilon.
    #[inline]
    pub fn effective_epsilon(&self) -> f64 {
        if let Some(e) = self.config.epsilon {
            e
        } else if let Some(ref cal) = self.calibrator {
            cal.effective_epsilon()
        } else {
            0.0
        }
    }

    /// Whether the calibrator has completed initial calibration.
    #[inline]
    pub fn is_calibrated(&self) -> bool {
        match &self.calibrator {
            Some(cal) => cal.is_calibrated(),
            None => true,
        }
    }

    #[inline]
    pub fn received(&self) -> u64 { self.received }

    #[inline]
    pub fn stored(&self) -> u64 { self.stored }

    #[inline]
    pub fn dropped(&self) -> u64 { self.received - self.stored }

    #[inline]
    pub fn compression_ratio(&self) -> f64 {
        if self.received == 0 { 1.0 } else { self.stored as f64 / self.received as f64 }
    }

    #[inline]
    pub fn pv_name(&self) -> &str { &self.pv_name }

    #[inline]
    pub fn last_value(&self) -> Option<f64> { self.last_value }

    #[inline]
    pub fn last_time(&self) -> Option<DateTime<Utc>> { self.last_time }

    #[inline]
    pub fn last_severity(&self) -> AlarmSeverity { self.last_severity }

    #[inline]
    pub fn calibrator(&self) -> Option<&Calibrator> { self.calibrator.as_ref() }

    // ── Runtime updates ──────────────────────────────────────────────

    /// Update epsilon at runtime (operator changes via API).
    pub fn update_epsilon(&mut self, epsilon: Option<f64>) {
        self.config.epsilon = epsilon;
        if epsilon.is_some() {
            self.calibrator = None;
        } else if self.calibrator.is_none() {
            self.calibrator = Some(Calibrator::new(self.config.calibrator.clone()));
        }
    }

    /// Update heartbeat interval at runtime.
    pub fn update_heartbeat(&mut self, heartbeat_s: f64) {
        self.config.heartbeat = Duration::milliseconds((heartbeat_s * 1000.0) as i64);
    }

    // ── Private: decision logic ──────────────────────────────────────

    fn decide_inner(
        &self,
        update: &PvUpdate,
        now: DateTime<Utc>,
        severity: AlarmSeverity,
        data_type: PvDataType,
    ) -> FilterDecision {
        // 1. Initial: first sample ever.
        if self.last_time.is_none() {
            return FilterDecision::Store(StoreReason::Initial);
        }

        // 2. Alarm change: severity transition.
        if severity != self.last_severity {
            return FilterDecision::Store(StoreReason::AlarmChange);
        }

        // 3. Heartbeat: force store if enough time has elapsed.
        //    Checked BEFORE value comparisons so that even unchanged
        //    arrays/scalars produce periodic liveness proofs.
        if let Some(last_time) = self.last_time {
            if now.signed_duration_since(last_time) >= self.config.heartbeat {
                return FilterDecision::Store(StoreReason::Heartbeat);
            }
        }

        // 4. Scalar epsilon check.
        if data_type.is_scalar_filterable() {
            if let (Some(current), Some(last)) = (update.as_f64(), self.last_value) {
                if (current - last).abs() > self.effective_epsilon() {
                    return FilterDecision::Store(StoreReason::EpsilonExceeded);
                }
            }
        }

        // 5. Array/Matrix: L2 distance check.
        if matches!(data_type, PvDataType::Array | PvDataType::Matrix) {
            return self.check_array_change(update);
        }

        // 6. Image: always store every frame.
        if data_type == PvDataType::Image {
            return FilterDecision::Store(StoreReason::ImageFrame);
        }

        // 7. JSON-stored types: always store.
        if data_type.is_json_stored() {
            return FilterDecision::Store(StoreReason::CustomChanged);
        }

        // 8. Other bulk data (histogram, continuum, namevalue).
        if data_type.is_bulk_data() {
            return FilterDecision::Store(StoreReason::ArrayChanged);
        }

        // 9. Nothing changed.
        FilterDecision::Drop
    }

    /// Compare current array/matrix to the last stored one via L2 distance.
    /// Returns a definitive Store or Drop — never falls through.
    fn check_array_change(&self, update: &PvUpdate) -> FilterDecision {
        match &update.data {
            NormativeType::NTScalarArray(a) => self.compare_array_l2(&a.value),
            NormativeType::NTMatrix(m) => self.compare_slice_l2(&m.value),
            // Should not reach here (guarded by data_type match), but be safe.
            _ => FilterDecision::Store(StoreReason::ArrayChanged),
        }
    }

    /// L2 comparison for ArrayValue against last_array.
    fn compare_array_l2(&self, current: &ArrayValue) -> FilterDecision {
        let last = match &self.last_array {
            Some(v) => v,
            None => return FilterDecision::Store(StoreReason::Initial),
        };

        // Fast path: DoubleArray → direct slice, no allocation.
        if let ArrayValue::DoubleArray(current_slice) = current {
            return self.l2_decide(current_slice, last);
        }

        // Slow path: convert to f64 Vec (other array types).
        match current.as_f64_vec() {
            Some(current_f64) => self.l2_decide(&current_f64, last),
            // StringArray or conversion failure → always store.
            None => FilterDecision::Store(StoreReason::ArrayChanged),
        }
    }

    /// L2 comparison for a raw f64 slice (NTMatrix.value) against last_array.
    #[inline]
    fn compare_slice_l2(&self, current: &[f64]) -> FilterDecision {
        match &self.last_array {
            Some(last) => self.l2_decide(current, last),
            None => FilterDecision::Store(StoreReason::Initial),
        }
    }

    /// Core L2 distance decision.
    ///
    /// Compares on ε² (avoids sqrt). Early exits once threshold exceeded
    /// to avoid iterating the full array on large waveforms.
    #[inline]
    fn l2_decide(&self, current: &[f64], last: &[f64]) -> FilterDecision {
        if current.len() != last.len() {
            return FilterDecision::Store(StoreReason::ArrayChanged);
        }

        let epsilon = self.effective_epsilon();
        let epsilon_sq = epsilon * epsilon;

        let mut dist_sq: f64 = 0.0;
        for (a, b) in current.iter().zip(last.iter()) {
            let d = a - b;
            dist_sq += d * d;
            // Early exit: once we exceed ε², no need to continue.
            if dist_sq > epsilon_sq {
                return FilterDecision::Store(StoreReason::ArrayChanged);
            }
        }

        FilterDecision::Drop
    }

    // ── Private: state updates ───────────────────────────────────────

    fn commit_store(&mut self, update: &PvUpdate, now: DateTime<Utc>, severity: AlarmSeverity) {
        self.stored += 1;
        self.last_time = Some(now);
        self.last_severity = severity;

        if let Some(v) = update.as_f64() {
            self.last_value = Some(v);
        }

        self.update_last_array(update);
    }

    fn update_last_array(&mut self, update: &PvUpdate) {
        match &update.data {
            NormativeType::NTScalarArray(a) => {
                self.last_array = match &a.value {
                    ArrayValue::DoubleArray(v) => Some(v.clone()),
                    other => other.as_f64_vec(),
                };
            }
            NormativeType::NTMatrix(m) => {
                self.last_array = Some(m.value.clone());
            }
            _ => {}
        }
    }
}

impl fmt::Debug for PvFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PvFilter")
            .field("pv", &self.pv_name)
            .field("epsilon", &self.effective_epsilon())
            .field("calibrated", &self.is_calibrated())
            .field("received", &self.received)
            .field("stored", &self.stored)
            .field("compression", &format!("{:.1}%", self.compression_ratio() * 100.0))
            .finish()
    }
}

impl fmt::Display for PvFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f, "{}: ε={:.6} recv={} stored={} ({:.1}%)",
            self.pv_name, self.effective_epsilon(),
            self.received, self.stored,
            self.compression_ratio() * 100.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aura_core::pva::*;

    fn ts(secs: i64) -> TimeStamp { TimeStamp::new(secs, 0) }

    fn scalar(value: f64, time_s: i64) -> PvUpdate {
        PvUpdate::new("TEST:PV", NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(value),
            alarm: Alarm::default(), timestamp: ts(time_s),
            display: None, control: None, value_alarm: None,
        }))
    }

    fn scalar_alarm(value: f64, time_s: i64, sev: AlarmSeverity) -> PvUpdate {
        PvUpdate::new("TEST:PV", NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(value),
            alarm: Alarm::new(sev, AlarmStatus::Device, ""),
            timestamp: ts(time_s),
            display: None, control: None, value_alarm: None,
        }))
    }

    fn array_update(values: Vec<f64>, time_s: i64) -> PvUpdate {
        PvUpdate::new("TEST:WAVE", NormativeType::NTScalarArray(NTScalarArray {
            value: ArrayValue::DoubleArray(values),
            alarm: Alarm::default(), timestamp: ts(time_s),
            display: None, control: None, value_alarm: None,
        }))
    }

    fn int_array_update(values: Vec<i32>, time_s: i64) -> PvUpdate {
        PvUpdate::new("TEST:WAVE", NormativeType::NTScalarArray(NTScalarArray {
            value: ArrayValue::IntArray(values),
            alarm: Alarm::default(), timestamp: ts(time_s),
            display: None, control: None, value_alarm: None,
        }))
    }

    fn matrix_update(values: Vec<f64>, dim: Vec<i32>, time_s: i64) -> PvUpdate {
        PvUpdate::new("TEST:MTX", NormativeType::NTMatrix(NTMatrix {
            value: values, dim,
            descriptor: String::new(), alarm: Alarm::default(),
            timestamp: ts(time_s), display: None,
        }))
    }

    fn fixed_filter(epsilon: f64, heartbeat_s: f64) -> PvFilter {
        PvFilter::new("TEST:PV", PvFilterConfig::fixed(epsilon, heartbeat_s))
    }

    // ── Construction ─────────────────────────────────────────────────

    #[test]
    fn test_new_fixed() {
        let f = fixed_filter(0.5, 60.0);
        assert_eq!(f.pv_name(), "TEST:PV");
        assert_eq!(f.effective_epsilon(), 0.5);
        assert!(f.is_calibrated());
        assert_eq!(f.received(), 0);
        assert_eq!(f.stored(), 0);
        assert_eq!(f.dropped(), 0);
        assert!(f.last_value().is_none());
        assert!(f.last_time().is_none());
        assert_eq!(f.last_severity(), AlarmSeverity::None);
        assert!(f.calibrator().is_none());
    }

    #[test]
    fn test_new_auto() {
        let config = PvFilterConfig::auto(60.0, CalibratorConfig {
            window_size: 4, sigma_k: 3.0,
            default_epsilon: 0.1, recalibrate_interval_s: 3600,
        });
        let f = PvFilter::new("AUTO:PV", config);
        assert_eq!(f.effective_epsilon(), 0.1);
        assert!(!f.is_calibrated());
        assert!(f.calibrator().is_some());
    }

    #[test]
    fn test_default_config() {
        let f = PvFilter::new("PV", PvFilterConfig::default());
        assert!(!f.is_calibrated());
        assert!(f.calibrator().is_some());
    }

    // ── Initial sample ───────────────────────────────────────────────

    #[test]
    fn test_first_sample_always_stored() {
        let mut f = fixed_filter(1.0, 60.0);
        let d = f.decide(&scalar(10.0, 1000));
        assert_eq!(d, FilterDecision::Store(StoreReason::Initial));
        assert_eq!(f.received(), 1);
        assert_eq!(f.stored(), 1);
        assert_eq!(f.last_value(), Some(10.0));
        assert!(f.last_time().is_some());
    }

    // ── Epsilon exceeded ─────────────────────────────────────────────

    #[test]
    fn test_epsilon_exceeded() {
        let mut f = fixed_filter(0.5, 60.0);
        f.decide(&scalar(10.0, 1000));
        let d = f.decide(&scalar(10.6, 1001));
        assert_eq!(d, FilterDecision::Store(StoreReason::EpsilonExceeded));
        assert_eq!(f.last_value(), Some(10.6));
    }

    #[test]
    fn test_epsilon_not_exceeded() {
        let mut f = fixed_filter(0.5, 60.0);
        f.decide(&scalar(10.0, 1000));
        let d = f.decide(&scalar(10.3, 1001));
        assert_eq!(d, FilterDecision::Drop);
        assert_eq!(f.last_value(), Some(10.0));
    }

    #[test]
    fn test_epsilon_exact_boundary_drops() {
        let mut f = fixed_filter(0.5, 60.0);
        f.decide(&scalar(10.0, 1000));
        assert_eq!(f.decide(&scalar(10.5, 1001)), FilterDecision::Drop);
    }

    #[test]
    fn test_epsilon_negative_change() {
        let mut f = fixed_filter(0.5, 60.0);
        f.decide(&scalar(10.0, 1000));
        assert_eq!(f.decide(&scalar(9.4, 1001)),
                   FilterDecision::Store(StoreReason::EpsilonExceeded));
    }

    #[test]
    fn test_epsilon_zero_stores_any_change() {
        let mut f = fixed_filter(0.0, 60.0);
        f.decide(&scalar(10.0, 1000));
        // Identical value → drop
        assert_eq!(f.decide(&scalar(10.0, 1001)), FilterDecision::Drop);
        // Any real change > 0 triggers store when ε=0
        // Use 0.001 — large enough to survive f64 arithmetic
        assert_eq!(f.decide(&scalar(10.001, 1002)),
                   FilterDecision::Store(StoreReason::EpsilonExceeded));
    }

    #[test]
    fn test_epsilon_chain_updates_last_value() {
        let mut f = fixed_filter(1.0, 3600.0);
        f.decide(&scalar(10.0, 1000));   // stored, last=10
        f.decide(&scalar(10.5, 1001));   // dropped
        f.decide(&scalar(11.0, 1002));   // dropped (|11-10|=1, not >1)
        assert_eq!(f.decide(&scalar(11.5, 1003)),
                   FilterDecision::Store(StoreReason::EpsilonExceeded));
        assert_eq!(f.last_value(), Some(11.5));
        assert_eq!(f.decide(&scalar(12.0, 1004)), FilterDecision::Drop);
    }

    // ── Heartbeat ────────────────────────────────────────────────────

    #[test]
    fn test_heartbeat_triggers() {
        let mut f = fixed_filter(1.0, 10.0);
        f.decide(&scalar(10.0, 1000));
        assert_eq!(f.decide(&scalar(10.0, 1005)), FilterDecision::Drop);
        assert_eq!(f.decide(&scalar(10.0, 1011)),
                   FilterDecision::Store(StoreReason::Heartbeat));
    }

    #[test]
    fn test_heartbeat_resets_after_store() {
        let mut f = fixed_filter(1.0, 10.0);
        f.decide(&scalar(10.0, 1000));
        f.decide(&scalar(10.0, 1011)); // heartbeat
        assert_eq!(f.decide(&scalar(10.0, 1015)), FilterDecision::Drop);
        assert_eq!(f.decide(&scalar(10.0, 1022)),
                   FilterDecision::Store(StoreReason::Heartbeat));
    }

    #[test]
    fn test_heartbeat_exact_boundary() {
        let mut f = fixed_filter(1.0, 10.0);
        f.decide(&scalar(10.0, 1000));
        assert_eq!(f.decide(&scalar(10.0, 1010)),
                   FilterDecision::Store(StoreReason::Heartbeat));
    }

    // ── Alarm change ─────────────────────────────────────────────────

    #[test]
    fn test_alarm_raised() {
        let mut f = fixed_filter(1.0, 60.0);
        f.decide(&scalar(10.0, 1000));
        let d = f.decide(&scalar_alarm(10.0, 1001, AlarmSeverity::Major));
        assert_eq!(d, FilterDecision::Store(StoreReason::AlarmChange));
        assert_eq!(f.last_severity(), AlarmSeverity::Major);
    }

    #[test]
    fn test_alarm_cleared() {
        let mut f = fixed_filter(1.0, 60.0);
        f.decide(&scalar_alarm(10.0, 1000, AlarmSeverity::Minor));
        let d = f.decide(&scalar(10.0, 1001));
        assert_eq!(d, FilterDecision::Store(StoreReason::AlarmChange));
        assert_eq!(f.last_severity(), AlarmSeverity::None);
    }

    #[test]
    fn test_alarm_severity_change() {
        let mut f = fixed_filter(1.0, 60.0);
        f.decide(&scalar_alarm(10.0, 1000, AlarmSeverity::Minor));
        let d = f.decide(&scalar_alarm(10.0, 1001, AlarmSeverity::Major));
        assert_eq!(d, FilterDecision::Store(StoreReason::AlarmChange));
    }

    #[test]
    fn test_same_alarm_no_store() {
        let mut f = fixed_filter(1.0, 60.0);
        f.decide(&scalar_alarm(10.0, 1000, AlarmSeverity::Minor));
        assert_eq!(f.decide(&scalar_alarm(10.0, 1001, AlarmSeverity::Minor)),
                   FilterDecision::Drop);
    }

    // ── Priority ─────────────────────────────────────────────────────

    // ── Priority ─────────────────────────────────────────────────────

    #[test]
    fn test_alarm_priority_over_heartbeat() {
        let mut f = fixed_filter(1.0, 10.0);
        f.decide(&scalar(10.0, 1000));
        // Both alarm change AND heartbeat would trigger → alarm wins
        let d = f.decide(&scalar_alarm(10.0, 1011, AlarmSeverity::Major));
        assert_eq!(d, FilterDecision::Store(StoreReason::AlarmChange));
    }

    #[test]
    fn test_alarm_priority_over_epsilon() {
        let mut f = fixed_filter(1.0, 60.0);
        f.decide(&scalar(10.0, 1000));
        // Value change < ε BUT alarm changed → alarm wins
        let d = f.decide(&scalar_alarm(10.1, 1001, AlarmSeverity::Major));
        assert_eq!(d, FilterDecision::Store(StoreReason::AlarmChange));
    }

    #[test]
    fn test_heartbeat_priority_over_epsilon() {
        let mut f = fixed_filter(0.5, 10.0);
        f.decide(&scalar(10.0, 1000));
        // Both heartbeat AND epsilon would trigger → heartbeat wins (checked first)
        let d = f.decide(&scalar(20.0, 1011));
        assert_eq!(d, FilterDecision::Store(StoreReason::Heartbeat));
    }

    // ── Array (waveform) filtering ───────────────────────────────────

    #[test]
    fn test_array_first_sample() {
        let mut f = PvFilter::new("W", PvFilterConfig::fixed(1.0, 60.0));
        assert_eq!(f.decide(&array_update(vec![1.0, 2.0, 3.0], 1000)),
                   FilterDecision::Store(StoreReason::Initial));
    }

    #[test]
    fn test_array_no_change() {
        let mut f = PvFilter::new("W", PvFilterConfig::fixed(1.0, 60.0));
        f.decide(&array_update(vec![1.0, 2.0, 3.0], 1000));
        assert_eq!(f.decide(&array_update(vec![1.0, 2.0, 3.0], 1001)),
                   FilterDecision::Drop);
    }

    #[test]
    fn test_array_change_exceeds_epsilon() {
        let mut f = PvFilter::new("W", PvFilterConfig::fixed(0.1, 60.0));
        f.decide(&array_update(vec![1.0, 2.0, 3.0], 1000));
        assert_eq!(f.decide(&array_update(vec![2.0, 2.0, 3.0], 1001)),
                   FilterDecision::Store(StoreReason::ArrayChanged));
    }

    #[test]
    fn test_array_change_below_epsilon() {
        let mut f = PvFilter::new("W", PvFilterConfig::fixed(10.0, 60.0));
        f.decide(&array_update(vec![1.0, 2.0, 3.0], 1000));
        assert_eq!(f.decide(&array_update(vec![1.01, 2.01, 3.01], 1001)),
                   FilterDecision::Drop);
    }

    #[test]
    fn test_array_size_change() {
        let mut f = PvFilter::new("W", PvFilterConfig::fixed(1.0, 60.0));
        f.decide(&array_update(vec![1.0, 2.0], 1000));
        assert_eq!(f.decide(&array_update(vec![1.0, 2.0, 3.0], 1001)),
                   FilterDecision::Store(StoreReason::ArrayChanged));
    }

    #[test]
    fn test_array_l2_early_exit() {
        let mut f = PvFilter::new("W", PvFilterConfig::fixed(0.01, 60.0));
        let base: Vec<f64> = (0..10_000).map(|i| i as f64).collect();
        f.decide(&array_update(base.clone(), 1000));

        let mut changed = base;
        changed[0] = 999_999.0;
        assert_eq!(f.decide(&array_update(changed, 1001)),
                   FilterDecision::Store(StoreReason::ArrayChanged));
    }

    #[test]
    fn test_int_array_conversion() {
        let mut f = PvFilter::new("W", PvFilterConfig::fixed(0.5, 60.0));
        f.decide(&int_array_update(vec![1, 2, 3], 1000));
        assert_eq!(f.decide(&int_array_update(vec![1, 2, 3], 1001)),
                   FilterDecision::Drop);
        assert_eq!(f.decide(&int_array_update(vec![100, 2, 3], 1002)),
                   FilterDecision::Store(StoreReason::ArrayChanged));
    }

    // ── Matrix filtering ─────────────────────────────────────────────

    #[test]
    fn test_matrix_first_sample() {
        let mut f = PvFilter::new("M", PvFilterConfig::fixed(1.0, 60.0));
        assert_eq!(f.decide(&matrix_update(vec![1.0, 2.0, 3.0, 4.0], vec![2, 2], 1000)),
                   FilterDecision::Store(StoreReason::Initial));
    }

    #[test]
    fn test_matrix_no_change() {
        let mut f = PvFilter::new("M", PvFilterConfig::fixed(1.0, 60.0));
        f.decide(&matrix_update(vec![1.0, 2.0, 3.0, 4.0], vec![2, 2], 1000));
        assert_eq!(f.decide(&matrix_update(vec![1.0, 2.0, 3.0, 4.0], vec![2, 2], 1001)),
                   FilterDecision::Drop);
    }

    #[test]
    fn test_matrix_changed() {
        let mut f = PvFilter::new("M", PvFilterConfig::fixed(0.1, 60.0));
        f.decide(&matrix_update(vec![1.0, 0.0, 0.0, 1.0], vec![2, 2], 1000));
        assert_eq!(f.decide(&matrix_update(vec![1.0, 0.0, 0.0, 99.0], vec![2, 2], 1001)),
                   FilterDecision::Store(StoreReason::ArrayChanged));
    }

    // ── Image always stored ──────────────────────────────────────────

    #[test]
    fn test_image_always_stored() {
        let mut f = PvFilter::new("CAM", PvFilterConfig::fixed(1.0, 60.0));
        let mk_img = |uid, t| PvUpdate::new("CAM", NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![0; 100]),
            codec: Codec::default(),
            compressed_size: 0, uncompressed_size: 100,
            dimension: vec![Dimension::new(10)],
            unique_id: uid, data_timestamp: None,
            alarm: Alarm::default(), timestamp: ts(t), attribute: vec![],
        }));

        assert_eq!(f.decide(&mk_img(1, 1000)),
                   FilterDecision::Store(StoreReason::Initial));
        assert_eq!(f.decide(&mk_img(2, 1001)),
                   FilterDecision::Store(StoreReason::ImageFrame));
        assert_eq!(f.decide(&mk_img(3, 1002)),
                   FilterDecision::Store(StoreReason::ImageFrame));
    }

    // ── Enum as scalar ───────────────────────────────────────────────

    #[test]
    fn test_enum_filter() {
        let mut f = fixed_filter(0.5, 60.0);
        let mk_enum = |idx, t| PvUpdate::new("TEST:PV", NormativeType::NTEnum(NTEnum {
            value: EnumValue::from_strs(idx, &["Off", "On"]),
            alarm: Alarm::default(), timestamp: ts(t),
        }));

        f.decide(&mk_enum(0, 1000));
        assert_eq!(f.decide(&mk_enum(0, 1001)), FilterDecision::Drop);
        assert_eq!(f.decide(&mk_enum(1, 1002)),
                   FilterDecision::Store(StoreReason::EpsilonExceeded));
    }

    // ── JSON-stored types ────────────────────────────────────────────

    #[test]
    fn test_table_always_stored() {
        let mut f = PvFilter::new("TBL", PvFilterConfig::fixed(1.0, 60.0));
        let mk_tbl = |t| PvUpdate::new("TBL", NormativeType::NTTable(NTTable {
            labels: vec!["x".into()],
            columns: vec![TableColumn::new("x", ArrayValue::DoubleArray(vec![1.0]))],
            alarm: Alarm::default(), timestamp: ts(t),
        }));

        assert_eq!(f.decide(&mk_tbl(1000)),
                   FilterDecision::Store(StoreReason::Initial));
        assert_eq!(f.decide(&mk_tbl(1001)),
                   FilterDecision::Store(StoreReason::CustomChanged));
    }

    // ── Bulk data types ──────────────────────────────────────────────

    #[test]
    fn test_histogram_always_stored() {
        let mut f = PvFilter::new("H", PvFilterConfig::fixed(1.0, 60.0));
        let mk_hist = |t| PvUpdate::new("H", NormativeType::NTHistogram(NTHistogram {
            ranges: vec![0.0, 1.0], value: HistogramValue::Int(vec![10]),
            descriptor: String::new(), alarm: Alarm::default(), timestamp: ts(t),
        }));

        f.decide(&mk_hist(1000));
        assert_eq!(f.decide(&mk_hist(1001)),
                   FilterDecision::Store(StoreReason::ArrayChanged));
    }

    #[test]
    fn test_continuum_always_stored() {
        let mut f = PvFilter::new("C", PvFilterConfig::fixed(1.0, 60.0));
        let mk = |t| PvUpdate::new("C", NormativeType::NTContinuum(NTContinuum {
            base: vec![0.0, 1.0], value: vec![1.0, 2.0],
            units: vec!["s".into(), "V".into()],
            descriptor: String::new(), alarm: Alarm::default(), timestamp: ts(t),
        }));

        f.decide(&mk(1000));
        assert_eq!(f.decide(&mk(1001)),
                   FilterDecision::Store(StoreReason::ArrayChanged));
    }

    // ── Counters ─────────────────────────────────────────────────────

    #[test]
    fn test_counters() {
        let mut f = fixed_filter(1.0, 60.0);
        f.decide(&scalar(10.0, 1000));
        f.decide(&scalar(10.0, 1001));
        f.decide(&scalar(10.0, 1002));
        f.decide(&scalar(20.0, 1003));

        assert_eq!(f.received(), 4);
        assert_eq!(f.stored(), 2);
        assert_eq!(f.dropped(), 2);
        assert!((f.compression_ratio() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_compression_ratio_empty() {
        assert_eq!(fixed_filter(1.0, 60.0).compression_ratio(), 1.0);
    }

    // ── Runtime updates ──────────────────────────────────────────────

    #[test]
    fn test_update_epsilon_fixed() {
        let mut f = fixed_filter(1.0, 60.0);
        f.update_epsilon(Some(0.01));
        assert_eq!(f.effective_epsilon(), 0.01);
        assert!(f.is_calibrated());
    }

    #[test]
    fn test_update_epsilon_to_auto() {
        let mut f = fixed_filter(1.0, 60.0);
        f.update_epsilon(None);
        assert!(!f.is_calibrated());
        assert!(f.calibrator().is_some());
    }

    #[test]
    fn test_update_epsilon_from_auto_to_fixed() {
        let mut f = PvFilter::new("PV", PvFilterConfig::default());
        f.update_epsilon(Some(0.42));
        assert!(f.is_calibrated());
        assert!(f.calibrator().is_none());
        assert_eq!(f.effective_epsilon(), 0.42);
    }

    #[test]
    fn test_update_heartbeat() {
        let mut f = fixed_filter(1.0, 60.0);
        f.decide(&scalar(10.0, 1000));
        f.update_heartbeat(5.0);
        assert_eq!(f.decide(&scalar(10.0, 1006)),
                   FilterDecision::Store(StoreReason::Heartbeat));
    }

    // ── Display / Debug ──────────────────────────────────────────────

    #[test]
    fn test_display() {
        let mut f = fixed_filter(0.01, 60.0);
        f.decide(&scalar(10.0, 1000));
        let s = f.to_string();
        assert!(s.contains("TEST:PV"));
        assert!(s.contains("ε=0.010000"));
        assert!(s.contains("recv=1"));
        assert!(s.contains("stored=1"));
    }

    #[test]
    fn test_debug() {
        let f = fixed_filter(0.01, 60.0);
        let d = format!("{:?}", f);
        assert!(d.contains("PvFilter"));
        assert!(d.contains("epsilon"));
        assert!(d.contains("calibrated"));
    }
}