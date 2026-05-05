//! Auto-epsilon calibration from signal noise.
//!
//! The calibrator estimates the noise floor of a PV's signal using
//! a sliding window of recent samples. Once the window is full, it
//! computes `ε = k × σ` where `σ` is the sample standard deviation
//! and `k` is a configurable sigma multiplier (typically 3.0).
//!
//! At 3σ, ~99.7% of noise fluctuations fall within ε, and only
//! genuine signal changes trigger a store.
//!
//! ## Recalibration
//!
//! The calibrator recalibrates periodically (configurable interval)
//! to adapt to changing noise conditions (e.g., a sensor degrading
//! over months). Set `recalibrate_interval_s = 0` to calibrate once.
//!
//! ## Constant signals
//!
//! If σ ≈ 0 (constant signal), the calibrator falls back to
//! `default_epsilon` to avoid setting ε = 0 (which would store
//! every single sample).
//!
//! ## Performance
//!
//! - O(1) per sample (Welford push/pop)
//! - VecDeque pre-allocated to window_size (no runtime allocations)
//! - Calibration check is a single integer comparison on the hot path

use std::collections::VecDeque;
use std::fmt;

use chrono::{DateTime, Utc};

use super::stats::WelfordStats;

/// Minimum window size (need at least 4 samples for meaningful σ).
const MIN_WINDOW_SIZE: usize = 4;

/// Configuration for the auto-calibrator.
#[derive(Debug, Clone, PartialEq)]
pub struct CalibratorConfig {
    /// Window size for noise estimation (number of samples).
    /// Clamped to minimum of 4.
    pub window_size: usize,
    /// Sigma multiplier: ε = k × σ.
    pub sigma_k: f64,
    /// Fallback epsilon if σ ≈ 0 (constant signal) or pre-calibration.
    pub default_epsilon: f64,
    /// Recalibration interval in seconds. 0 = calibrate once only.
    pub recalibrate_interval_s: u64,
}

impl Default for CalibratorConfig {
    fn default() -> Self {
        Self {
            window_size: 128,
            sigma_k: 3.0,
            default_epsilon: 0.0,
            recalibrate_interval_s: 3600,
        }
    }
}

impl CalibratorConfig {
    /// Create a config for a one-shot calibration (no recalibration).
    pub fn one_shot(window_size: usize, sigma_k: f64) -> Self {
        Self {
            window_size,
            sigma_k,
            default_epsilon: 0.0,
            recalibrate_interval_s: 0,
        }
    }

    /// The effective window size (after clamping).
    #[inline]
    pub fn effective_window_size(&self) -> usize {
        self.window_size.max(MIN_WINDOW_SIZE)
    }
}

/// Auto-epsilon calibrator for a single PV.
///
/// Maintains a sliding window of recent values and computes
/// the noise-based epsilon threshold: `ε = k × σ`.
pub struct Calibrator {
    config: CalibratorConfig,
    /// Effective window size (clamped from config).
    window_size: usize,
    /// Sliding window of recent values (pre-allocated, no runtime alloc).
    window: VecDeque<f64>,
    /// Running statistics over the window (O(1) push/pop).
    stats: WelfordStats,
    /// Current calibrated epsilon. `None` = not yet calibrated.
    epsilon: Option<f64>,
    /// Timestamp of last calibration.
    last_calibrated: Option<DateTime<Utc>>,
    /// Number of calibrations performed.
    calibration_count: u64,
}

impl Calibrator {
    /// Create a new calibrator with the given configuration.
    pub fn new(config: CalibratorConfig) -> Self {
        let window_size = config.effective_window_size();
        Self {
            config,
            window_size,
            window: VecDeque::with_capacity(window_size),
            stats: WelfordStats::new(),
            epsilon: None,
            last_calibrated: None,
            calibration_count: 0,
        }
    }

    /// Create a calibrator with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(CalibratorConfig::default())
    }

    /// Feed a new sample value into the calibrator.
    ///
    /// **Hot path** — called for every scalar sample.
    /// O(1) time, zero allocations (VecDeque pre-sized).
    ///
    /// Returns the current effective epsilon after processing.
    #[inline]
    pub fn push(&mut self, value: f64, now: DateTime<Utc>) -> f64 {
        // Evict oldest if window is full.
        if self.window.len() >= self.window_size {
            // Safety: len >= window_size >= MIN_WINDOW_SIZE >= 4,
            // so pop_front always returns Some.
            let old = self.window.pop_front().unwrap();
            self.stats.pop(old);
        }

        self.window.push_back(value);
        self.stats.push(value);

        // Calibrate if window is full and it's time.
        if self.window.len() >= self.window_size && self.should_recalibrate(now) {
            self.calibrate(now);
        }

        self.effective_epsilon()
    }

    /// The current effective epsilon.
    /// Returns calibrated value if available, else `default_epsilon`.
    #[inline]
    pub fn effective_epsilon(&self) -> f64 {
        self.epsilon.unwrap_or(self.config.default_epsilon)
    }

    /// Whether calibration has been performed at least once.
    #[inline]
    pub fn is_calibrated(&self) -> bool {
        self.epsilon.is_some()
    }

    /// The current noise estimate (σ of the window).
    #[inline]
    pub fn noise_sigma(&self) -> f64 {
        self.stats.stddev()
    }

    /// The window mean (current signal baseline).
    #[inline]
    pub fn window_mean(&self) -> f64 {
        self.stats.mean()
    }

    /// Number of samples currently in the window.
    #[inline]
    pub fn window_len(&self) -> usize {
        self.window.len()
    }

    /// The configured window size (after clamping).
    #[inline]
    pub fn window_capacity(&self) -> usize {
        self.window_size
    }

    /// Whether the window is full (ready for calibration).
    #[inline]
    pub fn is_window_full(&self) -> bool {
        self.window.len() >= self.window_size
    }

    /// Number of calibrations performed.
    #[inline]
    pub fn calibration_count(&self) -> u64 {
        self.calibration_count
    }

    /// Timestamp of last calibration.
    #[inline]
    pub fn last_calibrated(&self) -> Option<DateTime<Utc>> {
        self.last_calibrated
    }

    /// Access the underlying configuration.
    #[inline]
    pub fn config(&self) -> &CalibratorConfig {
        &self.config
    }

    /// Force a recalibration now (even if interval hasn't elapsed).
    /// Requires at least 2 samples in the window.
    pub fn force_calibrate(&mut self, now: DateTime<Utc>) {
        if self.stats.count() >= 2 {
            self.calibrate(now);
        }
    }

    /// Reset to initial state (clears window, epsilon, counters).
    pub fn reset(&mut self) {
        self.window.clear();
        self.stats.reset();
        self.epsilon = None;
        self.last_calibrated = None;
        self.calibration_count = 0;
    }

    fn calibrate(&mut self, now: DateTime<Utc>) {
        let sigma = self.stats.stddev();

        // If σ ≈ 0 (constant signal), fall back to default_epsilon.
        // Setting ε = 0 would store every sample (wasteful).
        let epsilon = if sigma < f64::EPSILON {
            self.config.default_epsilon
        } else {
            self.config.sigma_k * sigma
        };

        self.epsilon = Some(epsilon);
        self.last_calibrated = Some(now);
        self.calibration_count += 1;
    }

    /// Check whether it's time to recalibrate.
    /// - Never calibrated → yes
    /// - recalibrate_interval_s == 0 → no (one-shot mode)
    /// - Elapsed >= interval → yes
    #[inline]
    fn should_recalibrate(&self, now: DateTime<Utc>) -> bool {
        match self.last_calibrated {
            None => true,
            Some(last) => {
                self.config.recalibrate_interval_s > 0
                    && now.signed_duration_since(last).num_seconds()
                    >= self.config.recalibrate_interval_s as i64
            }
        }
    }
}

impl fmt::Debug for Calibrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Calibrator")
            .field("window", &format!("{}/{}", self.window.len(), self.window_size))
            .field("epsilon", &self.effective_epsilon())
            .field("sigma", &self.noise_sigma())
            .field("calibrated", &self.is_calibrated())
            .field("calibrations", &self.calibration_count)
            .finish()
    }
}

impl fmt::Display for Calibrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_calibrated() {
            write!(
                f, "ε={:.6} (σ={:.6}, k={}, n={})",
                self.effective_epsilon(), self.noise_sigma(),
                self.config.sigma_k, self.calibration_count
            )
        } else {
            write!(
                f, "ε={:.6} (uncalibrated, {}/{} samples)",
                self.effective_epsilon(), self.window.len(), self.window_size
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::cell::Cell;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn small_config() -> CalibratorConfig {
        CalibratorConfig {
            window_size: 4,
            sigma_k: 3.0,
            default_epsilon: 0.1,
            recalibrate_interval_s: 3600,
        }
    }

    /// Deterministic pseudo-noise — thread-local, no unsafe.
    fn deterministic_noise(scale: f64) -> f64 {
        thread_local! {
            static SEED: Cell<u64> = const { Cell::new(42) };
        }
        SEED.with(|s| {
            let v = s.get().wrapping_mul(6364136223846793005).wrapping_add(1);
            s.set(v);
            let frac = (v >> 33) as f64 / (1u64 << 31) as f64;
            (frac - 0.5) * 2.0 * scale
        })
    }

    #[test]
    fn test_config_default() {
        let cfg = CalibratorConfig::default();
        assert_eq!(cfg.window_size, 128);
        assert_eq!(cfg.sigma_k, 3.0);
        assert_eq!(cfg.default_epsilon, 0.0);
        assert_eq!(cfg.recalibrate_interval_s, 3600);
    }

    #[test]
    fn test_config_one_shot() {
        let cfg = CalibratorConfig::one_shot(64, 2.5);
        assert_eq!(cfg.window_size, 64);
        assert_eq!(cfg.sigma_k, 2.5);
        assert_eq!(cfg.recalibrate_interval_s, 0);
    }

    #[test]
    fn test_config_effective_window_size() {
        assert_eq!(CalibratorConfig::default().effective_window_size(), 128);
        let tiny = CalibratorConfig { window_size: 1, ..CalibratorConfig::default() };
        assert_eq!(tiny.effective_window_size(), MIN_WINDOW_SIZE);
    }

    #[test]
    fn test_config_eq() {
        let a = CalibratorConfig::default();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_new() {
        let c = Calibrator::new(small_config());
        assert!(!c.is_calibrated());
        assert_eq!(c.effective_epsilon(), 0.1);
        assert_eq!(c.window_len(), 0);
        assert_eq!(c.window_capacity(), 4);
        assert!(!c.is_window_full());
        assert_eq!(c.calibration_count(), 0);
        assert!(c.last_calibrated().is_none());
        assert_eq!(c.noise_sigma(), 0.0);
        assert_eq!(c.window_mean(), 0.0);
    }

    #[test]
    fn test_with_defaults() {
        let c = Calibrator::with_defaults();
        assert_eq!(c.config().window_size, 128);
        assert_eq!(c.config().sigma_k, 3.0);
        assert_eq!(c.window_capacity(), 128);
    }

    #[test]
    fn test_min_window_size_clamped() {
        let cfg = CalibratorConfig { window_size: 1, ..CalibratorConfig::default() };
        let c = Calibrator::new(cfg);
        assert_eq!(c.window_capacity(), MIN_WINDOW_SIZE);
    }

    #[test]
    fn test_min_window_size_zero() {
        let cfg = CalibratorConfig { window_size: 0, ..CalibratorConfig::default() };
        let c = Calibrator::new(cfg);
        assert_eq!(c.window_capacity(), MIN_WINDOW_SIZE);
    }

    #[test]
    fn test_before_window_full() {
        let mut c = Calibrator::new(small_config());

        c.push(10.0, t(1000));
        c.push(10.1, t(1000));
        c.push(9.9, t(1000));

        assert_eq!(c.window_len(), 3);
        assert!(!c.is_window_full());
        assert!(!c.is_calibrated());
        assert_eq!(c.effective_epsilon(), 0.1);
    }

    #[test]
    fn test_push_returns_epsilon() {
        let mut c = Calibrator::new(small_config());
        let eps = c.push(10.0, t(1000));
        assert_eq!(eps, 0.1); // default before calibration
    }

    #[test]
    fn test_calibrates_when_window_full() {
        let mut c = Calibrator::new(small_config());

        c.push(10.0, t(1000));
        c.push(10.1, t(1000));
        c.push(9.9, t(1000));
        let eps = c.push(10.05, t(1000));

        assert!(c.is_calibrated());
        assert!(c.is_window_full());
        assert_eq!(c.calibration_count(), 1);
        assert!(eps > 0.0);
        assert!(eps < 1.0);
        assert_eq!(eps, c.effective_epsilon());
        assert_eq!(c.last_calibrated(), Some(t(1000)));
    }

    #[test]
    fn test_epsilon_is_k_times_sigma() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 2.0,
            default_epsilon: 0.0, recalibrate_interval_s: 3600,
        };
        let mut c = Calibrator::new(cfg);

        c.push(0.0, t(1000));
        c.push(1.0, t(1000));
        c.push(0.0, t(1000));
        c.push(1.0, t(1000));

        let sigma = c.noise_sigma();
        assert!((c.effective_epsilon() - 2.0 * sigma).abs() < 1e-10);
    }

    #[test]
    fn test_sigma_k_one() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 1.0,
            default_epsilon: 0.0, recalibrate_interval_s: 3600,
        };
        let mut c = Calibrator::new(cfg);
        c.push(0.0, t(1000));
        c.push(10.0, t(1000));
        c.push(0.0, t(1000));
        c.push(10.0, t(1000));

        let sigma = c.noise_sigma();
        assert!((c.effective_epsilon() - sigma).abs() < 1e-10);
    }

    #[test]
    fn test_constant_signal_uses_default() {
        let mut c = Calibrator::new(small_config());
        for _ in 0..4 {
            c.push(4.217, t(1000));
        }
        assert!(c.is_calibrated());
        assert_eq!(c.effective_epsilon(), 0.1); // falls back to default
    }

    #[test]
    fn test_constant_signal_zero_default() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 3.0,
            default_epsilon: 0.0, recalibrate_interval_s: 3600,
        };
        let mut c = Calibrator::new(cfg);
        for _ in 0..4 {
            c.push(42.0, t(1000));
        }
        assert!(c.is_calibrated());
        assert_eq!(c.effective_epsilon(), 0.0);
    }

    #[test]
    fn test_window_eviction() {
        let mut c = Calibrator::new(small_config());

        c.push(1.0, t(1000));
        c.push(2.0, t(1000));
        c.push(3.0, t(1000));
        c.push(4.0, t(1000));
        assert_eq!(c.window_len(), 4);

        c.push(5.0, t(1000));
        assert_eq!(c.window_len(), 4);
        assert!((c.window_mean() - 3.5).abs() < 0.01);
    }

    #[test]
    fn test_window_never_exceeds_capacity() {
        let mut c = Calibrator::new(small_config());
        for i in 0..100 {
            c.push(i as f64, t(1000));
            assert!(c.window_len() <= c.window_capacity());
        }
    }

    #[test]
    fn test_no_recalibration_before_interval() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 3.0,
            default_epsilon: 0.1, recalibrate_interval_s: 3600,
        };
        let mut c = Calibrator::new(cfg);

        for _ in 0..4 {
            c.push(10.0 + deterministic_noise(0.01), t(1000));
        }
        let eps1 = c.effective_epsilon();
        assert_eq!(c.calibration_count(), 1);

        // 500s < 3600s → no recalibration
        for _ in 0..4 {
            c.push(10.0 + deterministic_noise(0.05), t(1500));
        }
        assert_eq!(c.calibration_count(), 1);
        assert_eq!(c.effective_epsilon(), eps1);
    }

    #[test]
    fn test_recalibration_after_interval() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 3.0,
            default_epsilon: 0.1, recalibrate_interval_s: 100,
        };
        let mut c = Calibrator::new(cfg);

        for _ in 0..4 {
            c.push(10.0, t(1000));
        }
        assert_eq!(c.calibration_count(), 1);

        // 200s > 100s → recalibrate
        for _ in 0..4 {
            c.push(20.0, t(1200));
        }
        assert_eq!(c.calibration_count(), 2);
    }

    #[test]
    fn test_recalibration_exact_boundary() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 3.0,
            default_epsilon: 0.0, recalibrate_interval_s: 100,
        };
        let mut c = Calibrator::new(cfg);

        for _ in 0..4 {
            c.push(10.0, t(1000));
        }
        assert_eq!(c.calibration_count(), 1);

        // Exactly 100s → should recalibrate (>=)
        for _ in 0..4 {
            c.push(10.0, t(1100));
        }
        assert_eq!(c.calibration_count(), 2);
    }

    #[test]
    fn test_calibrate_once_mode() {
        let cfg = CalibratorConfig::one_shot(4, 3.0);
        let mut c = Calibrator::new(cfg);

        for _ in 0..4 {
            c.push(10.0, t(1000));
        }
        assert_eq!(c.calibration_count(), 1);

        // No recalibration even after a very long time
        for _ in 0..100 {
            c.push(99.0, t(999_999));
        }
        assert_eq!(c.calibration_count(), 1);
    }

    #[test]
    fn test_force_calibrate() {
        let mut c = Calibrator::new(small_config());
        c.push(1.0, t(1000));
        c.push(2.0, t(1000));
        c.force_calibrate(t(1000));

        assert!(c.is_calibrated());
        assert_eq!(c.calibration_count(), 1);
        assert!(c.effective_epsilon() > 0.0);
    }

    #[test]
    fn test_force_calibrate_empty() {
        let mut c = Calibrator::new(small_config());
        c.force_calibrate(t(1000));
        assert!(!c.is_calibrated());
        assert_eq!(c.calibration_count(), 0);
    }

    #[test]
    fn test_force_calibrate_one_sample() {
        let mut c = Calibrator::new(small_config());
        c.push(42.0, t(1000));
        c.force_calibrate(t(1000));
        assert!(!c.is_calibrated());
    }

    #[test]
    fn test_force_calibrate_overrides_interval() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 3.0,
            default_epsilon: 0.0, recalibrate_interval_s: 999_999,
        };
        let mut c = Calibrator::new(cfg);
        for _ in 0..4 {
            c.push(10.0, t(1000));
        }
        assert_eq!(c.calibration_count(), 1);

        // Normally wouldn't recalibrate (interval very long)
        // but force_calibrate ignores the interval
        c.push(20.0, t(1001));
        c.force_calibrate(t(1001));
        assert_eq!(c.calibration_count(), 2);
    }

    #[test]
    fn test_reset() {
        let mut c = Calibrator::new(small_config());
        for _ in 0..4 {
            c.push(10.0, t(1000));
        }

        c.reset();
        assert!(!c.is_calibrated());
        assert_eq!(c.window_len(), 0);
        assert_eq!(c.calibration_count(), 0);
        assert!(c.last_calibrated().is_none());
        assert_eq!(c.noise_sigma(), 0.0);
        assert_eq!(c.window_mean(), 0.0);
    }

    #[test]
    fn test_reset_then_reuse() {
        let mut c = Calibrator::new(small_config());
        for _ in 0..4 {
            c.push(10.0, t(1000));
        }
        c.reset();

        for _ in 0..4 {
            c.push(20.0, t(2000));
        }
        assert!(c.is_calibrated());
        assert!((c.window_mean() - 20.0).abs() < 0.01);
        assert_eq!(c.calibration_count(), 1);
    }

    #[test]
    fn test_reset_preserves_config() {
        let cfg = CalibratorConfig {
            window_size: 8, sigma_k: 5.0,
            default_epsilon: 0.5, recalibrate_interval_s: 100,
        };
        let mut c = Calibrator::new(cfg.clone());
        c.push(1.0, t(1000));
        c.reset();

        assert_eq!(c.config().window_size, 8);
        assert_eq!(c.config().sigma_k, 5.0);
        assert_eq!(c.window_capacity(), 8);
    }

    #[test]
    fn test_noisier_signal_increases_epsilon() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 3.0,
            default_epsilon: 0.0, recalibrate_interval_s: 10,
        };
        let mut c = Calibrator::new(cfg);

        // Quiet signal — small noise
        c.push(10.0, t(1000));
        c.push(10.01, t(1000));
        c.push(9.99, t(1000));
        c.push(10.0, t(1000));
        let eps_quiet = c.effective_epsilon();
        assert!(eps_quiet > 0.0);

        // Evict the quiet window completely with noisy data.
        // Use t(1100) so recalibration interval (10s) is exceeded.
        // Push 4 samples to fully replace the window.
        c.push(10.0, t(1100));
        c.push(11.0, t(1100));
        c.push(9.0, t(1100));
        c.push(10.5, t(1100));

        // Force recalibrate to ensure we pick up the new noise level.
        c.force_calibrate(t(1100));
        let eps_noisy = c.effective_epsilon();

        assert!(eps_noisy > eps_quiet,
                "noisy ε ({}) should be > quiet ε ({})", eps_noisy, eps_quiet);
    }

    #[test]
    fn test_display_uncalibrated() {
        let c = Calibrator::new(small_config());
        let s = c.to_string();
        assert!(s.contains("uncalibrated"));
        assert!(s.contains("0/4"));
    }

    #[test]
    fn test_display_calibrated() {
        let mut c = Calibrator::new(small_config());
        for _ in 0..4 {
            c.push(10.0, t(1000));
        }
        let s = c.to_string();
        assert!(s.contains("ε="));
        assert!(s.contains("σ="));
        assert!(s.contains("k=3"));
        assert!(s.contains("n=1"));
    }

    #[test]
    fn test_display_after_multiple_calibrations() {
        let cfg = CalibratorConfig {
            window_size: 4, sigma_k: 3.0,
            default_epsilon: 0.0, recalibrate_interval_s: 10,
        };
        let mut c = Calibrator::new(cfg);

        for _ in 0..4 { c.push(1.0, t(1000)); }
        for _ in 0..4 { c.push(2.0, t(1100)); }

        let s = c.to_string();
        assert!(s.contains("n=2"));
    }

    #[test]
    fn test_debug() {
        let c = Calibrator::new(small_config());
        let d = format!("{:?}", c);
        assert!(d.contains("Calibrator"));
        assert!(d.contains("epsilon"));
        assert!(d.contains("sigma"));
        assert!(d.contains("calibrated"));
        assert!(d.contains("calibrations"));
    }

    #[test]
    fn test_debug_shows_window_fill() {
        let mut c = Calibrator::new(small_config());
        c.push(1.0, t(1000));
        c.push(2.0, t(1000));
        let d = format!("{:?}", c);
        assert!(d.contains("2/4"));
    }
}