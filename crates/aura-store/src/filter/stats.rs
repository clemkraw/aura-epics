//! Welford online mean/variance/standard deviation.
//!
//! Computes running statistics in O(1) per sample with zero allocation.
//! Numerically stable
//!
//! Used by [`super::calibrator::Calibrator`] to estimate signal noise
//! for auto-epsilon calibration: `ε = k × σ`.
//!
//! Reference: Welford, B.P. (1962). "Note on a Method for Calculating
//! Corrected Sums of Squares and Products". Technometrics. 4 (3): 419–420.

use std::fmt;

/// Online mean/variance/stddev accumulator (Welford's algorithm).
///
/// Maintains running statistics with O(1) time and O(1) space per sample.
/// The algorithm is numerically stable for large sample counts.
///
/// ```text
/// let mut s = WelfordStats::new();
/// s.push(4.217);
/// s.push(4.219);
/// s.push(4.215);
/// assert!(s.stddev() < 0.003);
/// ```
#[derive(Clone)]
pub struct WelfordStats {
    /// Number of samples seen.
    count: u64,
    /// Running mean.
    mean: f64,
    /// Running sum of squared deviations from the mean (M2).
    m2: f64,
}

impl WelfordStats {
    /// Create an empty accumulator.
    #[inline]
    pub const fn new() -> Self {
        Self { count: 0, mean: 0.0, m2: 0.0 }
    }

    /// Add a sample value.
    #[inline]
    pub fn push(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    /// Remove the effect of a sample (for sliding window support).
    ///
    /// **Must** be called with the exact value that was pushed.
    /// Calling with a different value corrupts the state.
    /// Only valid when `count > 1`.
    #[inline]
    pub fn pop(&mut self, value: f64) {
        debug_assert!(self.count > 0, "pop on empty WelfordStats");
        if self.count <= 1 {
            *self = Self::new();
            return;
        }
        let delta = value - self.mean;
        self.count -= 1;
        self.mean -= delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 -= delta * delta2;
        // Guard against floating-point drift making m2 negative.
        if self.m2 < 0.0 {
            self.m2 = 0.0;
        }
    }

    /// Number of samples accumulated.
    #[inline]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// Running mean. Returns 0.0 if empty.
    #[inline]
    pub fn mean(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.mean }
    }

    /// Population variance (σ²). Returns 0.0 if count < 2.
    #[inline]
    pub fn variance_population(&self) -> f64 {
        if self.count < 2 { 0.0 } else { self.m2 / self.count as f64 }
    }

    /// Sample variance (s²). Returns 0.0 if count < 2.
    /// Uses Bessel's correction (N-1 denominator).
    #[inline]
    pub fn variance_sample(&self) -> f64 {
        if self.count < 2 { 0.0 } else { self.m2 / (self.count - 1) as f64 }
    }

    /// Population standard deviation (σ). Returns 0.0 if count < 2.
    #[inline]
    pub fn stddev_population(&self) -> f64 {
        self.variance_population().sqrt()
    }

    /// Sample standard deviation (s). Returns 0.0 if count < 2.
    /// This is what the calibrator uses for `ε = k × s`.
    #[inline]
    pub fn stddev(&self) -> f64 {
        self.variance_sample().sqrt()
    }

    /// Whether the accumulator has no samples.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Reset to initial state.
    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new();
    }
}

impl Default for WelfordStats {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for WelfordStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WelfordStats")
            .field("count", &self.count)
            .field("mean", &self.mean())
            .field("stddev", &self.stddev())
            .finish()
    }
}

impl fmt::Display for WelfordStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n={} μ={:.6} σ={:.6}", self.count, self.mean(), self.stddev())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-10;

    fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < EPSILON
    }

    // ── Construction ─────────────────────────────────────────────────

    #[test]
    fn test_new() {
        let s = WelfordStats::new();
        assert_eq!(s.count(), 0);
        assert!(s.is_empty());
        assert_eq!(s.mean(), 0.0);
        assert_eq!(s.stddev(), 0.0);
        assert_eq!(s.variance_sample(), 0.0);
        assert_eq!(s.variance_population(), 0.0);
    }

    #[test]
    fn test_default() {
        let s = WelfordStats::default();
        assert_eq!(s.count(), 0);
        assert!(s.is_empty());
    }

    // ── Single sample ────────────────────────────────────────────────

    #[test]
    fn test_single_sample() {
        let mut s = WelfordStats::new();
        s.push(42.0);
        assert_eq!(s.count(), 1);
        assert!(!s.is_empty());
        assert!(approx_eq(s.mean(), 42.0));
        assert_eq!(s.stddev(), 0.0);         // can't compute std with 1 sample
        assert_eq!(s.variance_sample(), 0.0);
        assert_eq!(s.variance_population(), 0.0);
    }

    // ── Two samples ──────────────────────────────────────────────────

    #[test]
    fn test_two_samples() {
        let mut s = WelfordStats::new();
        s.push(10.0);
        s.push(20.0);
        assert_eq!(s.count(), 2);
        assert!(approx_eq(s.mean(), 15.0));
        // Sample variance: (10-15)² + (20-15)² / (2-1) = 50
        assert!(approx_eq(s.variance_sample(), 50.0));
        // Population variance: 50 / 2 = 25
        assert!(approx_eq(s.variance_population(), 25.0));
        // Sample stddev: sqrt(50) ≈ 7.0711
        assert!(approx_eq(s.stddev(), 50.0_f64.sqrt()));
        // Population stddev: sqrt(25) = 5.0
        assert!(approx_eq(s.stddev_population(), 5.0));
    }

    // ── Known dataset ────────────────────────────────────────────────

    #[test]
    fn test_known_dataset() {
        // Values: [2, 4, 4, 4, 5, 5, 7, 9]
        // Mean = 5.0, Population variance = 4.0, Sample variance = 32/7
        let mut s = WelfordStats::new();
        for &v in &[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            s.push(v);
        }
        assert_eq!(s.count(), 8);
        assert!(approx_eq(s.mean(), 5.0));
        assert!(approx_eq(s.variance_population(), 4.0));
        assert!(approx_eq(s.variance_sample(), 32.0 / 7.0));
        assert!(approx_eq(s.stddev_population(), 2.0));
    }

    // ── Constant signal ──────────────────────────────────────────────

    #[test]
    fn test_constant_signal() {
        let mut s = WelfordStats::new();
        for _ in 0..1000 {
            s.push(4.217);
        }
        assert_eq!(s.count(), 1000);
        assert!(approx_eq(s.mean(), 4.217));
        assert!(s.stddev() < 1e-12); // essentially zero
    }

    // ── Large count ────────────────────────────

    #[test]
    fn test_large_count_stability() {
        let mut s = WelfordStats::new();

        let base = 1_000_000.0;
        for i in 0..100_000u64 {
            s.push(base + (i % 10) as f64 * 0.001);
        }
        assert_eq!(s.count(), 100_000);
        // Mean should be close to base + 0.0045
        assert!((s.mean() - (base + 0.0045)).abs() < 0.001);
        // Stddev should be small (noise is 0..0.009)
        assert!(s.stddev() < 0.01);
        assert!(s.stddev() > 0.0);
    }

    // ── Negative values ──────────────────────────────────────────────

    #[test]
    fn test_negative_values() {
        let mut s = WelfordStats::new();
        s.push(-10.0);
        s.push(-20.0);
        s.push(-30.0);
        assert!(approx_eq(s.mean(), -20.0));
        assert!(s.stddev() > 0.0);
    }

    // ── Mixed positive/negative ──────────────────────────────────────

    #[test]
    fn test_mixed_values() {
        let mut s = WelfordStats::new();
        s.push(-5.0);
        s.push(5.0);
        assert!(approx_eq(s.mean(), 0.0));
        assert!(approx_eq(s.variance_sample(), 50.0));
    }

    // ── Pop (sliding window support) ─────────────────────────────────

    #[test]
    fn test_push_pop_returns_to_state() {
        let mut s = WelfordStats::new();
        s.push(10.0);
        s.push(20.0);
        s.push(30.0);

        let mean_before = s.mean();
        let std_before = s.stddev();

        // Push then pop an outlier — should return to same state
        s.push(100.0);
        assert_eq!(s.count(), 4);
        s.pop(100.0);
        assert_eq!(s.count(), 3);

        assert!((s.mean() - mean_before).abs() < 1e-8);
        assert!((s.stddev() - std_before).abs() < 1e-8);
    }

    #[test]
    fn test_pop_to_one() {
        let mut s = WelfordStats::new();
        s.push(10.0);
        s.push(20.0);
        s.pop(20.0);
        assert_eq!(s.count(), 1);
        assert!(approx_eq(s.mean(), 10.0));
        assert_eq!(s.stddev(), 0.0);
    }

    #[test]
    fn test_pop_to_empty() {
        let mut s = WelfordStats::new();
        s.push(42.0);
        s.pop(42.0);
        assert_eq!(s.count(), 0);
        assert!(s.is_empty());
        assert_eq!(s.mean(), 0.0);
    }

    #[test]
    fn test_pop_m2_guard() {
        // After many push/pop cycles, m2 could drift slightly negative
        // due to floating-point arithmetic. The guard clamps it to 0.
        let mut s = WelfordStats::new();
        for i in 0..100 {
            s.push(i as f64 * 0.001);
        }
        for i in (0..100).rev() {
            s.pop(i as f64 * 0.001);
        }
        assert!(s.is_empty());
        // m2 should be 0 (guarded), not negative
    }

    // ── Reset ────────────────────────────────────────────────────────

    #[test]
    fn test_reset() {
        let mut s = WelfordStats::new();
        s.push(1.0);
        s.push(2.0);
        s.push(3.0);
        s.reset();
        assert!(s.is_empty());
        assert_eq!(s.count(), 0);
        assert_eq!(s.mean(), 0.0);
        assert_eq!(s.stddev(), 0.0);
    }

    #[test]
    fn test_reset_then_reuse() {
        let mut s = WelfordStats::new();
        s.push(100.0);
        s.push(200.0);
        s.reset();
        s.push(10.0);
        s.push(20.0);
        assert_eq!(s.count(), 2);
        assert!(approx_eq(s.mean(), 15.0));
    }

    // ── Clone ────────────────────────────────────────────────────────

    #[test]
    fn test_clone_independent() {
        let mut s = WelfordStats::new();
        s.push(10.0);
        s.push(20.0);
        let mut clone = s.clone();
        clone.push(30.0);
        assert_eq!(s.count(), 2);    // original unchanged
        assert_eq!(clone.count(), 3);
    }

    // ── Display / Debug ──────────────────────────────────────────────

    #[test]
    fn test_display() {
        let mut s = WelfordStats::new();
        s.push(10.0);
        s.push(20.0);
        let display = s.to_string();
        assert!(display.contains("n=2"));
        assert!(display.contains("μ="));
        assert!(display.contains("σ="));
    }

    #[test]
    fn test_debug() {
        let s = WelfordStats::new();
        let debug = format!("{:?}", s);
        assert!(debug.contains("WelfordStats"));
        assert!(debug.contains("count"));
        assert!(debug.contains("mean"));
        assert!(debug.contains("stddev"));
    }

    // ── Edge cases ───────────────────────────────────────────────────

    #[test]
    fn test_zero_values() {
        let mut s = WelfordStats::new();
        s.push(0.0);
        s.push(0.0);
        s.push(0.0);
        assert!(approx_eq(s.mean(), 0.0));
        assert_eq!(s.stddev(), 0.0);
    }

    #[test]
    fn test_very_large_values() {
        let mut s = WelfordStats::new();
        s.push(1e15);
        s.push(1e15 + 1.0);
        assert!(approx_eq(s.mean(), 1e15 + 0.5));
        assert!(s.stddev() > 0.0);
    }

    #[test]
    fn test_very_small_values() {
        let mut s = WelfordStats::new();
        s.push(1e-15);
        s.push(2e-15);
        assert!(s.mean() > 0.0);
        assert!(s.stddev() > 0.0);
    }

    #[test]
    fn test_alternating_values() {
        let mut s = WelfordStats::new();
        for i in 0..1000 {
            s.push(if i % 2 == 0 { 1.0 } else { -1.0 });
        }
        assert!(s.mean().abs() < 0.01);
        assert!(approx_eq(s.stddev_population(), 1.0));
    }
}