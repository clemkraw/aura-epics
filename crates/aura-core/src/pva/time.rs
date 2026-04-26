//! EPICS PVA timestamp — nanosecond precision.
//!
//! Every PVAccess Normative Type carries a `time_t` structure with
//! nanosecond-resolution timestamps. This module provides conversion
//! to/from `chrono::DateTime<Utc>` and ordering/arithmetic operations.
//!
//! Reference: EPICS PVAccess Normative Types Specification, Section 4.2

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// PVAccess timestamp — seconds + nanoseconds since Unix epoch.
///
/// Implements `Ord` for chronological sorting and `Copy` for zero-cost passing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TimeStamp {
    /// Seconds since 1970-01-01 00:00:00 UTC.
    pub seconds: i64,
    /// Sub-second nanoseconds (0–999_999_999).
    pub nanoseconds: u32,
    /// User-defined tag (application-specific, typically 0).
    #[serde(default)]
    pub user_tag: i32,
}

impl TimeStamp {
    /// Create a timestamp with no user tag.
    #[inline]
    pub fn new(seconds: i64, nanoseconds: u32) -> Self {
        Self { seconds, nanoseconds, user_tag: 0 }
    }

    /// Create a timestamp with a user tag.
    #[inline]
    pub fn with_tag(seconds: i64, nanoseconds: u32, user_tag: i32) -> Self {
        Self { seconds, nanoseconds, user_tag }
    }

    pub fn now() -> Self {
        Self::from_datetime(Utc::now())
    }

    /// Convert to `chrono::DateTime<Utc>`.
    #[inline]
    pub fn to_datetime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.seconds, self.nanoseconds)
            .unwrap_or_default()
    }

    /// Create from `chrono::DateTime<Utc>`.
    #[inline]
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self {
            seconds: dt.timestamp(),
            nanoseconds: dt.timestamp_subsec_nanos(),
            user_tag: 0,
        }
    }

    /// Create from `std::time::SystemTime`.
    pub fn from_system_time(st: SystemTime) -> Self {
        match st.duration_since(UNIX_EPOCH) {
            Ok(d) => Self::new(d.as_secs() as i64, d.subsec_nanos()),
            Err(e) => {
                let d = e.duration();
                Self::new(-(d.as_secs() as i64), d.subsec_nanos())
            }
        }
    }

    /// Convert to total nanoseconds since epoch.
    #[inline]
    pub fn as_nanos(&self) -> i128 {
        (self.seconds as i128) * 1_000_000_000 + (self.nanoseconds as i128)
    }

    /// Create from total nanoseconds since epoch.
    pub fn from_nanos(nanos: i128) -> Self {
        let seconds = (nanos / 1_000_000_000) as i64;
        let nanoseconds = (nanos % 1_000_000_000) as u32;
        Self::new(seconds, nanoseconds)
    }

    /// Convert to total microseconds since epoch.
    #[inline]
    pub fn as_micros(&self) -> i64 {
        self.seconds * 1_000_000 + (self.nanoseconds / 1_000) as i64
    }

    /// Duration elapsed between two timestamps.
    /// Returns `None` if `other` is before `self`.
    pub fn duration_since(&self, other: &Self) -> Option<Duration> {
        let diff_nanos = self.as_nanos() - other.as_nanos();
        if diff_nanos < 0 {
            return None;
        }
        Some(Duration::from_nanos(diff_nanos as u64))
    }

    /// Whether this timestamp represents the epoch (zero).
    #[inline]
    pub fn is_epoch(&self) -> bool {
        self.seconds == 0 && self.nanoseconds == 0
    }
}

impl Default for TimeStamp {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

impl fmt::Display for TimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_datetime().format("%Y-%m-%dT%H:%M:%S%.9fZ"))
    }
}

impl From<DateTime<Utc>> for TimeStamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Self::from_datetime(dt)
    }
}

impl From<TimeStamp> for DateTime<Utc> {
    fn from(ts: TimeStamp) -> Self {
        ts.to_datetime()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Construction ─────────────────────────────────────────────────

    #[test]
    fn test_new() {
        let ts = TimeStamp::new(1713520000, 500_000_000);
        assert_eq!(ts.seconds, 1713520000);
        assert_eq!(ts.nanoseconds, 500_000_000);
        assert_eq!(ts.user_tag, 0);
    }

    #[test]
    fn test_with_tag() {
        let ts = TimeStamp::with_tag(100, 200, 42);
        assert_eq!(ts.seconds, 100);
        assert_eq!(ts.nanoseconds, 200);
        assert_eq!(ts.user_tag, 42);
    }

    #[test]
    fn test_default_is_epoch() {
        let ts = TimeStamp::default();
        assert_eq!(ts.seconds, 0);
        assert_eq!(ts.nanoseconds, 0);
        assert!(ts.is_epoch());
    }

    #[test]
    fn test_now_is_not_epoch() {
        let ts = TimeStamp::now();
        assert!(!ts.is_epoch());
        assert!(ts.seconds > 1_700_000_000); // after 2023
    }

    // ── Chrono conversion roundtrip ──────────────────────────────────

    #[test]
    fn test_chrono_roundtrip() {
        let ts = TimeStamp::new(1713520000, 123_456_789);
        let dt = ts.to_datetime();
        let ts2 = TimeStamp::from_datetime(dt);
        assert_eq!(ts.seconds, ts2.seconds);
        assert_eq!(ts.nanoseconds, ts2.nanoseconds);
    }

    #[test]
    fn test_chrono_roundtrip_zero_nanos() {
        let ts = TimeStamp::new(1713520000, 0);
        let dt = ts.to_datetime();
        let ts2 = TimeStamp::from_datetime(dt);
        assert_eq!(ts, ts2);
    }

    #[test]
    fn test_chrono_roundtrip_max_nanos() {
        let ts = TimeStamp::new(1713520000, 999_999_999);
        let dt = ts.to_datetime();
        let ts2 = TimeStamp::from_datetime(dt);
        assert_eq!(ts, ts2);
    }

    // ── From/Into traits ─────────────────────────────────────────────

    #[test]
    fn test_from_datetime() {
        let dt = Utc::now();
        let ts: TimeStamp = dt.into();
        assert_eq!(ts.seconds, dt.timestamp());
        assert_eq!(ts.nanoseconds, dt.timestamp_subsec_nanos());
    }

    #[test]
    fn test_into_datetime() {
        let ts = TimeStamp::new(1713520000, 0);
        let dt: DateTime<Utc> = ts.into();
        assert_eq!(dt.timestamp(), 1713520000);
    }

    // ── SystemTime conversion ────────────────────────────────────────

    #[test]
    fn test_from_system_time() {
        let st = UNIX_EPOCH + Duration::from_secs(1713520000) + Duration::from_nanos(123_456_000);
        let ts = TimeStamp::from_system_time(st);
        assert_eq!(ts.seconds, 1713520000);
        assert_eq!(ts.nanoseconds, 123_456_000);
    }

    #[test]
    fn test_from_system_time_epoch() {
        let ts = TimeStamp::from_system_time(UNIX_EPOCH);
        assert!(ts.is_epoch());
    }

    // ── Nanosecond conversion ────────────────────────────────────────

    #[test]
    fn test_as_nanos() {
        let ts = TimeStamp::new(1, 500_000_000);
        assert_eq!(ts.as_nanos(), 1_500_000_000);
    }

    #[test]
    fn test_as_nanos_zero() {
        assert_eq!(TimeStamp::default().as_nanos(), 0);
    }

    #[test]
    fn test_from_nanos_roundtrip() {
        let ts = TimeStamp::new(1713520000, 123_456_789);
        let nanos = ts.as_nanos();
        let ts2 = TimeStamp::from_nanos(nanos);
        assert_eq!(ts.seconds, ts2.seconds);
        assert_eq!(ts.nanoseconds, ts2.nanoseconds);
    }

    // ── Microsecond conversion ───────────────────────────────────────

    #[test]
    fn test_as_micros() {
        let ts = TimeStamp::new(1, 500_000);
        assert_eq!(ts.as_micros(), 1_000_500);
    }

    #[test]
    fn test_as_micros_truncates_nanos() {
        // 123_456_789 ns → 123_456 µs (truncated, not rounded)
        let ts = TimeStamp::new(0, 123_456_789);
        assert_eq!(ts.as_micros(), 123_456);
    }

    // ── Duration since ───────────────────────────────────────────────

    #[test]
    fn test_duration_since() {
        let a = TimeStamp::new(10, 0);
        let b = TimeStamp::new(7, 500_000_000);
        let d = a.duration_since(&b).unwrap();
        assert_eq!(d, Duration::from_millis(2500));
    }

    #[test]
    fn test_duration_since_same() {
        let a = TimeStamp::new(10, 0);
        let d = a.duration_since(&a).unwrap();
        assert_eq!(d, Duration::ZERO);
    }

    #[test]
    fn test_duration_since_before_returns_none() {
        let a = TimeStamp::new(5, 0);
        let b = TimeStamp::new(10, 0);
        assert!(a.duration_since(&b).is_none());
    }

    // ── Ordering ─────────────────────────────────────────────────────

    #[test]
    fn test_ordering_by_seconds() {
        let a = TimeStamp::new(1, 0);
        let b = TimeStamp::new(2, 0);
        assert!(a < b);
    }

    #[test]
    fn test_ordering_by_nanoseconds() {
        let a = TimeStamp::new(1, 100);
        let b = TimeStamp::new(1, 200);
        assert!(a < b);
    }

    #[test]
    fn test_ordering_equal() {
        let a = TimeStamp::new(1, 100);
        let b = TimeStamp::new(1, 100);
        assert_eq!(a, b);
    }

    #[test]
    fn test_sort() {
        let mut timestamps = vec![
            TimeStamp::new(3, 0),
            TimeStamp::new(1, 0),
            TimeStamp::new(2, 500),
            TimeStamp::new(2, 0),
        ];
        timestamps.sort();
        assert_eq!(timestamps[0], TimeStamp::new(1, 0));
        assert_eq!(timestamps[1], TimeStamp::new(2, 0));
        assert_eq!(timestamps[2], TimeStamp::new(2, 500));
        assert_eq!(timestamps[3], TimeStamp::new(3, 0));
    }

    // ── Display ──────────────────────────────────────────────────────

    #[test]
    fn test_display() {
        let ts = TimeStamp::new(1713520000, 123_456_789);
        let s = ts.to_string();
        assert!(s.contains("2024-04-19"), "got: {}", s);
        assert!(s.contains("123456789"), "nanoseconds missing: {}", s);
        assert!(s.ends_with('Z'));
    }

    #[test]
    fn test_display_epoch() {
        let ts = TimeStamp::default();
        let s = ts.to_string();
        assert!(s.contains("1970-01-01"), "got: {}", s);
    }

    // ── Copy / Hash ──────────────────────────────────────────────────

    #[test]
    fn test_copy() {
        let a = TimeStamp::new(1, 2);
        let b = a; // Copy
        assert_eq!(a, b);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TimeStamp::new(1, 0));
        set.insert(TimeStamp::new(1, 0)); // duplicate
        set.insert(TimeStamp::new(2, 0));
        assert_eq!(set.len(), 2);
    }

    // ── Serde ────────────────────────────────────────────────────────

    #[test]
    fn test_serde_roundtrip() {
        let ts = TimeStamp::with_tag(1713520000, 123_456_789, 7);
        let json = serde_json::to_string(&ts).unwrap();
        let back: TimeStamp = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, back);
    }

    #[test]
    fn test_serde_default_user_tag() {
        let json = r#"{"seconds":100,"nanoseconds":200}"#;
        let ts: TimeStamp = serde_json::from_str(json).unwrap();
        assert_eq!(ts.user_tag, 0); // default
    }

    #[test]
    fn test_serde_json_format() {
        let ts = TimeStamp::new(100, 200);
        let json = serde_json::to_string(&ts).unwrap();
        assert!(json.contains(r#""seconds":100"#));
        assert!(json.contains(r#""nanoseconds":200"#));
    }

    // ── Debug ────────────────────────────────────────────────────────

    #[test]
    fn test_debug() {
        let ts = TimeStamp::new(1, 2);
        let debug = format!("{:?}", ts);
        assert!(debug.contains("seconds"));
        assert!(debug.contains("nanoseconds"));
    }
}