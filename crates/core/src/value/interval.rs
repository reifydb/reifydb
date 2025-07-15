// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use chrono::Duration;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// An interval value representing a duration between two points in time.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Interval {
    inner: Duration,
}

impl Default for Interval {
    fn default() -> Self {
        Self::from_seconds(0)
    }
}

impl Interval {
    pub fn new(duration: Duration) -> Self {
        Self { inner: duration }
    }

    pub fn from_seconds(seconds: i64) -> Self {
        Self { inner: Duration::seconds(seconds) }
    }

    pub fn from_milliseconds(milliseconds: i64) -> Self {
        Self { inner: Duration::milliseconds(milliseconds) }
    }

    pub fn from_microseconds(microseconds: i64) -> Self {
        Self { inner: Duration::microseconds(microseconds) }
    }

    pub fn from_nanoseconds(nanoseconds: i64) -> Self {
        Self { inner: Duration::nanoseconds(nanoseconds) }
    }

    pub fn from_minutes(minutes: i64) -> Self {
        Self { inner: Duration::minutes(minutes) }
    }

    pub fn from_hours(hours: i64) -> Self {
        Self { inner: Duration::hours(hours) }
    }

    pub fn from_days(days: i64) -> Self {
        Self { inner: Duration::days(days) }
    }

    pub fn from_weeks(weeks: i64) -> Self {
        Self { inner: Duration::weeks(weeks) }
    }

    pub fn zero() -> Self {
        Self { inner: Duration::zero() }
    }

    pub fn seconds(&self) -> i64 {
        self.inner.num_seconds()
    }

    pub fn milliseconds(&self) -> i64 {
        self.inner.num_milliseconds()
    }

    pub fn microseconds(&self) -> i64 {
        self.inner.num_microseconds().unwrap_or(0)
    }

    pub fn nanoseconds(&self) -> i64 {
        self.inner.num_nanoseconds().unwrap_or(0)
    }

    pub fn inner(&self) -> &Duration {
        &self.inner
    }

    pub fn is_positive(&self) -> bool {
        self.inner > Duration::zero()
    }

    pub fn is_negative(&self) -> bool {
        self.inner < Duration::zero()
    }

    pub fn abs(&self) -> Self {
        Self { inner: self.inner.abs() }
    }

    pub fn negate(&self) -> Self {
        Self { inner: -self.inner }
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl Interval {
    /// Convert to nanoseconds for storage
    pub fn to_nanos(&self) -> i64 {
        self.inner.num_nanoseconds().unwrap_or(0)
    }

    /// Create from nanoseconds for storage
    pub fn from_nanos(nanos: i64) -> Self {
        Self { inner: chrono::Duration::nanoseconds(nanos) }
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let total_seconds = self.inner.num_seconds();
        let days = total_seconds / 86400;
        let hours = (total_seconds % 86400) / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        let nanos = self.inner.subsec_nanos();

        if days != 0 {
            write!(f, "{}d {}h {}m {}.{:09}s", days, hours, minutes, seconds, nanos)
        } else if hours != 0 {
            write!(f, "{}h {}m {}.{:09}s", hours, minutes, seconds, nanos)
        } else if minutes != 0 {
            write!(f, "{}m {}.{:09}s", minutes, seconds, nanos)
        } else {
            write!(f, "{}.{:09}s", seconds, nanos)
        }
    }
}
