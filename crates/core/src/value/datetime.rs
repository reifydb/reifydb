// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use chrono::{DateTime as ChronoDateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use super::{Date, Time};

/// A date and time value with nanosecond precision.
/// Always in UTC timezone.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DateTime {
    inner: ChronoDateTime<Utc>,
}

impl Default for DateTime {
    fn default() -> Self {
        Self::new(1970, 1, 1, 0, 0, 0, 0).unwrap()
    }
}

impl DateTime {
    pub fn new(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        nano: u32,
    ) -> Option<Self> {
        NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_nano_opt(hour, min, sec, nano))
            .map(|naive| Self { inner: naive.and_utc() })
    }

    pub fn from_chrono_datetime(dt: ChronoDateTime<Utc>) -> Self {
        Self { inner: dt }
    }

    pub fn now() -> Self {
        Self { inner: Utc::now() }
    }

    pub fn timestamp(&self) -> i64 {
        self.inner.timestamp()
    }

    pub fn timestamp_nanos(&self) -> i64 {
        self.inner.timestamp_nanos_opt().unwrap_or(0)
    }

    pub fn date(&self) -> Date {
        Date::from_naive_date(self.inner.date_naive())
    }

    pub fn time(&self) -> Time {
        Time::from_naive_time(self.inner.time())
    }

    pub fn inner(&self) -> &ChronoDateTime<Utc> {
        &self.inner
    }
}

impl DateTime {
    /// Convert to nanoseconds since Unix epoch for storage
    pub fn to_nanos_since_epoch(&self) -> i64 {
        self.inner.timestamp_nanos_opt().unwrap_or(0)
    }

    /// Create from nanoseconds since Unix epoch for storage
    pub fn from_nanos_since_epoch(nanos: i64) -> Self {
        Self { inner: chrono::DateTime::from_timestamp_nanos(nanos) }
    }
}

impl Display for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.format("%Y-%m-%dT%H:%M:%S%.9fZ"))
    }
}
