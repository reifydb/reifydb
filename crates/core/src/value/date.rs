// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// A date value representing a calendar date (year, month, day) without time information.
/// Always interpreted in UTC.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct Date {
    inner: NaiveDate,
}

impl Default for Date{
    fn default() -> Self {
        Self::new(1970, 1, 1).unwrap()
    }
}

impl Date {
    pub fn new(year: i32, month: u32, day: u32) -> Option<Self> {
        NaiveDate::from_ymd_opt(year, month, day).map(|inner| Self { inner })
    }

    pub fn from_ymd(year: i32, month: u32, day: u32) -> Result<Self, String> {
        Self::new(year, month, day).ok_or_else(|| format!("Invalid date: {}-{:02}-{:02}", year, month, day))
    }

    pub fn today() -> Self {
        Self::from_naive_date(chrono::Utc::now().date_naive())
    }

    pub fn from_naive_date(date: NaiveDate) -> Self {
        Self { inner: date }
    }

    pub fn year(&self) -> i32 {
        self.inner.year()
    }

    pub fn month(&self) -> u32 {
        self.inner.month()
    }

    pub fn day(&self) -> u32 {
        self.inner.day()
    }

    pub fn inner(&self) -> &NaiveDate {
        &self.inner
    }
}

impl Date {
    /// Convert to days since Unix epoch for storage
    pub fn to_days_since_epoch(&self) -> i32 {
        self.inner.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
    }

    /// Create from days since Unix epoch for storage
    pub fn from_days_since_epoch(days: i32) -> Option<Self> {
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
            .map(|inner| Self { inner })
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.format("%Y-%m-%d"))
    }
}
