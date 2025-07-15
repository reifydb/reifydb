// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use chrono::Datelike;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// A date value representing a calendar date (year, month, day) without time information.
/// Always interpreted in UTC.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct Date {
    inner: NaiveDate,
}

impl Date {
    pub fn new(year: i32, month: u32, day: u32) -> Option<Self> {
        NaiveDate::from_ymd_opt(year, month, day).map(|inner| Self { inner })
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

impl Display for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.format("%Y-%m-%d"))
    }
}
