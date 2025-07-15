// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use chrono::{NaiveTime, Timelike};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// A time value representing time of day (hour, minute, second, nanosecond) without date information.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct Time {
    inner: NaiveTime,
}

impl Time {
    pub fn new(hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
        NaiveTime::from_hms_nano_opt(hour, min, sec, nano).map(|inner| Self { inner })
    }

    pub fn from_naive_time(time: NaiveTime) -> Self {
        Self { inner: time }
    }

    pub fn hour(&self) -> u32 {
        self.inner.hour()
    }

    pub fn minute(&self) -> u32 {
        self.inner.minute()
    }

    pub fn second(&self) -> u32 {
        self.inner.second()
    }

    pub fn nanosecond(&self) -> u32 {
        self.inner.nanosecond()
    }

    pub fn inner(&self) -> &NaiveTime {
        &self.inner
    }
}

impl Display for Time {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.format("%H:%M:%S%.9f"))
    }
}
