// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{Display, Formatter};

use chrono::{DateTime as ChronoDateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{Date, Time};

/// A date and time value with nanosecond precision.
/// Always in UTC timezone.
#[derive(
	Copy,
	Clone,
	Debug,
	PartialEq,
	Eq,
	Hash,
	Serialize,
	Deserialize,
	PartialOrd,
	Ord,
)]
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
			.and_then(|date| {
				date.and_hms_nano_opt(hour, min, sec, nano)
			})
			.map(|naive| Self {
				inner: naive.and_utc(),
			})
	}

	pub fn from_chrono_datetime(dt: ChronoDateTime<Utc>) -> Self {
		Self {
			inner: dt,
		}
	}

	pub fn from_ymd_hms(
		year: i32,
		month: u32,
		day: u32,
		hour: u32,
		min: u32,
		sec: u32,
	) -> Result<Self, String> {
		Self::new(year, month, day, hour, min, sec, 0).ok_or_else(
			|| {
				format!(
					"Invalid datetime: {}-{:02}-{:02} {:02}:{:02}:{:02}",
					year, month, day, hour, min, sec
				)
			},
		)
	}

	pub fn from_timestamp(timestamp: i64) -> Result<Self, String> {
		chrono::DateTime::from_timestamp(timestamp, 0)
			.map(Self::from_chrono_datetime)
			.ok_or_else(|| {
				format!("Invalid timestamp: {}", timestamp)
			})
	}

	pub fn from_timestamp_millis(millis: i64) -> Result<Self, String> {
		chrono::DateTime::from_timestamp_millis(millis)
			.map(Self::from_chrono_datetime)
			.ok_or_else(|| {
				format!("Invalid timestamp millis: {}", millis)
			})
	}

	pub fn now() -> Self {
		Self {
			inner: Utc::now(),
		}
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
		Self {
			inner: chrono::DateTime::from_timestamp_nanos(nanos),
		}
	}

	/// Create from separate seconds and nanoseconds
	pub fn from_parts(seconds: i64, nanos: u32) -> Result<Self, String> {
		chrono::DateTime::from_timestamp(seconds, nanos)
			.map(Self::from_chrono_datetime)
			.ok_or_else(|| {
				format!(
					"Invalid timestamp parts: seconds={}, nanos={}",
					seconds, nanos
				)
			})
	}

	/// Get separate seconds and nanoseconds for storage
	pub fn to_parts(&self) -> (i64, u32) {
		let seconds = self.inner.timestamp();
		let nanos = self.inner.timestamp_subsec_nanos();
		(seconds, nanos)
	}
}

impl Display for DateTime {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.inner.format("%Y-%m-%dT%H:%M:%S%.9fZ"))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_datetime_display_standard_format() {
		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 123456789)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.123456789Z"
		);

		let datetime = DateTime::new(2000, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2000-01-01T00:00:00.000000000Z"
		);

		let datetime =
			DateTime::new(1999, 12, 31, 23, 59, 59, 999999999)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"1999-12-31T23:59:59.999999999Z"
		);
	}

	#[test]
	fn test_datetime_display_millisecond_precision() {
		// Test various millisecond values
		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 123000000)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.123000000Z"
		);

		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 001000000)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.001000000Z"
		);

		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 999000000)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.999000000Z"
		);
	}

	#[test]
	fn test_datetime_display_microsecond_precision() {
		// Test various microsecond values
		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 123456000)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.123456000Z"
		);

		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 000001000)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.000001000Z"
		);

		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 999999000)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.999999000Z"
		);
	}

	#[test]
	fn test_datetime_display_nanosecond_precision() {
		// Test various nanosecond values
		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 123456789)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.123456789Z"
		);

		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 000000001)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.000000001Z"
		);

		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 999999999)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.999999999Z"
		);
	}

	#[test]
	fn test_datetime_display_zero_fractional_seconds() {
		let datetime =
			DateTime::new(2024, 3, 15, 14, 30, 45, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T14:30:45.000000000Z"
		);

		let datetime = DateTime::new(2024, 3, 15, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T00:00:00.000000000Z"
		);
	}

	#[test]
	fn test_datetime_display_edge_times() {
		// Midnight
		let datetime = DateTime::new(2024, 3, 15, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T00:00:00.000000000Z"
		);

		// Almost midnight next day
		let datetime =
			DateTime::new(2024, 3, 15, 23, 59, 59, 999999999)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T23:59:59.999999999Z"
		);

		// Noon
		let datetime = DateTime::new(2024, 3, 15, 12, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-03-15T12:00:00.000000000Z"
		);
	}

	#[test]
	fn test_datetime_display_unix_epoch() {
		let datetime = DateTime::new(1970, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"1970-01-01T00:00:00.000000000Z"
		);

		let datetime = DateTime::new(1970, 1, 1, 0, 0, 1, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"1970-01-01T00:00:01.000000000Z"
		);
	}

	#[test]
	fn test_datetime_display_leap_year() {
		let datetime =
			DateTime::new(2024, 2, 29, 12, 30, 45, 123456789)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2024-02-29T12:30:45.123456789Z"
		);

		let datetime = DateTime::new(2000, 2, 29, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2000-02-29T00:00:00.000000000Z"
		);
	}

	#[test]
	fn test_datetime_display_boundary_dates() {
		// Very early date
		let datetime = DateTime::new(1, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"0001-01-01T00:00:00.000000000Z"
		);

		// Far future date
		let datetime =
			DateTime::new(9999, 12, 31, 23, 59, 59, 999999999)
				.unwrap();
		assert_eq!(
			format!("{}", datetime),
			"9999-12-31T23:59:59.999999999Z"
		);

		// Century boundaries
		let datetime = DateTime::new(1900, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"1900-01-01T00:00:00.000000000Z"
		);

		let datetime = DateTime::new(2000, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2000-01-01T00:00:00.000000000Z"
		);

		let datetime = DateTime::new(2100, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2100-01-01T00:00:00.000000000Z"
		);
	}

	#[test]
	fn test_datetime_display_default() {
		let datetime = DateTime::default();
		assert_eq!(
			format!("{}", datetime),
			"1970-01-01T00:00:00.000000000Z"
		);
	}

	#[test]
	fn test_datetime_display_all_hours() {
		for hour in 0..24 {
			let datetime = DateTime::new(
				2024, 3, 15, hour, 30, 45, 123456789,
			)
			.unwrap();
			let expected = format!(
				"2024-03-15T{:02}:30:45.123456789Z",
				hour
			);
			assert_eq!(format!("{}", datetime), expected);
		}
	}

	#[test]
	fn test_datetime_display_all_minutes() {
		for minute in 0..60 {
			let datetime = DateTime::new(
				2024, 3, 15, 14, minute, 45, 123456789,
			)
			.unwrap();
			let expected = format!(
				"2024-03-15T14:{:02}:45.123456789Z",
				minute
			);
			assert_eq!(format!("{}", datetime), expected);
		}
	}

	#[test]
	fn test_datetime_display_all_seconds() {
		for second in 0..60 {
			let datetime = DateTime::new(
				2024, 3, 15, 14, 30, second, 123456789,
			)
			.unwrap();
			let expected = format!(
				"2024-03-15T14:30:{:02}.123456789Z",
				second
			);
			assert_eq!(format!("{}", datetime), expected);
		}
	}

	#[test]
	fn test_datetime_display_from_timestamp() {
		let datetime = DateTime::from_timestamp(0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"1970-01-01T00:00:00.000000000Z"
		);

		let datetime = DateTime::from_timestamp(1234567890).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2009-02-13T23:31:30.000000000Z"
		);
	}

	#[test]
	fn test_datetime_display_from_timestamp_millis() {
		let datetime =
			DateTime::from_timestamp_millis(1234567890123).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2009-02-13T23:31:30.123000000Z"
		);

		let datetime = DateTime::from_timestamp_millis(0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"1970-01-01T00:00:00.000000000Z"
		);
	}

	#[test]
	fn test_datetime_display_from_parts() {
		let datetime =
			DateTime::from_parts(1234567890, 123456789).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"2009-02-13T23:31:30.123456789Z"
		);

		let datetime = DateTime::from_parts(0, 0).unwrap();
		assert_eq!(
			format!("{}", datetime),
			"1970-01-01T00:00:00.000000000Z"
		);
	}
}
