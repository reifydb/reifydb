// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::{Display, Formatter};

use chrono::{NaiveTime, Timelike};
use serde::{Deserialize, Serialize};

/// A time value representing time of day (hour, minute, second, nanosecond)
/// without date information.
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
pub struct Time {
	inner: NaiveTime,
}

impl Default for Time {
	fn default() -> Self {
		Self::new(0, 0, 0, 0).unwrap()
	}
}

impl Time {
	pub fn new(hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
		NaiveTime::from_hms_nano_opt(hour, min, sec, nano).map(
			|inner| Self {
				inner,
			},
		)
	}

	pub fn from_naive_time(time: NaiveTime) -> Self {
		Self {
			inner: time,
		}
	}

	pub fn from_hms(hour: u32, min: u32, sec: u32) -> Result<Self, String> {
		Self::new(hour, min, sec, 0).ok_or_else(|| {
			format!(
				"Invalid time: {:02}:{:02}:{:02}",
				hour, min, sec
			)
		})
	}

	pub fn from_hms_nano(
		hour: u32,
		min: u32,
		sec: u32,
		nano: u32,
	) -> Result<Self, String> {
		Self::new(hour, min, sec, nano).ok_or_else(|| {
			format!(
				"Invalid time: {:02}:{:02}:{:02}.{:09}",
				hour, min, sec, nano
			)
		})
	}

	pub fn midnight() -> Self {
		Self::new(0, 0, 0, 0).unwrap()
	}

	pub fn noon() -> Self {
		Self::new(12, 0, 0, 0).unwrap()
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

impl Time {
	/// Convert to nanoseconds since midnight for storage
	pub fn to_nanos_since_midnight(&self) -> u64 {
		self.inner.num_seconds_from_midnight() as u64 * 1_000_000_000
			+ self.inner.nanosecond() as u64
	}

	/// Create from nanoseconds since midnight for storage
	pub fn from_nanos_since_midnight(nanos: u64) -> Option<Self> {
		let seconds = (nanos / 1_000_000_000) as u32;
		let nano = (nanos % 1_000_000_000) as u32;
		NaiveTime::from_num_seconds_from_midnight_opt(seconds, nano)
			.map(|inner| Self {
				inner,
			})
	}
}

impl Display for Time {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.inner.format("%H:%M:%S%.9f"))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_time_display_standard_format() {
		let time = Time::new(14, 30, 45, 123456789).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.123456789");

		let time = Time::new(0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", time), "00:00:00.000000000");

		let time = Time::new(23, 59, 59, 999999999).unwrap();
		assert_eq!(format!("{}", time), "23:59:59.999999999");
	}

	#[test]
	fn test_time_display_millisecond_precision() {
		// Test various millisecond values
		let time = Time::new(14, 30, 45, 123000000).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.123000000");

		let time = Time::new(14, 30, 45, 001000000).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.001000000");

		let time = Time::new(14, 30, 45, 999000000).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.999000000");
	}

	#[test]
	fn test_time_display_microsecond_precision() {
		// Test various microsecond values
		let time = Time::new(14, 30, 45, 123456000).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.123456000");

		let time = Time::new(14, 30, 45, 000001000).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.000001000");

		let time = Time::new(14, 30, 45, 999999000).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.999999000");
	}

	#[test]
	fn test_time_display_nanosecond_precision() {
		// Test various nanosecond values
		let time = Time::new(14, 30, 45, 123456789).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.123456789");

		let time = Time::new(14, 30, 45, 000000001).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.000000001");

		let time = Time::new(14, 30, 45, 999999999).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.999999999");
	}

	#[test]
	fn test_time_display_zero_fractional_seconds() {
		let time = Time::new(14, 30, 45, 0).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.000000000");

		let time = Time::new(0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", time), "00:00:00.000000000");
	}

	#[test]
	fn test_time_display_edge_times() {
		// Midnight
		let time = Time::new(0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", time), "00:00:00.000000000");

		// Almost midnight next day
		let time = Time::new(23, 59, 59, 999999999).unwrap();
		assert_eq!(format!("{}", time), "23:59:59.999999999");

		// Noon
		let time = Time::new(12, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", time), "12:00:00.000000000");

		// One second before midnight
		let time = Time::new(23, 59, 58, 999999999).unwrap();
		assert_eq!(format!("{}", time), "23:59:58.999999999");

		// One second after midnight
		let time = Time::new(0, 0, 1, 0).unwrap();
		assert_eq!(format!("{}", time), "00:00:01.000000000");
	}

	#[test]
	fn test_time_display_special_times() {
		// Test midnight and noon constructors
		let midnight = Time::midnight();
		assert_eq!(format!("{}", midnight), "00:00:00.000000000");

		let noon = Time::noon();
		assert_eq!(format!("{}", noon), "12:00:00.000000000");

		// Test default
		let default = Time::default();
		assert_eq!(format!("{}", default), "00:00:00.000000000");
	}

	#[test]
	fn test_time_display_all_hours() {
		for hour in 0..24 {
			let time = Time::new(hour, 30, 45, 123456789).unwrap();
			let expected = format!("{:02}:30:45.123456789", hour);
			assert_eq!(format!("{}", time), expected);
		}
	}

	#[test]
	fn test_time_display_all_minutes() {
		for minute in 0..60 {
			let time =
				Time::new(14, minute, 45, 123456789).unwrap();
			let expected = format!("14:{:02}:45.123456789", minute);
			assert_eq!(format!("{}", time), expected);
		}
	}

	#[test]
	fn test_time_display_all_seconds() {
		for second in 0..60 {
			let time =
				Time::new(14, 30, second, 123456789).unwrap();
			let expected = format!("14:30:{:02}.123456789", second);
			assert_eq!(format!("{}", time), expected);
		}
	}

	#[test]
	fn test_time_display_from_hms() {
		let time = Time::from_hms(14, 30, 45).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.000000000");

		let time = Time::from_hms(0, 0, 0).unwrap();
		assert_eq!(format!("{}", time), "00:00:00.000000000");

		let time = Time::from_hms(23, 59, 59).unwrap();
		assert_eq!(format!("{}", time), "23:59:59.000000000");
	}

	#[test]
	fn test_time_display_from_hms_nano() {
		let time = Time::from_hms_nano(14, 30, 45, 123456789).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.123456789");

		let time = Time::from_hms_nano(0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", time), "00:00:00.000000000");

		let time = Time::from_hms_nano(23, 59, 59, 999999999).unwrap();
		assert_eq!(format!("{}", time), "23:59:59.999999999");
	}

	#[test]
	fn test_time_display_from_nanos_since_midnight() {
		// Test midnight
		let time = Time::from_nanos_since_midnight(0).unwrap();
		assert_eq!(format!("{}", time), "00:00:00.000000000");

		// Test 1 second
		let time =
			Time::from_nanos_since_midnight(1_000_000_000).unwrap();
		assert_eq!(format!("{}", time), "00:00:01.000000000");

		// Test 1 minute
		let time = Time::from_nanos_since_midnight(60_000_000_000)
			.unwrap();
		assert_eq!(format!("{}", time), "00:01:00.000000000");

		// Test 1 hour
		let time = Time::from_nanos_since_midnight(3_600_000_000_000)
			.unwrap();
		assert_eq!(format!("{}", time), "01:00:00.000000000");

		// Test complex time with nanoseconds
		let nanos = 14 * 3600 * 1_000_000_000
			+ 30 * 60 * 1_000_000_000
			+ 45 * 1_000_000_000 + 123456789;
		let time = Time::from_nanos_since_midnight(nanos).unwrap();
		assert_eq!(format!("{}", time), "14:30:45.123456789");
	}

	#[test]
	fn test_time_display_boundary_values() {
		// Test the very last nanosecond of the day
		let nanos = 24 * 3600 * 1_000_000_000 - 1;
		let time = Time::from_nanos_since_midnight(nanos).unwrap();
		assert_eq!(format!("{}", time), "23:59:59.999999999");

		// Test the very first nanosecond of the day
		let time = Time::from_nanos_since_midnight(1).unwrap();
		assert_eq!(format!("{}", time), "00:00:00.000000001");
	}

	#[test]
	fn test_time_display_precision_patterns() {
		// Test different precision patterns
		let time = Time::new(14, 30, 45, 100000000).unwrap(); // 0.1 seconds
		assert_eq!(format!("{}", time), "14:30:45.100000000");

		let time = Time::new(14, 30, 45, 010000000).unwrap(); // 0.01 seconds
		assert_eq!(format!("{}", time), "14:30:45.010000000");

		let time = Time::new(14, 30, 45, 001000000).unwrap(); // 0.001 seconds
		assert_eq!(format!("{}", time), "14:30:45.001000000");

		let time = Time::new(14, 30, 45, 000100000).unwrap(); // 0.0001 seconds
		assert_eq!(format!("{}", time), "14:30:45.000100000");

		let time = Time::new(14, 30, 45, 000010000).unwrap(); // 0.00001 seconds
		assert_eq!(format!("{}", time), "14:30:45.000010000");

		let time = Time::new(14, 30, 45, 000001000).unwrap(); // 0.000001 seconds
		assert_eq!(format!("{}", time), "14:30:45.000001000");

		let time = Time::new(14, 30, 45, 000000100).unwrap(); // 0.0000001 seconds
		assert_eq!(format!("{}", time), "14:30:45.000000100");

		let time = Time::new(14, 30, 45, 000000010).unwrap(); // 0.00000001 seconds
		assert_eq!(format!("{}", time), "14:30:45.000000010");

		let time = Time::new(14, 30, 45, 000000001).unwrap(); // 0.000000001 seconds
		assert_eq!(format!("{}", time), "14:30:45.000000001");
	}
}
