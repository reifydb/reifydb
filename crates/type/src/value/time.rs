// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{Display, Formatter};

use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

/// A time value representing time of day (hour, minute, second, nanosecond)
/// without date information.
///
/// Internally stored as nanoseconds since midnight (00:00:00.000000000).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Time {
	// Nanoseconds since midnight (0 to 86_399_999_999_999)
	nanos_since_midnight: u64,
}

impl Default for Time {
	fn default() -> Self {
		Self {
			nanos_since_midnight: 0,
		} // 00:00:00.000000000
	}
}

impl Time {
	/// Maximum valid nanoseconds in a day
	const MAX_NANOS_IN_DAY: u64 = 86_399_999_999_999;
	const NANOS_PER_SECOND: u64 = 1_000_000_000;
	const NANOS_PER_MINSVTE: u64 = 60 * Self::NANOS_PER_SECOND;
	const NANOS_PER_HOUR: u64 = 60 * Self::NANOS_PER_MINSVTE;

	pub fn new(hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
		// Validate inputs
		if hour >= 24 || min >= 60 || sec >= 60 || nano >= Self::NANOS_PER_SECOND as u32 {
			return None;
		}

		let nanos = hour as u64 * Self::NANOS_PER_HOUR
			+ min as u64 * Self::NANOS_PER_MINSVTE
			+ sec as u64 * Self::NANOS_PER_SECOND
			+ nano as u64;

		Some(Self {
			nanos_since_midnight: nanos,
		})
	}

	pub fn from_hms(hour: u32, min: u32, sec: u32) -> Result<Self, String> {
		Self::new(hour, min, sec, 0).ok_or_else(|| format!("Invalid time: {:02}:{:02}:{:02}", hour, min, sec))
	}

	pub fn from_hms_nano(hour: u32, min: u32, sec: u32, nano: u32) -> Result<Self, String> {
		Self::new(hour, min, sec, nano)
			.ok_or_else(|| format!("Invalid time: {:02}:{:02}:{:02}.{:09}", hour, min, sec, nano))
	}

	pub fn midnight() -> Self {
		Self {
			nanos_since_midnight: 0,
		}
	}

	pub fn noon() -> Self {
		Self {
			nanos_since_midnight: 12 * Self::NANOS_PER_HOUR,
		}
	}

	pub fn hour(&self) -> u32 {
		(self.nanos_since_midnight / Self::NANOS_PER_HOUR) as u32
	}

	pub fn minute(&self) -> u32 {
		((self.nanos_since_midnight % Self::NANOS_PER_HOUR) / Self::NANOS_PER_MINSVTE) as u32
	}

	pub fn second(&self) -> u32 {
		((self.nanos_since_midnight % Self::NANOS_PER_MINSVTE) / Self::NANOS_PER_SECOND) as u32
	}

	pub fn nanosecond(&self) -> u32 {
		(self.nanos_since_midnight % Self::NANOS_PER_SECOND) as u32
	}

	/// Convert to nanoseconds since midnight for storage
	pub fn to_nanos_since_midnight(&self) -> u64 {
		self.nanos_since_midnight
	}

	/// Create from nanoseconds since midnight for storage
	pub fn from_nanos_since_midnight(nanos: u64) -> Option<Self> {
		if nanos > Self::MAX_NANOS_IN_DAY {
			return None;
		}
		Some(Self {
			nanos_since_midnight: nanos,
		})
	}
}

impl Display for Time {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let hours = self.hour();
		let minutes = self.minute();
		let seconds = self.second();
		let nanos = self.nanosecond();

		write!(f, "{:02}:{:02}:{:02}.{:09}", hours, minutes, seconds, nanos)
	}
}

// Serde implementation for ISO 8601 format
impl Serialize for Time {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(&self.to_string())
	}
}

struct TimeVisitor;

impl<'de> Visitor<'de> for TimeVisitor {
	type Value = Time;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		formatter.write_str("a time in ISO 8601 format (HH:MM:SS or HH:MM:SS.nnnnnnnnn)")
	}

	fn visit_str<E>(self, value: &str) -> Result<Time, E>
	where
		E: de::Error,
	{
		// Parse ISO 8601 time format: HH:MM:SS[.nnnnnnnnn]
		let (time_part, nano_part) = if let Some(dot_pos) = value.find('.') {
			(&value[..dot_pos], Some(&value[dot_pos + 1..]))
		} else {
			(value, None)
		};

		let time_parts: Vec<&str> = time_part.split(':').collect();
		if time_parts.len() != 3 {
			return Err(E::custom(format!("invalid time format: {}", value)));
		}

		let hour = time_parts[0]
			.parse::<u32>()
			.map_err(|_| E::custom(format!("invalid hour: {}", time_parts[0])))?;
		let minute = time_parts[1]
			.parse::<u32>()
			.map_err(|_| E::custom(format!("invalid minute: {}", time_parts[1])))?;
		let second = time_parts[2]
			.parse::<u32>()
			.map_err(|_| E::custom(format!("invalid second: {}", time_parts[2])))?;

		let nano = if let Some(nano_str) = nano_part {
			// Pad or truncate to 9 digits
			let padded = if nano_str.len() < 9 {
				format!("{:0<9}", nano_str)
			} else {
				nano_str[..9].to_string()
			};
			padded.parse::<u32>().map_err(|_| E::custom(format!("invalid nanoseconds: {}", nano_str)))?
		} else {
			0
		};

		Time::new(hour, minute, second, nano).ok_or_else(|| {
			E::custom(format!("invalid time: {:02}:{:02}:{:02}.{:09}", hour, minute, second, nano))
		})
	}
}

impl<'de> Deserialize<'de> for Time {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_str(TimeVisitor)
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
			let time = Time::new(14, minute, 45, 123456789).unwrap();
			let expected = format!("14:{:02}:45.123456789", minute);
			assert_eq!(format!("{}", time), expected);
		}
	}

	#[test]
	fn test_time_display_all_seconds() {
		for second in 0..60 {
			let time = Time::new(14, 30, second, 123456789).unwrap();
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
		let time = Time::from_nanos_since_midnight(1_000_000_000).unwrap();
		assert_eq!(format!("{}", time), "00:00:01.000000000");

		// Test 1 minute
		let time = Time::from_nanos_since_midnight(60_000_000_000).unwrap();
		assert_eq!(format!("{}", time), "00:01:00.000000000");

		// Test 1 hour
		let time = Time::from_nanos_since_midnight(3_600_000_000_000).unwrap();
		assert_eq!(format!("{}", time), "01:00:00.000000000");

		// Test complex time with nanoseconds
		let nanos = 14 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000 + 45 * 1_000_000_000 + 123456789;
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

	#[test]
	fn test_invalid_times() {
		assert!(Time::new(24, 0, 0, 0).is_none()); // Invalid hour
		assert!(Time::new(0, 60, 0, 0).is_none()); // Invalid minute
		assert!(Time::new(0, 0, 60, 0).is_none()); // Invalid second
		assert!(Time::new(0, 0, 0, 1_000_000_000).is_none()); // Invalid nanosecond
	}

	#[test]
	fn test_time_roundtrip() {
		let test_times = [(0, 0, 0, 0), (12, 30, 45, 123456789), (23, 59, 59, 999999999)];

		for (h, m, s, n) in test_times {
			let time = Time::new(h, m, s, n).unwrap();
			let nanos = time.to_nanos_since_midnight();
			let recovered = Time::from_nanos_since_midnight(nanos).unwrap();

			assert_eq!(time.hour(), recovered.hour());
			assert_eq!(time.minute(), recovered.minute());
			assert_eq!(time.second(), recovered.second());
			assert_eq!(time.nanosecond(), recovered.nanosecond());
		}
	}

	#[test]
	fn test_serde_roundtrip() {
		let time = Time::new(14, 30, 45, 123456789).unwrap();
		let json = serde_json::to_string(&time).unwrap();
		assert_eq!(json, "\"14:30:45.123456789\"");

		let recovered: Time = serde_json::from_str(&json).unwrap();
		assert_eq!(time, recovered);
	}
}
