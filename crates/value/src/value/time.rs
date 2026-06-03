// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

use crate::{
	error::{TemporalKind, TypeError},
	fragment::Fragment,
	value::duration::Duration,
};

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Time {
	nanos_since_midnight: u64,
}

impl Time {
	const MAX_NANOS_IN_DAY: u64 = 86_399_999_999_999;
	const NANOS_PER_SECOND: u64 = 1_000_000_000;
	const NANOS_PER_MINSVTE: u64 = 60 * Self::NANOS_PER_SECOND;
	const NANOS_PER_HOUR: u64 = 60 * Self::NANOS_PER_MINSVTE;

	fn overflow_err(message: impl Into<String>) -> TypeError {
		TypeError::Temporal {
			kind: TemporalKind::TimeOverflow {
				message: message.into(),
			},
			message: "time overflow".to_string(),
			fragment: Fragment::None,
		}
	}

	pub fn new(hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
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

	pub fn from_hms(hour: u32, min: u32, sec: u32) -> Result<Self, Box<TypeError>> {
		Self::new(hour, min, sec, 0).ok_or_else(|| {
			Box::new(Self::overflow_err(format!("invalid time: {:02}:{:02}:{:02}", hour, min, sec)))
		})
	}

	pub fn from_hms_nano(hour: u32, min: u32, sec: u32, nano: u32) -> Result<Self, Box<TypeError>> {
		Self::new(hour, min, sec, nano).ok_or_else(|| {
			Box::new(Self::overflow_err(format!(
				"invalid time: {:02}:{:02}:{:02}.{:09}",
				hour, min, sec, nano
			)))
		})
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

	pub fn to_nanos_since_midnight(&self) -> u64 {
		self.nanos_since_midnight
	}

	pub fn from_nanos_since_midnight(nanos: u64) -> Option<Self> {
		if nanos > Self::MAX_NANOS_IN_DAY {
			return None;
		}
		Some(Self {
			nanos_since_midnight: nanos,
		})
	}

	pub fn saturating_add(self, rhs: Duration) -> Time {
		let total = rhs.as_nanos().unwrap_or(if rhs.is_negative() {
			i64::MIN
		} else {
			i64::MAX
		});
		let nanos =
			(self.nanos_since_midnight as i128 + total as i128).clamp(0, Self::MAX_NANOS_IN_DAY as i128);
		Self {
			nanos_since_midnight: nanos as u64,
		}
	}

	pub fn saturating_sub(self, rhs: Duration) -> Time {
		let total = rhs.as_nanos().unwrap_or(if rhs.is_negative() {
			i64::MIN
		} else {
			i64::MAX
		});
		let nanos =
			(self.nanos_since_midnight as i128 - total as i128).clamp(0, Self::MAX_NANOS_IN_DAY as i128);
		Self {
			nanos_since_midnight: nanos as u64,
		}
	}
}

impl Display for Time {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let hours = self.hour();
		let minutes = self.minute();
		let seconds = self.second();
		let nanos = self.nanosecond();

		write!(f, "{:02}:{:02}:{:02}.{:09}", hours, minutes, seconds, nanos)
	}
}

impl Serialize for Time {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.nanos_since_midnight)
	}
}

struct TimeVisitor;

impl<'de> Visitor<'de> for TimeVisitor {
	type Value = Time;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("a time as nanoseconds since midnight (u64)")
	}

	fn visit_u64<E>(self, value: u64) -> Result<Time, E>
	where
		E: de::Error,
	{
		Time::from_nanos_since_midnight(value)
			.ok_or_else(|| E::custom(format!("time nanoseconds out of range: {}", value)))
	}
}

impl<'de> Deserialize<'de> for Time {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_u64(TimeVisitor)
	}
}

#[cfg(test)]
pub mod tests {
	use std::fmt::Debug;

	use postcard::{from_bytes, to_allocvec};
	use serde_json::{from_str, to_string};

	use super::*;
	use crate::error::{TemporalKind, TypeError};

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
		let json = to_string(&time).unwrap();
		// Wire format is the raw nanos-since-midnight integer, not an ISO-8601 string.
		assert_eq!(json, time.to_nanos_since_midnight().to_string());

		let recovered: Time = from_str(&json).unwrap();
		assert_eq!(time, recovered);
	}

	#[test]
	fn test_serde_postcard_roundtrip_preserves_all_fields() {
		// Binary (postcard) is the hot CDC path; verify every component survives the integer encoding.
		for (h, m, s, n) in [(0u32, 0u32, 0u32, 0u32), (14, 30, 45, 123456789), (23, 59, 59, 999999999)] {
			let time = Time::new(h, m, s, n).unwrap();
			let bytes = to_allocvec(&time).unwrap();
			let recovered: Time = from_bytes(&bytes).unwrap();
			assert_eq!(time, recovered);
			assert_eq!(recovered.hour(), h);
			assert_eq!(recovered.minute(), m);
			assert_eq!(recovered.second(), s);
			assert_eq!(recovered.nanosecond(), n);
		}
	}

	#[test]
	fn test_deserialize_rejects_out_of_range_nanos() {
		// Nanos beyond the last instant of the day must not decode to a Time.
		let json = (Time::MAX_NANOS_IN_DAY + 1).to_string();
		assert!(from_str::<Time>(&json).is_err());
	}

	fn assert_time_overflow<T: Debug>(result: Result<T, Box<TypeError>>) {
		let err = result.expect_err("expected TimeOverflow error");
		match *err {
			TypeError::Temporal {
				kind: TemporalKind::TimeOverflow {
					..
				},
				..
			} => {}
			other => panic!("expected TimeOverflow, got: {:?}", other),
		}
	}

	#[test]
	fn test_from_hms_invalid_hour() {
		assert_time_overflow(Time::from_hms(24, 0, 0));
	}

	#[test]
	fn test_from_hms_invalid_minute() {
		assert_time_overflow(Time::from_hms(0, 60, 0));
	}

	#[test]
	fn test_from_hms_invalid_second() {
		assert_time_overflow(Time::from_hms(0, 0, 60));
	}

	#[test]
	fn test_from_hms_nano_invalid_nano() {
		assert_time_overflow(Time::from_hms_nano(0, 0, 0, 1_000_000_000));
	}

	#[test]
	fn saturating_sub_before_midnight_clamps_to_midnight() {
		// Subtracting past 00:00 clamps to midnight rather than wrapping or panicking.
		let midnight = Time::midnight();
		assert_eq!(midnight.saturating_sub(Duration::from_seconds(1).unwrap()), midnight);
	}

	#[test]
	fn saturating_add_past_end_of_day_clamps_to_max() {
		// Adding past 23:59:59.999999999 clamps to the last representable instant of the day.
		let late = Time::from_hms(23, 59, 59).unwrap();
		let max = Time::from_nanos_since_midnight(86_399_999_999_999).unwrap();
		assert_eq!(late.saturating_add(Duration::from_seconds(3600).unwrap()), max);
	}

	#[test]
	fn saturating_add_sub_within_day() {
		let t = Time::from_hms(10, 30, 0).unwrap();
		assert_eq!(t.saturating_add(Duration::from_seconds(60).unwrap()), Time::from_hms(10, 31, 0).unwrap());
		assert_eq!(t.saturating_sub(Duration::from_seconds(60).unwrap()), Time::from_hms(10, 29, 0).unwrap());
	}
}
