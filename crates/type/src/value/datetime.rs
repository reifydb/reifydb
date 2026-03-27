// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

use crate::{
	error::{TemporalKind, TypeError},
	fragment::Fragment,
	value::{date::Date, duration::Duration, time::Time},
};

const NANOS_PER_SECOND: u64 = 1_000_000_000;
const NANOS_PER_MILLI: u64 = 1_000_000;
const NANOS_PER_DAY: u64 = 86_400 * NANOS_PER_SECOND;

/// A date and time value with nanosecond precision.
/// Always in SVTC timezone.
///
/// Internally stored as nanoseconds since Unix epoch (1970-01-01T00:00:00Z).
/// Only supports dates from 1970-01-01 onward.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DateTime {
	nanos: u64,
}

impl Default for DateTime {
	fn default() -> Self {
		Self {
			nanos: 0,
		} // 1970-01-01T00:00:00.000000000Z
	}
}

impl DateTime {
	/// Create from year, month, day, hour, minute, second, nanosecond.
	/// Returns None if the date is invalid or before Unix epoch.
	pub fn new(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
		let date = Date::new(year, month, day)?;
		let time = Time::new(hour, min, sec, nano)?;

		let days = date.to_days_since_epoch();
		if days < 0 {
			return None; // Before Unix epoch
		}

		let nanos = (days as u64).checked_mul(NANOS_PER_DAY)?.checked_add(time.to_nanos_since_midnight())?;
		Some(Self {
			nanos,
		})
	}

	pub fn from_ymd_hms(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Result<Self, TypeError> {
		Self::new(year, month, day, hour, min, sec, 0).ok_or_else(|| {
			Self::overflow_err(format!(
				"invalid datetime: {}-{:02}-{:02} {:02}:{:02}:{:02}",
				year, month, day, hour, min, sec
			))
		})
	}

	fn overflow_err(message: impl Into<String>) -> TypeError {
		TypeError::Temporal {
			kind: TemporalKind::DateTimeOverflow {
				message: message.into(),
			},
			message: "datetime overflow".to_string(),
			fragment: Fragment::None,
		}
	}

	/// Create from a primary u64 nanoseconds value.
	/// Values beyond MAX_SAFE_NANOS are rejected to prevent downstream i32 overflow in date().
	pub fn from_nanos(nanos: u64) -> Self {
		Self {
			nanos,
		}
	}

	/// Get the raw nanoseconds since epoch.
	pub fn to_nanos(&self) -> u64 {
		self.nanos
	}

	pub fn from_timestamp(timestamp: i64) -> Result<Self, TypeError> {
		if timestamp < 0 {
			return Err(Self::overflow_err(format!(
				"DateTime does not support timestamps before Unix epoch: {}",
				timestamp
			)));
		}
		let nanos = (timestamp as u64).checked_mul(NANOS_PER_SECOND).ok_or_else(|| {
			Self::overflow_err(format!("timestamp {} overflows DateTime range", timestamp))
		})?;
		Ok(Self {
			nanos,
		})
	}

	pub fn from_timestamp_millis(millis: u64) -> Result<Self, TypeError> {
		let nanos = millis.checked_mul(NANOS_PER_MILLI).ok_or_else(|| {
			Self::overflow_err(format!("timestamp_millis {} overflows DateTime range", millis))
		})?;
		Ok(Self {
			nanos,
		})
	}

	pub fn from_timestamp_nanos(nanos: u128) -> Result<Self, TypeError> {
		let nanos = u64::try_from(nanos).map_err(|_| {
			Self::overflow_err(format!("timestamp_nanos {} overflows u64 DateTime range", nanos))
		})?;
		Ok(Self {
			nanos,
		})
	}

	pub fn timestamp(&self) -> i64 {
		(self.nanos / NANOS_PER_SECOND) as i64
	}

	pub fn timestamp_millis(&self) -> i64 {
		(self.nanos / NANOS_PER_MILLI) as i64
	}

	pub fn timestamp_nanos(&self) -> Result<i64, TypeError> {
		i64::try_from(self.nanos).map_err(|_| Self::overflow_err("DateTime nanos exceeds i64::MAX"))
	}

	pub fn try_date(&self) -> Result<Date, TypeError> {
		let days_u64 = self.nanos / NANOS_PER_DAY;
		let days = i32::try_from(days_u64)
			.map_err(|_| Self::overflow_err("DateTime nanos too large for date extraction"))?;
		Date::from_days_since_epoch(days)
			.ok_or_else(|| Self::overflow_err("DateTime days out of range for Date"))
	}

	pub fn date(&self) -> Date {
		self.try_date().expect("DateTime nanos too large for date extraction")
	}

	pub fn time(&self) -> Time {
		let nanos_in_day = self.nanos % NANOS_PER_DAY;
		Time::from_nanos_since_midnight(nanos_in_day).unwrap()
	}

	/// Convert to nanoseconds since Unix epoch as u128.
	pub fn to_nanos_since_epoch_u128(&self) -> u128 {
		self.nanos as u128
	}

	pub fn year(&self) -> i32 {
		self.date().year()
	}

	pub fn month(&self) -> u32 {
		self.date().month()
	}

	pub fn day(&self) -> u32 {
		self.date().day()
	}

	pub fn hour(&self) -> u32 {
		self.time().hour()
	}

	pub fn minute(&self) -> u32 {
		self.time().minute()
	}

	pub fn second(&self) -> u32 {
		self.time().second()
	}

	pub fn nanosecond(&self) -> u32 {
		self.time().nanosecond()
	}

	/// Add a Duration to this DateTime, handling calendar arithmetic for months/days.
	pub fn add_duration(&self, dur: &Duration) -> Result<Self, TypeError> {
		let date = self.date();
		let time = self.time();
		let mut year = date.year();
		let mut month = date.month() as i32;
		let mut day = date.day();

		// Add months component
		let total_months = month + dur.get_months();
		year += (total_months - 1).div_euclid(12);
		month = (total_months - 1).rem_euclid(12) + 1;

		// Clamp day to valid range for the new month
		let max_day = Date::days_in_month(year, month as u32);
		if day > max_day {
			day = max_day;
		}

		// Convert to nanos since epoch and add day/nanos components
		let base_date = Date::new(year, month as u32, day).ok_or_else(|| {
			Self::overflow_err(format!(
				"invalid date after adding duration: {}-{:02}-{:02}",
				year, month, day
			))
		})?;
		let base_days = base_date.to_days_since_epoch() as i64 + dur.get_days() as i64;
		let time_nanos = time.to_nanos_since_midnight() as i64 + dur.get_nanos();

		let total_nanos = base_days as i128 * 86_400_000_000_000i128 + time_nanos as i128;

		if total_nanos < 0 {
			return Err(Self::overflow_err("result is before Unix epoch"));
		}

		let nanos =
			u64::try_from(total_nanos).map_err(|_| Self::overflow_err("result exceeds DateTime range"))?;
		Ok(Self {
			nanos,
		})
	}
}

impl Display for DateTime {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let date = self.date();
		let time = self.time();

		// Format as ISO 8601: YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ
		write!(f, "{}T{}Z", date, time)
	}
}

// Serde implementation for ISO 8601 format
impl Serialize for DateTime {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(&self.to_string())
	}
}

struct DateTimeVisitor;

impl<'de> Visitor<'de> for DateTimeVisitor {
	type Value = DateTime;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("a datetime in ISO 8601 format (YYYY-MM-DDTHH:MM:SS[.nnnnnnnnn]Z)")
	}

	fn visit_str<E>(self, value: &str) -> Result<DateTime, E>
	where
		E: de::Error,
	{
		// Parse ISO 8601 datetime format:
		// YYYY-MM-DDTHH:MM:SS[.nnnnnnnnn]Z Remove trailing Z if
		// present
		let value = value.strip_suffix('Z').unwrap_or(value);

		// Split on T
		let parts: Vec<&str> = value.split('T').collect();
		if parts.len() != 2 {
			return Err(E::custom(format!("invalid datetime format: {}", value)));
		}

		// Parse date part
		let date_parts: Vec<&str> = parts[0].split('-').collect();
		if date_parts.len() != 3 {
			return Err(E::custom(format!("invalid date format: {}", parts[0])));
		}

		let (year_str, month_str, day_str) = (date_parts[0], date_parts[1], date_parts[2]);

		let year = year_str.parse::<i32>().map_err(|_| E::custom(format!("invalid year: {}", year_str)))?;
		if year < 1970 {
			return Err(E::custom(format!("DateTime does not support pre-epoch years: {}", year)));
		}
		let month = month_str.parse::<u32>().map_err(|_| E::custom(format!("invalid month: {}", month_str)))?;
		let day = day_str.parse::<u32>().map_err(|_| E::custom(format!("invalid day: {}", day_str)))?;

		// Parse time part
		let (time_part, nano_part) = if let Some(dot_pos) = parts[1].find('.') {
			(&parts[1][..dot_pos], Some(&parts[1][dot_pos + 1..]))
		} else {
			(parts[1], None)
		};

		let time_parts: Vec<&str> = time_part.split(':').collect();
		if time_parts.len() != 3 {
			return Err(E::custom(format!("invalid time format: {}", parts[1])));
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

		DateTime::new(year, month, day, hour, minute, second, nano).ok_or_else(|| {
			E::custom(format!(
				"invalid datetime: {}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
				year, month, day, hour, minute, second, nano
			))
		})
	}
}

impl<'de> Deserialize<'de> for DateTime {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_str(DateTimeVisitor)
	}
}

#[cfg(test)]
pub mod tests {
	use std::fmt::Debug;

	use serde_json::{from_str, to_string};

	use crate::{
		error::{TemporalKind, TypeError},
		value::{
			datetime::{DateTime, NANOS_PER_DAY},
			duration::Duration,
		},
	};

	#[test]
	fn test_datetime_display_standard_format() {
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123456789).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.123456789Z");

		let datetime = DateTime::new(2000, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2000-01-01T00:00:00.000000000Z");

		let datetime = DateTime::new(1999, 12, 31, 23, 59, 59, 999999999).unwrap();
		assert_eq!(format!("{}", datetime), "1999-12-31T23:59:59.999999999Z");
	}

	#[test]
	fn test_datetime_display_millisecond_precision() {
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123000000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.123000000Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 001000000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.001000000Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 999000000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.999000000Z");
	}

	#[test]
	fn test_datetime_display_microsecond_precision() {
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123456000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.123456000Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 000001000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.000001000Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 999999000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.999999000Z");
	}

	#[test]
	fn test_datetime_display_nanosecond_precision() {
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123456789).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.123456789Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 000000001).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.000000001Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 999999999).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.999999999Z");
	}

	#[test]
	fn test_datetime_display_zero_fractional_seconds() {
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.000000000Z");

		let datetime = DateTime::new(2024, 3, 15, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T00:00:00.000000000Z");
	}

	#[test]
	fn test_datetime_display_edge_times() {
		// Midnight
		let datetime = DateTime::new(2024, 3, 15, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T00:00:00.000000000Z");

		// Almost midnight next day
		let datetime = DateTime::new(2024, 3, 15, 23, 59, 59, 999999999).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T23:59:59.999999999Z");

		// Noon
		let datetime = DateTime::new(2024, 3, 15, 12, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T12:00:00.000000000Z");
	}

	#[test]
	fn test_datetime_display_unix_epoch() {
		let datetime = DateTime::new(1970, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:00.000000000Z");

		let datetime = DateTime::new(1970, 1, 1, 0, 0, 1, 0).unwrap();
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:01.000000000Z");
	}

	#[test]
	fn test_datetime_display_leap_year() {
		let datetime = DateTime::new(2024, 2, 29, 12, 30, 45, 123456789).unwrap();
		assert_eq!(format!("{}", datetime), "2024-02-29T12:30:45.123456789Z");

		let datetime = DateTime::new(2000, 2, 29, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2000-02-29T00:00:00.000000000Z");
	}

	#[test]
	fn test_datetime_display_boundary_dates() {
		// Century boundaries
		let datetime = DateTime::new(2000, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2000-01-01T00:00:00.000000000Z");

		let datetime = DateTime::new(2100, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2100-01-01T00:00:00.000000000Z");

		// Max representable date (~year 2554 with u64 nanos)
		let datetime = DateTime::new(2554, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2554-01-01T00:00:00.000000000Z");

		// Year 9999 exceeds u64 nanos range
		assert!(DateTime::new(9999, 12, 31, 23, 59, 59, 999999999).is_none());
	}

	#[test]
	fn test_datetime_rejects_pre_epoch() {
		// Year 1 is before epoch
		assert!(DateTime::new(1, 1, 1, 0, 0, 0, 0).is_none());

		// 1900 is before epoch
		assert!(DateTime::new(1900, 1, 1, 0, 0, 0, 0).is_none());

		// 1969 is before epoch
		assert!(DateTime::new(1969, 12, 31, 23, 59, 59, 999999999).is_none());

		// Negative timestamp
		assert!(DateTime::from_timestamp(-1).is_err());
	}

	#[test]
	fn test_datetime_display_default() {
		let datetime = DateTime::default();
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:00.000000000Z");
	}

	#[test]
	fn test_datetime_display_all_hours() {
		for hour in 0..24 {
			let datetime = DateTime::new(2024, 3, 15, hour, 30, 45, 123456789).unwrap();
			let expected = format!("2024-03-15T{:02}:30:45.123456789Z", hour);
			assert_eq!(format!("{}", datetime), expected);
		}
	}

	#[test]
	fn test_datetime_display_all_minutes() {
		for minute in 0..60 {
			let datetime = DateTime::new(2024, 3, 15, 14, minute, 45, 123456789).unwrap();
			let expected = format!("2024-03-15T14:{:02}:45.123456789Z", minute);
			assert_eq!(format!("{}", datetime), expected);
		}
	}

	#[test]
	fn test_datetime_display_all_seconds() {
		for second in 0..60 {
			let datetime = DateTime::new(2024, 3, 15, 14, 30, second, 123456789).unwrap();
			let expected = format!("2024-03-15T14:30:{:02}.123456789Z", second);
			assert_eq!(format!("{}", datetime), expected);
		}
	}

	#[test]
	fn test_datetime_display_from_timestamp() {
		let datetime = DateTime::from_timestamp(0).unwrap();
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:00.000000000Z");

		let datetime = DateTime::from_timestamp(1234567890).unwrap();
		assert_eq!(format!("{}", datetime), "2009-02-13T23:31:30.000000000Z");
	}

	#[test]
	fn test_datetime_display_from_timestamp_millis() {
		let datetime = DateTime::from_timestamp_millis(1234567890123).unwrap();
		assert_eq!(format!("{}", datetime), "2009-02-13T23:31:30.123000000Z");

		let datetime = DateTime::from_timestamp_millis(0).unwrap();
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:00.000000000Z");
	}

	#[test]
	fn test_datetime_from_nanos_roundtrip() {
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123456789).unwrap();
		let nanos = datetime.to_nanos();
		let recovered = DateTime::from_nanos(nanos);
		assert_eq!(datetime, recovered);
	}

	#[test]
	fn test_datetime_roundtrip() {
		let test_cases = [
			(1970, 1, 1, 0, 0, 0, 0u32),
			(2024, 3, 15, 14, 30, 45, 123456789),
			(2000, 2, 29, 23, 59, 59, 999999999),
		];

		for (y, m, d, h, min, s, n) in test_cases {
			let datetime = DateTime::new(y, m, d, h, min, s, n).unwrap();
			let nanos = datetime.to_nanos();
			let recovered = DateTime::from_nanos(nanos);

			assert_eq!(datetime.year(), recovered.year());
			assert_eq!(datetime.month(), recovered.month());
			assert_eq!(datetime.day(), recovered.day());
			assert_eq!(datetime.hour(), recovered.hour());
			assert_eq!(datetime.minute(), recovered.minute());
			assert_eq!(datetime.second(), recovered.second());
			assert_eq!(datetime.nanosecond(), recovered.nanosecond());
		}
	}

	#[test]
	fn test_datetime_components() {
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123456789).unwrap();

		assert_eq!(datetime.year(), 2024);
		assert_eq!(datetime.month(), 3);
		assert_eq!(datetime.day(), 15);
		assert_eq!(datetime.hour(), 14);
		assert_eq!(datetime.minute(), 30);
		assert_eq!(datetime.second(), 45);
		assert_eq!(datetime.nanosecond(), 123456789);
	}

	#[test]
	fn test_serde_roundtrip() {
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123456789).unwrap();
		let json = to_string(&datetime).unwrap();
		assert_eq!(json, "\"2024-03-15T14:30:45.123456789Z\"");

		let recovered: DateTime = from_str(&json).unwrap();
		assert_eq!(datetime, recovered);
	}

	fn assert_datetime_overflow<T: Debug>(result: Result<T, TypeError>) {
		let err = result.expect_err("expected DateTimeOverflow error");
		match err {
			TypeError::Temporal {
				kind: TemporalKind::DateTimeOverflow {
					..
				},
				..
			} => {}
			other => panic!("expected DateTimeOverflow, got: {:?}", other),
		}
	}

	#[test]
	fn test_from_timestamp_nanos_overflow() {
		let huge: u128 = u64::MAX as u128 + 1;
		assert_datetime_overflow(DateTime::from_timestamp_nanos(huge));
	}

	#[test]
	fn test_from_timestamp_nanos_max_u64_ok() {
		let dt = DateTime::from_timestamp_nanos(u64::MAX as u128).unwrap();
		assert_eq!(dt.to_nanos(), u64::MAX);
	}

	#[test]
	fn test_from_timestamp_large_value_overflow() {
		assert_datetime_overflow(DateTime::from_timestamp(i64::MAX));
	}

	#[test]
	fn test_from_timestamp_negative_overflow() {
		assert_datetime_overflow(DateTime::from_timestamp(-1));
	}

	#[test]
	fn test_from_timestamp_millis_overflow() {
		assert_datetime_overflow(DateTime::from_timestamp_millis(u64::MAX));
	}

	#[test]
	fn test_from_timestamp_millis_boundary_ok() {
		let dt = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
		assert!(dt.to_nanos() > 0);
	}

	#[test]
	fn test_timestamp_nanos_large_value_returns_err() {
		let dt = DateTime::from_nanos(i64::MAX as u64 + 1);
		assert_datetime_overflow(dt.timestamp_nanos());
	}

	#[test]
	fn test_timestamp_nanos_within_range_ok() {
		let dt = DateTime::from_nanos(i64::MAX as u64);
		assert_eq!(dt.timestamp_nanos().unwrap(), i64::MAX);
	}

	#[test]
	fn test_try_date_max_nanos_ok() {
		// u64::MAX nanos / NANOS_PER_DAY = 213_503 which fits in i32
		let dt = DateTime::from_nanos(u64::MAX);
		let date = dt.try_date().unwrap();
		assert!(date.year() > 2500);
	}

	#[test]
	fn test_add_duration_overflow() {
		let dt = DateTime::from_nanos(u64::MAX - 1);
		let dur = Duration::from_days(1).unwrap();
		assert_datetime_overflow(dt.add_duration(&dur));
	}

	#[test]
	fn test_add_duration_before_epoch() {
		let dt = DateTime::new(1970, 1, 1, 0, 0, 0, 0).unwrap();
		let dur = Duration::from_seconds(-1).unwrap();
		assert_datetime_overflow(dt.add_duration(&dur));
	}

	#[test]
	fn test_add_duration_negative_nanos_borrows_from_days() {
		let dt = DateTime::new(2024, 3, 15, 0, 0, 30, 0).unwrap();
		let dur = Duration::from_seconds(-60).unwrap();
		let result = dt.add_duration(&dur).unwrap();
		assert_eq!(result.year(), 2024);
		assert_eq!(result.month(), 3);
		assert_eq!(result.day(), 14);
		assert_eq!(result.hour(), 23);
		assert_eq!(result.minute(), 59);
		assert_eq!(result.second(), 30);
	}

	#[test]
	fn test_add_duration_nanos_overflow_into_next_day() {
		let dt = DateTime::new(2024, 3, 15, 23, 59, 30, 0).unwrap();
		let dur = Duration::from_seconds(60).unwrap();
		let result = dt.add_duration(&dur).unwrap();
		assert_eq!(result.year(), 2024);
		assert_eq!(result.month(), 3);
		assert_eq!(result.day(), 16);
		assert_eq!(result.hour(), 0);
		assert_eq!(result.minute(), 0);
		assert_eq!(result.second(), 30);
	}
}
