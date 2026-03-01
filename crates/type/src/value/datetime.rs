// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

use crate::value::{date::Date, time::Time};

/// A date and time value with nanosecond precision.
/// Always in SVTC timezone.
///
/// Internally stored as seconds and nanoseconds since Unix epoch.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DateTime {
	// Seconds since Unix epoch (can be negative for dates before 1970)
	seconds: i64,
	// Nanosecond part (0 to 999_999_999)
	nanos: u32,
}

impl Default for DateTime {
	fn default() -> Self {
		Self {
			seconds: 0,
			nanos: 0,
		} // 1970-01-01T00:00:00.000000000Z
	}
}

impl DateTime {
	pub fn new(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
		// Validate date
		let date = Date::new(year, month, day)?;

		// Validate time
		let time = Time::new(hour, min, sec, nano)?;

		// Convert date to seconds since epoch
		let days = date.to_days_since_epoch() as i64;
		let date_seconds = days * 86400;

		// Convert time to seconds and nanos
		let time_nanos = time.to_nanos_since_midnight();
		let time_seconds = (time_nanos / 1_000_000_000) as i64;
		let time_nano_part = (time_nanos % 1_000_000_000) as u32;

		Some(Self {
			seconds: date_seconds + time_seconds,
			nanos: time_nano_part,
		})
	}

	pub fn from_ymd_hms(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Result<Self, String> {
		Self::new(year, month, day, hour, min, sec, 0).ok_or_else(|| {
			format!("Invalid datetime: {}-{:02}-{:02} {:02}:{:02}:{:02}", year, month, day, hour, min, sec)
		})
	}

	pub fn from_timestamp(timestamp: i64) -> Result<Self, String> {
		Ok(Self {
			seconds: timestamp,
			nanos: 0,
		})
	}

	pub fn from_timestamp_millis(millis: u64) -> Self {
		let seconds = (millis / 1000) as i64;
		let nanos = ((millis % 1000) * 1_000_000) as u32;
		Self {
			seconds,
			nanos,
		}
	}

	pub fn from_timestamp_nanos(nanos: u128) -> Self {
		let seconds = (nanos / 1_000_000_000) as i64;
		let nanos = (nanos % 1_000_000_000) as u32;

		Self {
			seconds,
			nanos,
		}
	}

	pub fn timestamp(&self) -> i64 {
		self.seconds
	}

	pub fn timestamp_millis(&self) -> i64 {
		self.seconds * 1000 + (self.nanos / 1_000_000) as i64
	}

	pub fn timestamp_nanos(&self) -> i64 {
		self.seconds.saturating_mul(1_000_000_000).saturating_add(self.nanos as i64)
	}

	pub fn date(&self) -> Date {
		// Convert seconds to days
		let days = (self.seconds / 86400) as i32;
		Date::from_days_since_epoch(days).unwrap()
	}

	pub fn time(&self) -> Time {
		// Get the time portion of the day
		let seconds_in_day = self.seconds % 86400;
		let seconds_in_day = if seconds_in_day < 0 {
			seconds_in_day + 86400
		} else {
			seconds_in_day
		} as u64;

		let nanos_in_day = seconds_in_day * 1_000_000_000 + self.nanos as u64;
		Time::from_nanos_since_midnight(nanos_in_day).unwrap()
	}

	/// Convert to nanoseconds since Unix epoch for storage
	pub fn to_nanos_since_epoch(&self) -> i64 {
		self.timestamp_nanos()
	}

	/// Create from nanoseconds since Unix epoch for storage
	pub fn from_nanos_since_epoch(nanos: i64) -> Self {
		let seconds = nanos / 1_000_000_000;
		let nano_part = nanos % 1_000_000_000;

		// Handle negative nanoseconds
		let (seconds, nanos) = if nanos < 0 && nano_part != 0 {
			(seconds - 1, (1_000_000_000 - nano_part.abs()) as u32)
		} else {
			(seconds, nano_part.abs() as u32)
		};

		Self {
			seconds,
			nanos,
		}
	}

	/// Create from separate seconds and nanoseconds
	pub fn from_parts(seconds: i64, nanos: u32) -> Result<Self, String> {
		if nanos >= 1_000_000_000 {
			return Err(format!("Invalid nanoseconds: {} (must be < 1_000_000_000)", nanos));
		}
		Ok(Self {
			seconds,
			nanos,
		})
	}

	/// Get separate seconds and nanoseconds for storage
	pub fn to_parts(&self) -> (i64, u32) {
		(self.seconds, self.nanos)
	}

	/// Get year component
	pub fn year(&self) -> i32 {
		self.date().year()
	}

	/// Get month component (1-12)
	pub fn month(&self) -> u32 {
		self.date().month()
	}

	/// Get day component (1-31)
	pub fn day(&self) -> u32 {
		self.date().day()
	}

	/// Get hour component (0-23)
	pub fn hour(&self) -> u32 {
		self.time().hour()
	}

	/// Get minute component (0-59)
	pub fn minute(&self) -> u32 {
		self.time().minute()
	}

	/// Get second component (0-59)
	pub fn second(&self) -> u32 {
		self.time().second()
	}

	/// Get nanosecond component (0-999_999_999)
	pub fn nanosecond(&self) -> u32 {
		self.time().nanosecond()
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

		// Handle negative years
		let (year_str, month_str, day_str) = if date_parts[0].is_empty() && date_parts.len() == 4 {
			// Negative year case
			(format!("-{}", date_parts[1]), date_parts[2], date_parts[3])
		} else {
			(date_parts[0].to_string(), date_parts[1], date_parts[2])
		};

		let year = year_str.parse::<i32>().map_err(|_| E::custom(format!("invalid year: {}", year_str)))?;
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
	use serde_json::{from_str, to_string};

	use super::*;

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
		// Test various millisecond values
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123000000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.123000000Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 001000000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.001000000Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 999000000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.999000000Z");
	}

	#[test]
	fn test_datetime_display_microsecond_precision() {
		// Test various microsecond values
		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 123456000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.123456000Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 000001000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.000001000Z");

		let datetime = DateTime::new(2024, 3, 15, 14, 30, 45, 999999000).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T14:30:45.999999000Z");
	}

	#[test]
	fn test_datetime_display_nanosecond_precision() {
		// Test various nanosecond values
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
		// Very early date
		let datetime = DateTime::new(1, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "0001-01-01T00:00:00.000000000Z");

		// Far future date
		let datetime = DateTime::new(9999, 12, 31, 23, 59, 59, 999999999).unwrap();
		assert_eq!(format!("{}", datetime), "9999-12-31T23:59:59.999999999Z");

		// Century boundaries
		let datetime = DateTime::new(1900, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "1900-01-01T00:00:00.000000000Z");

		let datetime = DateTime::new(2000, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2000-01-01T00:00:00.000000000Z");

		let datetime = DateTime::new(2100, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2100-01-01T00:00:00.000000000Z");
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
		let datetime = DateTime::from_timestamp_millis(1234567890123);
		assert_eq!(format!("{}", datetime), "2009-02-13T23:31:30.123000000Z");

		let datetime = DateTime::from_timestamp_millis(0);
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:00.000000000Z");
	}

	#[test]
	fn test_datetime_display_from_parts() {
		let datetime = DateTime::from_parts(1234567890, 123456789).unwrap();
		assert_eq!(format!("{}", datetime), "2009-02-13T23:31:30.123456789Z");

		let datetime = DateTime::from_parts(0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:00.000000000Z");
	}

	#[test]
	fn test_datetime_roundtrip() {
		let test_cases = [
			(1970, 1, 1, 0, 0, 0, 0),
			(2024, 3, 15, 14, 30, 45, 123456789),
			(2000, 2, 29, 23, 59, 59, 999999999),
		];

		for (y, m, d, h, min, s, n) in test_cases {
			let datetime = DateTime::new(y, m, d, h, min, s, n).unwrap();
			let nanos = datetime.to_nanos_since_epoch();
			let recovered = DateTime::from_nanos_since_epoch(nanos);

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
}
