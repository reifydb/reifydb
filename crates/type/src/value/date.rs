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
};

/// A date value representing a calendar date (year, month, day) without time
/// information. Always interpreted in SVTC.
///
/// Internally stored as days since Unix epoch (1970-01-01).
///
/// `#[repr(transparent)]` is required: the FFI ABI hands guests a borrow of
/// `Vec<Date>` storage as a contiguous `[i32]` payload. Adding a field or
/// changing the wrapping breaks the wire format.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Date {
	// Days since Unix epoch (1970-01-01)
	// Negative values represent dates before 1970
	days_since_epoch: i32,
}

// Calendar utilities
impl Date {
	/// Check if a year is a leap year
	#[inline]
	pub fn is_leap_year(year: i32) -> bool {
		(year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
	}

	/// Get the number of days in a month
	#[inline]
	pub fn days_in_month(year: i32, month: u32) -> u32 {
		match month {
			1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
			4 | 6 | 9 | 11 => 30,
			2 => {
				if Self::is_leap_year(year) {
					29
				} else {
					28
				}
			}
			_ => 0,
		}
	}

	/// Convert year/month/day to days since Unix epoch
	fn ymd_to_days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
		// Validate input
		if !(1..=12).contains(&month) || day < 1 || day > Self::days_in_month(year, month) {
			return None;
		}

		// Algorithm based on Howard Hinnant's date algorithms
		// Convert month from [1,12] to [0,11] where Mar=0
		let (y, m) = if month <= 2 {
			(year - 1, month as i32 + 9) // Jan->10, Feb->11
		} else {
			(year, month as i32 - 3) // Mar->0, Apr->1, ..., Dec->9
		};

		let era = if y >= 0 {
			y
		} else {
			y - 399
		} / 400;
		let yoe = y - era * 400; // [0, 399]
		let doy = (153 * m + 2) / 5 + day as i32 - 1; // [0, 365]
		let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
		let days = era * 146097 + doe - 719468;

		Some(days)
	}

	/// Convert days since Unix epoch to year/month/day
	fn days_since_epoch_to_ymd(days: i32) -> (i32, u32, u32) {
		// Adjust to the algorithm's epoch
		let days_since_ce = days + 719468;

		let era = if days_since_ce >= 0 {
			days_since_ce
		} else {
			days_since_ce - 146096
		} / 146097;
		let doe = days_since_ce - era * 146097; // [0, 146096]
		let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
		let y = yoe + era * 400;
		let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
		let mp = (5 * doy + 2) / 153; // [0, 11]
		let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
		let m = if mp < 10 {
			mp + 3
		} else {
			mp - 9
		}; // [1, 12]
		let year = if m <= 2 {
			y + 1
		} else {
			y
		};

		(year, m as u32, d as u32)
	}
}

impl Date {
	fn overflow_err(message: impl Into<String>) -> TypeError {
		TypeError::Temporal {
			kind: TemporalKind::DateOverflow {
				message: message.into(),
			},
			message: "date overflow".to_string(),
			fragment: Fragment::None,
		}
	}

	pub fn new(year: i32, month: u32, day: u32) -> Option<Self> {
		Self::ymd_to_days_since_epoch(year, month, day).map(|days_since_epoch| Self {
			days_since_epoch,
		})
	}

	pub fn from_ymd(year: i32, month: u32, day: u32) -> Result<Self, Box<TypeError>> {
		Self::new(year, month, day).ok_or_else(|| {
			Box::new(Self::overflow_err(format!("invalid date: {}-{:02}-{:02}", year, month, day)))
		})
	}

	pub fn year(&self) -> i32 {
		Self::days_since_epoch_to_ymd(self.days_since_epoch).0
	}

	pub fn month(&self) -> u32 {
		Self::days_since_epoch_to_ymd(self.days_since_epoch).1
	}

	pub fn day(&self) -> u32 {
		Self::days_since_epoch_to_ymd(self.days_since_epoch).2
	}

	/// Convert to days since Unix epoch for storage
	pub fn to_days_since_epoch(&self) -> i32 {
		self.days_since_epoch
	}

	/// Create from days since Unix epoch for storage
	pub fn from_days_since_epoch(days: i32) -> Option<Self> {
		// Validate the range (approximately -1 million to +1 million
		// years from 1970)
		if !(-365_250_000..=365_250_000).contains(&days) {
			return None;
		}
		Some(Self {
			days_since_epoch: days,
		})
	}
}

impl Display for Date {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let (year, month, day) = Self::days_since_epoch_to_ymd(self.days_since_epoch);
		if year < 0 {
			write!(f, "-{:04}-{:02}-{:02}", -year, month, day)
		} else {
			write!(f, "{:04}-{:02}-{:02}", year, month, day)
		}
	}
}

// Serde implementation for ISO 8601 format
impl Serialize for Date {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(&self.to_string())
	}
}

struct DateVisitor;

impl<'de> Visitor<'de> for DateVisitor {
	type Value = Date;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("a date in ISO 8601 format (YYYY-MM-DD)")
	}

	fn visit_str<E>(self, value: &str) -> Result<Date, E>
	where
		E: de::Error,
	{
		// Parse ISO 8601 date format: YYYY-MM-DD
		let parts: Vec<&str> = value.split('-').collect();

		if parts.len() != 3 {
			return Err(E::custom(format!("invalid date format: {}", value)));
		}

		// Handle negative years
		let (year_str, month_str, day_str) = if parts[0].is_empty() && parts.len() == 4 {
			// Negative year case: "-YYYY-MM-DD" splits as
			// ["", "YYYY", "MM", "DD"]
			(format!("-{}", parts[1]), parts[2], parts[3])
		} else {
			(parts[0].to_string(), parts[1], parts[2])
		};

		let year = year_str.parse::<i32>().map_err(|_| E::custom(format!("invalid year: {}", year_str)))?;
		let month = month_str.parse::<u32>().map_err(|_| E::custom(format!("invalid month: {}", month_str)))?;
		let day = day_str.parse::<u32>().map_err(|_| E::custom(format!("invalid day: {}", day_str)))?;

		Date::new(year, month, day)
			.ok_or_else(|| E::custom(format!("invalid date: {}-{:02}-{:02}", year, month, day)))
	}
}

impl<'de> Deserialize<'de> for Date {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_str(DateVisitor)
	}
}

#[cfg(test)]
pub mod tests {
	use std::fmt::Debug;

	use serde_json::{from_str, to_string};

	use super::*;
	use crate::error::{TemporalKind, TypeError};

	#[test]
	fn test_date_display_standard_dates() {
		// Standard dates
		let date = Date::new(2024, 3, 15).unwrap();
		assert_eq!(format!("{}", date), "2024-03-15");

		let date = Date::new(2000, 1, 1).unwrap();
		assert_eq!(format!("{}", date), "2000-01-01");

		let date = Date::new(1999, 12, 31).unwrap();
		assert_eq!(format!("{}", date), "1999-12-31");
	}

	#[test]
	fn test_date_display_edge_cases() {
		// Unix epoch
		let date = Date::new(1970, 1, 1).unwrap();
		assert_eq!(format!("{}", date), "1970-01-01");

		// Leap year
		let date = Date::new(2024, 2, 29).unwrap();
		assert_eq!(format!("{}", date), "2024-02-29");

		// Single digit day/month
		let date = Date::new(2024, 1, 9).unwrap();
		assert_eq!(format!("{}", date), "2024-01-09");

		let date = Date::new(2024, 9, 1).unwrap();
		assert_eq!(format!("{}", date), "2024-09-01");
	}

	#[test]
	fn test_date_display_boundary_dates() {
		// Very early date
		let date = Date::new(1, 1, 1).unwrap();
		assert_eq!(format!("{}", date), "0001-01-01");

		// Far future date
		let date = Date::new(9999, 12, 31).unwrap();
		assert_eq!(format!("{}", date), "9999-12-31");

		// Century boundaries
		let date = Date::new(1900, 1, 1).unwrap();
		assert_eq!(format!("{}", date), "1900-01-01");

		let date = Date::new(2000, 1, 1).unwrap();
		assert_eq!(format!("{}", date), "2000-01-01");

		let date = Date::new(2100, 1, 1).unwrap();
		assert_eq!(format!("{}", date), "2100-01-01");
	}

	#[test]
	fn test_date_display_negative_years() {
		// Year 0 (1 BC)
		let date = Date::new(0, 1, 1).unwrap();
		assert_eq!(format!("{}", date), "0000-01-01");

		// Negative years (BC)
		let date = Date::new(-1, 1, 1).unwrap();
		assert_eq!(format!("{}", date), "-0001-01-01");

		let date = Date::new(-100, 12, 31).unwrap();
		assert_eq!(format!("{}", date), "-0100-12-31");
	}

	#[test]
	fn test_date_display_default() {
		let date = Date::default();
		assert_eq!(format!("{}", date), "1970-01-01");
	}

	#[test]
	fn test_date_display_all_months() {
		let months = [
			(1, "01"),
			(2, "02"),
			(3, "03"),
			(4, "04"),
			(5, "05"),
			(6, "06"),
			(7, "07"),
			(8, "08"),
			(9, "09"),
			(10, "10"),
			(11, "11"),
			(12, "12"),
		];

		for (month, expected) in months {
			let date = Date::new(2024, month, 15).unwrap();
			assert_eq!(format!("{}", date), format!("2024-{}-15", expected));
		}
	}

	#[test]
	fn test_date_display_days_in_month() {
		// Test first and last days of various months
		let test_cases = [
			(2024, 1, 1, "2024-01-01"),
			(2024, 1, 31, "2024-01-31"),
			(2024, 2, 1, "2024-02-01"),
			(2024, 2, 29, "2024-02-29"), // Leap year
			(2024, 4, 1, "2024-04-01"),
			(2024, 4, 30, "2024-04-30"),
			(2024, 12, 1, "2024-12-01"),
			(2024, 12, 31, "2024-12-31"),
		];

		for (year, month, day, expected) in test_cases {
			let date = Date::new(year, month, day).unwrap();
			assert_eq!(format!("{}", date), expected);
		}
	}

	#[test]
	fn test_date_roundtrip() {
		// Test that converting to/from days preserves the date
		let test_dates = [
			(1900, 1, 1),
			(1970, 1, 1),
			(2000, 2, 29), // Leap year
			(2024, 12, 31),
			(2100, 6, 15),
		];

		for (year, month, day) in test_dates {
			let date = Date::new(year, month, day).unwrap();
			let days = date.to_days_since_epoch();
			let recovered = Date::from_days_since_epoch(days).unwrap();

			assert_eq!(date.year(), recovered.year());
			assert_eq!(date.month(), recovered.month());
			assert_eq!(date.day(), recovered.day());
		}
	}

	#[test]
	fn test_leap_year_detection() {
		assert!(Date::is_leap_year(2000)); // Divisible by 400
		assert!(Date::is_leap_year(2024)); // Divisible by 4, not by 100
		assert!(!Date::is_leap_year(1900)); // Divisible by 100, not by 400
		assert!(!Date::is_leap_year(2023)); // Not divisible by 4
	}

	#[test]
	fn test_invalid_dates() {
		assert!(Date::new(2024, 0, 1).is_none()); // Invalid month
		assert!(Date::new(2024, 13, 1).is_none()); // Invalid month
		assert!(Date::new(2024, 1, 0).is_none()); // Invalid day
		assert!(Date::new(2024, 1, 32).is_none()); // Invalid day
		assert!(Date::new(2023, 2, 29).is_none()); // Not a leap year
		assert!(Date::new(2024, 4, 31).is_none()); // April has 30 days
	}

	#[test]
	fn test_serde_roundtrip() {
		let date = Date::new(2024, 3, 15).unwrap();
		let json = to_string(&date).unwrap();
		assert_eq!(json, "\"2024-03-15\"");

		let recovered: Date = from_str(&json).unwrap();
		assert_eq!(date, recovered);
	}

	fn assert_date_overflow<T: Debug>(result: Result<T, Box<TypeError>>) {
		let err = result.expect_err("expected DateOverflow error");
		match *err {
			TypeError::Temporal {
				kind: TemporalKind::DateOverflow {
					..
				},
				..
			} => {}
			other => panic!("expected DateOverflow, got: {:?}", other),
		}
	}

	#[test]
	fn test_from_ymd_invalid_month() {
		assert_date_overflow(Date::from_ymd(2024, 0, 1));
		assert_date_overflow(Date::from_ymd(2024, 13, 1));
	}

	#[test]
	fn test_from_ymd_invalid_day() {
		assert_date_overflow(Date::from_ymd(2024, 1, 0));
		assert_date_overflow(Date::from_ymd(2024, 1, 32));
	}

	#[test]
	fn test_from_ymd_non_leap_year() {
		assert_date_overflow(Date::from_ymd(2023, 2, 29));
	}
}
