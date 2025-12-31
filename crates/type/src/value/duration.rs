// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

/// A duration value representing a duration between two points in time.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Duration {
	months: i32, // Store years*12 + months
	days: i32,   // Separate days
	nanos: i64,  // All time components as nanoseconds
}

impl Default for Duration {
	fn default() -> Self {
		Self::zero()
	}
}

impl Duration {
	pub fn new(months: i32, days: i32, nanos: i64) -> Self {
		Self {
			months,
			days,
			nanos,
		}
	}

	pub fn from_seconds(seconds: i64) -> Self {
		Self {
			months: 0,
			days: 0,
			nanos: seconds * 1_000_000_000,
		}
	}

	pub fn from_milliseconds(milliseconds: i64) -> Self {
		Self {
			months: 0,
			days: 0,
			nanos: milliseconds * 1_000_000,
		}
	}

	pub fn from_microseconds(microseconds: i64) -> Self {
		Self {
			months: 0,
			days: 0,
			nanos: microseconds * 1_000,
		}
	}

	pub fn from_nanoseconds(nanoseconds: i64) -> Self {
		Self {
			months: 0,
			days: 0,
			nanos: nanoseconds,
		}
	}

	pub fn from_minutes(minutes: i64) -> Self {
		Self {
			months: 0,
			days: 0,
			nanos: minutes * 60 * 1_000_000_000,
		}
	}

	pub fn from_hours(hours: i64) -> Self {
		Self {
			months: 0,
			days: 0,
			nanos: hours * 60 * 60 * 1_000_000_000,
		}
	}

	pub fn from_days(days: i64) -> Self {
		Self {
			months: 0,
			days: days as i32,
			nanos: 0,
		}
	}

	pub fn from_weeks(weeks: i64) -> Self {
		Self {
			months: 0,
			days: (weeks * 7) as i32,
			nanos: 0,
		}
	}

	pub fn from_months(months: i64) -> Self {
		Self {
			months: months as i32,
			days: 0,
			nanos: 0,
		}
	}

	pub fn from_years(years: i64) -> Self {
		Self {
			months: (years * 12) as i32,
			days: 0,
			nanos: 0,
		}
	}

	pub fn zero() -> Self {
		Self {
			months: 0,
			days: 0,
			nanos: 0,
		}
	}

	pub fn seconds(&self) -> i64 {
		self.nanos / 1_000_000_000
	}

	pub fn milliseconds(&self) -> i64 {
		self.nanos / 1_000_000
	}

	pub fn microseconds(&self) -> i64 {
		self.nanos / 1_000
	}

	pub fn nanoseconds(&self) -> i64 {
		self.nanos
	}

	pub fn get_months(&self) -> i32 {
		self.months
	}

	pub fn get_days(&self) -> i32 {
		self.days
	}

	pub fn get_nanos(&self) -> i64 {
		self.nanos
	}

	pub fn is_positive(&self) -> bool {
		self.months > 0 || self.days > 0 || self.nanos > 0
	}

	pub fn is_negative(&self) -> bool {
		self.months < 0 || self.days < 0 || self.nanos < 0
	}

	pub fn abs(&self) -> Self {
		Self {
			months: self.months.abs(),
			days: self.days.abs(),
			nanos: self.nanos.abs(),
		}
	}

	pub fn negate(&self) -> Self {
		Self {
			months: -self.months,
			days: -self.days,
			nanos: -self.nanos,
		}
	}
}

impl PartialOrd for Duration {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Duration {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		// Compare months first
		match self.months.cmp(&other.months) {
			std::cmp::Ordering::Equal => {
				// Then days
				match self.days.cmp(&other.days) {
					std::cmp::Ordering::Equal => {
						// Finally nanos
						self.nanos.cmp(&other.nanos)
					}
					other_order => other_order,
				}
			}
			other_order => other_order,
		}
	}
}

impl Display for Duration {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		// ISO 8601 duration format: P[n]Y[n]M[n]DT[n]H[n]M[n.n]S
		if self.months == 0 && self.days == 0 && self.nanos == 0 {
			return write!(f, "PT0S");
		}

		write!(f, "P")?;

		// Extract years and months
		let years = self.months / 12;
		let months = self.months % 12;

		if years != 0 {
			write!(f, "{}Y", years)?;
		}

		if months != 0 {
			write!(f, "{}M", months)?;
		}

		// Time components from nanos with normalization
		let total_seconds = self.nanos / 1_000_000_000;
		let remaining_nanos = self.nanos % 1_000_000_000;

		// Normalize to days if hours >= 24
		let extra_days = total_seconds / 86400; // 24 * 60 * 60
		let remaining_seconds = total_seconds % 86400;

		let display_days = self.days + extra_days as i32;
		let hours = remaining_seconds / 3600;
		let minutes = (remaining_seconds % 3600) / 60;
		let seconds = remaining_seconds % 60;

		if display_days != 0 {
			write!(f, "{}D", display_days)?;
		}

		if hours != 0 || minutes != 0 || seconds != 0 || remaining_nanos != 0 {
			write!(f, "T")?;

			if hours != 0 {
				write!(f, "{}H", hours)?;
			}

			if minutes != 0 {
				write!(f, "{}M", minutes)?;
			}

			if seconds != 0 || remaining_nanos != 0 {
				if remaining_nanos != 0 {
					// Format fractional seconds with
					// trailing zeros removed
					let fractional = remaining_nanos as f64 / 1_000_000_000.0;
					let total_seconds_f = seconds as f64 + fractional;
					// Remove trailing zeros from fractional
					// part
					let formatted_str = format!("{:.9}", total_seconds_f);
					let formatted = formatted_str.trim_end_matches('0').trim_end_matches('.');
					write!(f, "{}S", formatted)?;
				} else {
					write!(f, "{}S", seconds)?;
				}
			}
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_duration_display_zero() {
		let duration = Duration::zero();
		assert_eq!(format!("{}", duration), "PT0S");

		let duration = Duration::from_seconds(0);
		assert_eq!(format!("{}", duration), "PT0S");

		let duration = Duration::from_nanoseconds(0);
		assert_eq!(format!("{}", duration), "PT0S");

		let duration = Duration::default();
		assert_eq!(format!("{}", duration), "PT0S");
	}

	#[test]
	fn test_duration_display_seconds_only() {
		let duration = Duration::from_seconds(1);
		assert_eq!(format!("{}", duration), "PT1S");

		let duration = Duration::from_seconds(30);
		assert_eq!(format!("{}", duration), "PT30S");

		let duration = Duration::from_seconds(59);
		assert_eq!(format!("{}", duration), "PT59S");
	}

	#[test]
	fn test_duration_display_minutes_only() {
		let duration = Duration::from_minutes(1);
		assert_eq!(format!("{}", duration), "PT1M");

		let duration = Duration::from_minutes(30);
		assert_eq!(format!("{}", duration), "PT30M");

		let duration = Duration::from_minutes(59);
		assert_eq!(format!("{}", duration), "PT59M");
	}

	#[test]
	fn test_duration_display_hours_only() {
		let duration = Duration::from_hours(1);
		assert_eq!(format!("{}", duration), "PT1H");

		let duration = Duration::from_hours(12);
		assert_eq!(format!("{}", duration), "PT12H");

		let duration = Duration::from_hours(23);
		assert_eq!(format!("{}", duration), "PT23H");
	}

	#[test]
	fn test_duration_display_days_only() {
		let duration = Duration::from_days(1);
		assert_eq!(format!("{}", duration), "P1D");

		let duration = Duration::from_days(7);
		assert_eq!(format!("{}", duration), "P7D");

		let duration = Duration::from_days(365);
		assert_eq!(format!("{}", duration), "P365D");
	}

	#[test]
	fn test_duration_display_weeks_only() {
		let duration = Duration::from_weeks(1);
		assert_eq!(format!("{}", duration), "P7D");

		let duration = Duration::from_weeks(2);
		assert_eq!(format!("{}", duration), "P14D");

		let duration = Duration::from_weeks(52);
		assert_eq!(format!("{}", duration), "P364D");
	}

	#[test]
	fn test_duration_display_combined_time() {
		// Hours and minutes
		let duration = Duration::new(0, 0, (1 * 60 * 60 + 30 * 60) * 1_000_000_000);
		assert_eq!(format!("{}", duration), "PT1H30M");

		// Minutes and seconds
		let duration = Duration::new(0, 0, (5 * 60 + 45) * 1_000_000_000);
		assert_eq!(format!("{}", duration), "PT5M45S");

		// Hours, minutes, and seconds
		let duration = Duration::new(0, 0, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000);
		assert_eq!(format!("{}", duration), "PT2H30M45S");
	}

	#[test]
	fn test_duration_display_combined_date_time() {
		// Days and hours
		let duration = Duration::new(0, 1, 2 * 60 * 60 * 1_000_000_000);
		assert_eq!(format!("{}", duration), "P1DT2H");

		// Days and minutes
		let duration = Duration::new(0, 1, 30 * 60 * 1_000_000_000);
		assert_eq!(format!("{}", duration), "P1DT30M");

		// Days, hours, and minutes
		let duration = Duration::new(0, 1, (2 * 60 * 60 + 30 * 60) * 1_000_000_000);
		assert_eq!(format!("{}", duration), "P1DT2H30M");

		// Days, hours, minutes, and seconds
		let duration = Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000);
		assert_eq!(format!("{}", duration), "P1DT2H30M45S");
	}

	#[test]
	fn test_duration_display_milliseconds() {
		let duration = Duration::from_milliseconds(123);
		assert_eq!(format!("{}", duration), "PT0.123S");

		let duration = Duration::from_milliseconds(1);
		assert_eq!(format!("{}", duration), "PT0.001S");

		let duration = Duration::from_milliseconds(999);
		assert_eq!(format!("{}", duration), "PT0.999S");

		let duration = Duration::from_milliseconds(1500);
		assert_eq!(format!("{}", duration), "PT1.5S");
	}

	#[test]
	fn test_duration_display_microseconds() {
		let duration = Duration::from_microseconds(123456);
		assert_eq!(format!("{}", duration), "PT0.123456S");

		let duration = Duration::from_microseconds(1);
		assert_eq!(format!("{}", duration), "PT0.000001S");

		let duration = Duration::from_microseconds(999999);
		assert_eq!(format!("{}", duration), "PT0.999999S");

		let duration = Duration::from_microseconds(1500000);
		assert_eq!(format!("{}", duration), "PT1.5S");
	}

	#[test]
	fn test_duration_display_nanoseconds() {
		let duration = Duration::from_nanoseconds(123456789);
		assert_eq!(format!("{}", duration), "PT0.123456789S");

		let duration = Duration::from_nanoseconds(1);
		assert_eq!(format!("{}", duration), "PT0.000000001S");

		let duration = Duration::from_nanoseconds(999999999);
		assert_eq!(format!("{}", duration), "PT0.999999999S");

		let duration = Duration::from_nanoseconds(1500000000);
		assert_eq!(format!("{}", duration), "PT1.5S");
	}

	#[test]
	fn test_duration_display_fractional_seconds_with_integers() {
		// Seconds with milliseconds
		let duration = Duration::new(0, 0, 1 * 1_000_000_000 + 500 * 1_000_000);
		assert_eq!(format!("{}", duration), "PT1.5S");

		// Seconds with microseconds
		let duration = Duration::new(0, 0, 2 * 1_000_000_000 + 123456 * 1_000);
		assert_eq!(format!("{}", duration), "PT2.123456S");

		// Seconds with nanoseconds
		let duration = Duration::new(0, 0, 3 * 1_000_000_000 + 123456789);
		assert_eq!(format!("{}", duration), "PT3.123456789S");
	}

	#[test]
	fn test_duration_display_comptokenize_durations() {
		// Comptokenize interval with all components
		let duration = Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000 + 123 * 1_000_000);
		assert_eq!(format!("{}", duration), "P1DT2H30M45.123S");

		// Another comptokenize interval
		let duration = Duration::new(0, 7, (12 * 60 * 60 + 45 * 60 + 30) * 1_000_000_000 + 456789 * 1_000);
		assert_eq!(format!("{}", duration), "P7DT12H45M30.456789S");
	}

	#[test]
	fn test_duration_display_trailing_zeros_removed() {
		// Test that trailing zeros are removed from fractional seconds
		let duration = Duration::from_nanoseconds(100000000); // 0.1 seconds
		assert_eq!(format!("{}", duration), "PT0.1S");

		let duration = Duration::from_nanoseconds(120000000); // 0.12 seconds
		assert_eq!(format!("{}", duration), "PT0.12S");

		let duration = Duration::from_nanoseconds(123000000); // 0.123 seconds
		assert_eq!(format!("{}", duration), "PT0.123S");

		let duration = Duration::from_nanoseconds(123400000); // 0.1234 seconds
		assert_eq!(format!("{}", duration), "PT0.1234S");

		let duration = Duration::from_nanoseconds(123450000); // 0.12345 seconds
		assert_eq!(format!("{}", duration), "PT0.12345S");

		let duration = Duration::from_nanoseconds(123456000); // 0.123456 seconds
		assert_eq!(format!("{}", duration), "PT0.123456S");

		let duration = Duration::from_nanoseconds(123456700); // 0.1234567 seconds
		assert_eq!(format!("{}", duration), "PT0.1234567S");

		let duration = Duration::from_nanoseconds(123456780); // 0.12345678 seconds
		assert_eq!(format!("{}", duration), "PT0.12345678S");

		let duration = Duration::from_nanoseconds(123456789); // 0.123456789 seconds
		assert_eq!(format!("{}", duration), "PT0.123456789S");
	}

	#[test]
	fn test_duration_display_negative_durations() {
		// Test negative intervals
		let duration = Duration::from_seconds(-30);
		assert_eq!(format!("{}", duration), "PT-30S");

		let duration = Duration::from_minutes(-5);
		assert_eq!(format!("{}", duration), "PT-5M");

		let duration = Duration::from_hours(-2);
		assert_eq!(format!("{}", duration), "PT-2H");

		let duration = Duration::from_days(-1);
		assert_eq!(format!("{}", duration), "P-1D");
	}

	#[test]
	fn test_duration_display_large_values() {
		// Test large intervals
		let duration = Duration::from_days(1000);
		assert_eq!(format!("{}", duration), "P1000D");

		let duration = Duration::from_hours(25);
		assert_eq!(format!("{}", duration), "P1DT1H");

		let duration = Duration::from_minutes(1500); // 25 hours
		assert_eq!(format!("{}", duration), "P1DT1H");

		let duration = Duration::from_seconds(90000); // 25 hours
		assert_eq!(format!("{}", duration), "P1DT1H");
	}

	#[test]
	fn test_duration_display_edge_cases() {
		// Test edge cases with single nanosecond
		let duration = Duration::from_nanoseconds(1);
		assert_eq!(format!("{}", duration), "PT0.000000001S");

		// Test maximum nanoseconds in a second
		let duration = Duration::from_nanoseconds(999999999);
		assert_eq!(format!("{}", duration), "PT0.999999999S");

		// Test exactly 1 second
		let duration = Duration::from_nanoseconds(1000000000);
		assert_eq!(format!("{}", duration), "PT1S");

		// Test exactly 1 minute
		let duration = Duration::from_nanoseconds(60 * 1000000000);
		assert_eq!(format!("{}", duration), "PT1M");

		// Test exactly 1 hour
		let duration = Duration::from_nanoseconds(3600 * 1000000000);
		assert_eq!(format!("{}", duration), "PT1H");

		// Test exactly 1 day
		let duration = Duration::from_nanoseconds(86400 * 1000000000);
		assert_eq!(format!("{}", duration), "P1D");
	}

	#[test]
	fn test_duration_display_precision_boundaries() {
		// Test precision boundaries
		let duration = Duration::from_nanoseconds(100); // 0.0000001 seconds
		assert_eq!(format!("{}", duration), "PT0.0000001S");

		let duration = Duration::from_nanoseconds(10); // 0.00000001 seconds
		assert_eq!(format!("{}", duration), "PT0.00000001S");

		let duration = Duration::from_nanoseconds(1); // 0.000000001 seconds
		assert_eq!(format!("{}", duration), "PT0.000000001S");
	}

	#[test]
	fn test_duration_display_from_nanos() {
		// Test the from_nanos method
		let duration = Duration::from_nanoseconds(123456789);
		assert_eq!(format!("{}", duration), "PT0.123456789S");

		let duration = Duration::from_nanoseconds(3661000000000); // 1 hour 1 minute 1 second
		assert_eq!(format!("{}", duration), "PT1H1M1S");
	}

	#[test]
	fn test_duration_display_abs_and_negate() {
		// Test absolute value
		let duration = Duration::from_seconds(-30);
		let abs_duration = duration.abs();
		assert_eq!(format!("{}", abs_duration), "PT30S");

		// Test negation
		let duration = Duration::from_seconds(30);
		let neg_duration = duration.negate();
		assert_eq!(format!("{}", neg_duration), "PT-30S");
	}
}
