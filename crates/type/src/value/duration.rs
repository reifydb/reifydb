// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cmp,
	fmt::{self, Display, Formatter, Write},
	ops,
};

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

	/// Format as ISO 8601 duration string: `P[n]Y[n]M[n]DT[n]H[n]M[n.n]S`
	pub fn to_iso_string(&self) -> String {
		if self.months == 0 && self.days == 0 && self.nanos == 0 {
			return "PT0S".to_string();
		}

		let mut result = String::from("P");

		let years = self.months / 12;
		let months = self.months % 12;

		if years != 0 {
			write!(result, "{}Y", years).unwrap();
		}
		if months != 0 {
			write!(result, "{}M", months).unwrap();
		}

		let total_seconds = self.nanos / 1_000_000_000;
		let remaining_nanos = self.nanos % 1_000_000_000;

		let extra_days = total_seconds / 86400;
		let remaining_seconds = total_seconds % 86400;

		let display_days = self.days + extra_days as i32;
		let hours = remaining_seconds / 3600;
		let minutes = (remaining_seconds % 3600) / 60;
		let seconds = remaining_seconds % 60;

		if display_days != 0 {
			write!(result, "{}D", display_days).unwrap();
		}

		if hours != 0 || minutes != 0 || seconds != 0 || remaining_nanos != 0 {
			result.push('T');

			if hours != 0 {
				write!(result, "{}H", hours).unwrap();
			}
			if minutes != 0 {
				write!(result, "{}M", minutes).unwrap();
			}
			if seconds != 0 || remaining_nanos != 0 {
				if remaining_nanos != 0 {
					let fractional = remaining_nanos as f64 / 1_000_000_000.0;
					let total_seconds_f = seconds as f64 + fractional;
					let formatted_str = format!("{:.9}", total_seconds_f);
					let formatted = formatted_str.trim_end_matches('0').trim_end_matches('.');
					write!(result, "{}S", formatted).unwrap();
				} else {
					write!(result, "{}S", seconds).unwrap();
				}
			}
		}

		result
	}
}

impl PartialOrd for Duration {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Duration {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		// Compare months first
		match self.months.cmp(&other.months) {
			cmp::Ordering::Equal => {
				// Then days
				match self.days.cmp(&other.days) {
					cmp::Ordering::Equal => {
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

impl ops::Add for Duration {
	type Output = Self;
	fn add(self, rhs: Self) -> Self {
		Self {
			months: self.months + rhs.months,
			days: self.days + rhs.days,
			nanos: self.nanos + rhs.nanos,
		}
	}
}

impl ops::Sub for Duration {
	type Output = Self;
	fn sub(self, rhs: Self) -> Self {
		Self {
			months: self.months - rhs.months,
			days: self.days - rhs.days,
			nanos: self.nanos - rhs.nanos,
		}
	}
}

impl ops::Mul<i64> for Duration {
	type Output = Self;
	fn mul(self, rhs: i64) -> Self {
		Self {
			months: self.months * rhs as i32,
			days: self.days * rhs as i32,
			nanos: self.nanos * rhs,
		}
	}
}

impl Display for Duration {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		if self.months == 0 && self.days == 0 && self.nanos == 0 {
			return write!(f, "0s");
		}

		let years = self.months / 12;
		let months = self.months % 12;

		let total_seconds = self.nanos / 1_000_000_000;
		let remaining_nanos = self.nanos % 1_000_000_000;

		let extra_days = total_seconds / 86400;
		let remaining_seconds = total_seconds % 86400;

		let display_days = self.days + extra_days as i32;
		let hours = remaining_seconds / 3600;
		let minutes = (remaining_seconds % 3600) / 60;
		let seconds = remaining_seconds % 60;

		let abs_remaining = remaining_nanos.abs();
		let ms = abs_remaining / 1_000_000;
		let us = (abs_remaining % 1_000_000) / 1_000;
		let ns = abs_remaining % 1_000;

		if years != 0 {
			write!(f, "{}y", years)?;
		}
		if months != 0 {
			write!(f, "{}mo", months)?;
		}
		if display_days != 0 {
			write!(f, "{}d", display_days)?;
		}
		if hours != 0 {
			write!(f, "{}h", hours)?;
		}
		if minutes != 0 {
			write!(f, "{}m", minutes)?;
		}
		if seconds != 0 {
			write!(f, "{}s", seconds)?;
		}

		if ms != 0 || us != 0 || ns != 0 {
			if remaining_nanos < 0
				&& seconds == 0 && hours == 0
				&& minutes == 0 && display_days == 0
				&& years == 0 && months == 0
			{
				write!(f, "-")?;
			}
			if ms != 0 {
				write!(f, "{}ms", ms)?;
			}
			if us != 0 {
				write!(f, "{}us", us)?;
			}
			if ns != 0 {
				write!(f, "{}ns", ns)?;
			}
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	// ---- to_iso_string tests (ISO 8601 format) ----

	#[test]
	fn test_duration_iso_string_zero() {
		assert_eq!(Duration::zero().to_iso_string(), "PT0S");
		assert_eq!(Duration::from_seconds(0).to_iso_string(), "PT0S");
		assert_eq!(Duration::from_nanoseconds(0).to_iso_string(), "PT0S");
		assert_eq!(Duration::default().to_iso_string(), "PT0S");
	}

	#[test]
	fn test_duration_iso_string_seconds() {
		assert_eq!(Duration::from_seconds(1).to_iso_string(), "PT1S");
		assert_eq!(Duration::from_seconds(30).to_iso_string(), "PT30S");
		assert_eq!(Duration::from_seconds(59).to_iso_string(), "PT59S");
	}

	#[test]
	fn test_duration_iso_string_minutes() {
		assert_eq!(Duration::from_minutes(1).to_iso_string(), "PT1M");
		assert_eq!(Duration::from_minutes(30).to_iso_string(), "PT30M");
		assert_eq!(Duration::from_minutes(59).to_iso_string(), "PT59M");
	}

	#[test]
	fn test_duration_iso_string_hours() {
		assert_eq!(Duration::from_hours(1).to_iso_string(), "PT1H");
		assert_eq!(Duration::from_hours(12).to_iso_string(), "PT12H");
		assert_eq!(Duration::from_hours(23).to_iso_string(), "PT23H");
	}

	#[test]
	fn test_duration_iso_string_days() {
		assert_eq!(Duration::from_days(1).to_iso_string(), "P1D");
		assert_eq!(Duration::from_days(7).to_iso_string(), "P7D");
		assert_eq!(Duration::from_days(365).to_iso_string(), "P365D");
	}

	#[test]
	fn test_duration_iso_string_weeks() {
		assert_eq!(Duration::from_weeks(1).to_iso_string(), "P7D");
		assert_eq!(Duration::from_weeks(2).to_iso_string(), "P14D");
		assert_eq!(Duration::from_weeks(52).to_iso_string(), "P364D");
	}

	#[test]
	fn test_duration_iso_string_combined_time() {
		let d = Duration::new(0, 0, (1 * 60 * 60 + 30 * 60) * 1_000_000_000);
		assert_eq!(d.to_iso_string(), "PT1H30M");

		let d = Duration::new(0, 0, (5 * 60 + 45) * 1_000_000_000);
		assert_eq!(d.to_iso_string(), "PT5M45S");

		let d = Duration::new(0, 0, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000);
		assert_eq!(d.to_iso_string(), "PT2H30M45S");
	}

	#[test]
	fn test_duration_iso_string_combined_date_time() {
		assert_eq!(Duration::new(0, 1, 2 * 60 * 60 * 1_000_000_000).to_iso_string(), "P1DT2H");
		assert_eq!(Duration::new(0, 1, 30 * 60 * 1_000_000_000).to_iso_string(), "P1DT30M");
		assert_eq!(Duration::new(0, 1, (2 * 60 * 60 + 30 * 60) * 1_000_000_000).to_iso_string(), "P1DT2H30M");
		assert_eq!(
			Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000).to_iso_string(),
			"P1DT2H30M45S"
		);
	}

	#[test]
	fn test_duration_iso_string_milliseconds() {
		assert_eq!(Duration::from_milliseconds(123).to_iso_string(), "PT0.123S");
		assert_eq!(Duration::from_milliseconds(1).to_iso_string(), "PT0.001S");
		assert_eq!(Duration::from_milliseconds(999).to_iso_string(), "PT0.999S");
		assert_eq!(Duration::from_milliseconds(1500).to_iso_string(), "PT1.5S");
	}

	#[test]
	fn test_duration_iso_string_microseconds() {
		assert_eq!(Duration::from_microseconds(123456).to_iso_string(), "PT0.123456S");
		assert_eq!(Duration::from_microseconds(1).to_iso_string(), "PT0.000001S");
		assert_eq!(Duration::from_microseconds(999999).to_iso_string(), "PT0.999999S");
		assert_eq!(Duration::from_microseconds(1500000).to_iso_string(), "PT1.5S");
	}

	#[test]
	fn test_duration_iso_string_nanoseconds() {
		assert_eq!(Duration::from_nanoseconds(123456789).to_iso_string(), "PT0.123456789S");
		assert_eq!(Duration::from_nanoseconds(1).to_iso_string(), "PT0.000000001S");
		assert_eq!(Duration::from_nanoseconds(999999999).to_iso_string(), "PT0.999999999S");
		assert_eq!(Duration::from_nanoseconds(1500000000).to_iso_string(), "PT1.5S");
	}

	#[test]
	fn test_duration_iso_string_fractional_seconds() {
		let d = Duration::new(0, 0, 1 * 1_000_000_000 + 500 * 1_000_000);
		assert_eq!(d.to_iso_string(), "PT1.5S");

		let d = Duration::new(0, 0, 2 * 1_000_000_000 + 123456 * 1_000);
		assert_eq!(d.to_iso_string(), "PT2.123456S");

		let d = Duration::new(0, 0, 3 * 1_000_000_000 + 123456789);
		assert_eq!(d.to_iso_string(), "PT3.123456789S");
	}

	#[test]
	fn test_duration_iso_string_complex() {
		let d = Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000 + 123 * 1_000_000);
		assert_eq!(d.to_iso_string(), "P1DT2H30M45.123S");

		let d = Duration::new(0, 7, (12 * 60 * 60 + 45 * 60 + 30) * 1_000_000_000 + 456789 * 1_000);
		assert_eq!(d.to_iso_string(), "P7DT12H45M30.456789S");
	}

	#[test]
	fn test_duration_iso_string_trailing_zeros() {
		assert_eq!(Duration::from_nanoseconds(100000000).to_iso_string(), "PT0.1S");
		assert_eq!(Duration::from_nanoseconds(120000000).to_iso_string(), "PT0.12S");
		assert_eq!(Duration::from_nanoseconds(123000000).to_iso_string(), "PT0.123S");
		assert_eq!(Duration::from_nanoseconds(123400000).to_iso_string(), "PT0.1234S");
		assert_eq!(Duration::from_nanoseconds(123450000).to_iso_string(), "PT0.12345S");
		assert_eq!(Duration::from_nanoseconds(123456000).to_iso_string(), "PT0.123456S");
		assert_eq!(Duration::from_nanoseconds(123456700).to_iso_string(), "PT0.1234567S");
		assert_eq!(Duration::from_nanoseconds(123456780).to_iso_string(), "PT0.12345678S");
		assert_eq!(Duration::from_nanoseconds(123456789).to_iso_string(), "PT0.123456789S");
	}

	#[test]
	fn test_duration_iso_string_negative() {
		assert_eq!(Duration::from_seconds(-30).to_iso_string(), "PT-30S");
		assert_eq!(Duration::from_minutes(-5).to_iso_string(), "PT-5M");
		assert_eq!(Duration::from_hours(-2).to_iso_string(), "PT-2H");
		assert_eq!(Duration::from_days(-1).to_iso_string(), "P-1D");
	}

	#[test]
	fn test_duration_iso_string_large() {
		assert_eq!(Duration::from_days(1000).to_iso_string(), "P1000D");
		assert_eq!(Duration::from_hours(25).to_iso_string(), "P1DT1H");
		assert_eq!(Duration::from_minutes(1500).to_iso_string(), "P1DT1H");
		assert_eq!(Duration::from_seconds(90000).to_iso_string(), "P1DT1H");
	}

	#[test]
	fn test_duration_iso_string_edge_cases() {
		assert_eq!(Duration::from_nanoseconds(1).to_iso_string(), "PT0.000000001S");
		assert_eq!(Duration::from_nanoseconds(999999999).to_iso_string(), "PT0.999999999S");
		assert_eq!(Duration::from_nanoseconds(1000000000).to_iso_string(), "PT1S");
		assert_eq!(Duration::from_nanoseconds(60 * 1000000000).to_iso_string(), "PT1M");
		assert_eq!(Duration::from_nanoseconds(3600 * 1000000000).to_iso_string(), "PT1H");
		assert_eq!(Duration::from_nanoseconds(86400 * 1000000000).to_iso_string(), "P1D");
	}

	#[test]
	fn test_duration_iso_string_precision() {
		assert_eq!(Duration::from_nanoseconds(100).to_iso_string(), "PT0.0000001S");
		assert_eq!(Duration::from_nanoseconds(10).to_iso_string(), "PT0.00000001S");
		assert_eq!(Duration::from_nanoseconds(1).to_iso_string(), "PT0.000000001S");
	}

	// ---- Display tests (human-readable format) ----

	#[test]
	fn test_duration_display_zero() {
		assert_eq!(format!("{}", Duration::zero()), "0s");
		assert_eq!(format!("{}", Duration::from_seconds(0)), "0s");
		assert_eq!(format!("{}", Duration::from_nanoseconds(0)), "0s");
		assert_eq!(format!("{}", Duration::default()), "0s");
	}

	#[test]
	fn test_duration_display_seconds_only() {
		assert_eq!(format!("{}", Duration::from_seconds(1)), "1s");
		assert_eq!(format!("{}", Duration::from_seconds(30)), "30s");
		assert_eq!(format!("{}", Duration::from_seconds(59)), "59s");
	}

	#[test]
	fn test_duration_display_minutes_only() {
		assert_eq!(format!("{}", Duration::from_minutes(1)), "1m");
		assert_eq!(format!("{}", Duration::from_minutes(30)), "30m");
		assert_eq!(format!("{}", Duration::from_minutes(59)), "59m");
	}

	#[test]
	fn test_duration_display_hours_only() {
		assert_eq!(format!("{}", Duration::from_hours(1)), "1h");
		assert_eq!(format!("{}", Duration::from_hours(12)), "12h");
		assert_eq!(format!("{}", Duration::from_hours(23)), "23h");
	}

	#[test]
	fn test_duration_display_days_only() {
		assert_eq!(format!("{}", Duration::from_days(1)), "1d");
		assert_eq!(format!("{}", Duration::from_days(7)), "7d");
		assert_eq!(format!("{}", Duration::from_days(365)), "365d");
	}

	#[test]
	fn test_duration_display_weeks_only() {
		assert_eq!(format!("{}", Duration::from_weeks(1)), "7d");
		assert_eq!(format!("{}", Duration::from_weeks(2)), "14d");
		assert_eq!(format!("{}", Duration::from_weeks(52)), "364d");
	}

	#[test]
	fn test_duration_display_months_only() {
		assert_eq!(format!("{}", Duration::from_months(1)), "1mo");
		assert_eq!(format!("{}", Duration::from_months(6)), "6mo");
		assert_eq!(format!("{}", Duration::from_months(11)), "11mo");
	}

	#[test]
	fn test_duration_display_years_only() {
		assert_eq!(format!("{}", Duration::from_years(1)), "1y");
		assert_eq!(format!("{}", Duration::from_years(10)), "10y");
		assert_eq!(format!("{}", Duration::from_years(100)), "100y");
	}

	#[test]
	fn test_duration_display_combined_time() {
		let d = Duration::new(0, 0, (1 * 60 * 60 + 30 * 60) * 1_000_000_000);
		assert_eq!(format!("{}", d), "1h30m");

		let d = Duration::new(0, 0, (5 * 60 + 45) * 1_000_000_000);
		assert_eq!(format!("{}", d), "5m45s");

		let d = Duration::new(0, 0, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000);
		assert_eq!(format!("{}", d), "2h30m45s");
	}

	#[test]
	fn test_duration_display_combined_date_time() {
		assert_eq!(format!("{}", Duration::new(0, 1, 2 * 60 * 60 * 1_000_000_000)), "1d2h");
		assert_eq!(format!("{}", Duration::new(0, 1, 30 * 60 * 1_000_000_000)), "1d30m");
		assert_eq!(format!("{}", Duration::new(0, 1, (2 * 60 * 60 + 30 * 60) * 1_000_000_000)), "1d2h30m");
		assert_eq!(
			format!("{}", Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000)),
			"1d2h30m45s"
		);
	}

	#[test]
	fn test_duration_display_years_months() {
		assert_eq!(format!("{}", Duration::new(13, 0, 0)), "1y1mo");
		assert_eq!(format!("{}", Duration::new(27, 0, 0)), "2y3mo");
	}

	#[test]
	fn test_duration_display_full_components() {
		let nanos = (4 * 60 * 60 + 5 * 60 + 6) * 1_000_000_000i64;
		assert_eq!(format!("{}", Duration::new(14, 3, nanos)), "1y2mo3d4h5m6s");
	}

	#[test]
	fn test_duration_display_milliseconds() {
		assert_eq!(format!("{}", Duration::from_milliseconds(123)), "123ms");
		assert_eq!(format!("{}", Duration::from_milliseconds(1)), "1ms");
		assert_eq!(format!("{}", Duration::from_milliseconds(999)), "999ms");
		assert_eq!(format!("{}", Duration::from_milliseconds(1500)), "1s500ms");
	}

	#[test]
	fn test_duration_display_microseconds() {
		assert_eq!(format!("{}", Duration::from_microseconds(123456)), "123ms456us");
		assert_eq!(format!("{}", Duration::from_microseconds(1)), "1us");
		assert_eq!(format!("{}", Duration::from_microseconds(999999)), "999ms999us");
		assert_eq!(format!("{}", Duration::from_microseconds(1500000)), "1s500ms");
	}

	#[test]
	fn test_duration_display_nanoseconds() {
		assert_eq!(format!("{}", Duration::from_nanoseconds(123456789)), "123ms456us789ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(1)), "1ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(999999999)), "999ms999us999ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(1500000000)), "1s500ms");
	}

	#[test]
	fn test_duration_display_sub_second_decomposition() {
		let d = Duration::new(0, 0, 1 * 1_000_000_000 + 500 * 1_000_000);
		assert_eq!(format!("{}", d), "1s500ms");

		let d = Duration::new(0, 0, 2 * 1_000_000_000 + 123456 * 1_000);
		assert_eq!(format!("{}", d), "2s123ms456us");

		let d = Duration::new(0, 0, 3 * 1_000_000_000 + 123456789);
		assert_eq!(format!("{}", d), "3s123ms456us789ns");
	}

	#[test]
	fn test_duration_display_complex() {
		let d = Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000 + 123 * 1_000_000);
		assert_eq!(format!("{}", d), "1d2h30m45s123ms");

		let d = Duration::new(0, 7, (12 * 60 * 60 + 45 * 60 + 30) * 1_000_000_000 + 456789 * 1_000);
		assert_eq!(format!("{}", d), "7d12h45m30s456ms789us");
	}

	#[test]
	fn test_duration_display_sub_second_only() {
		assert_eq!(format!("{}", Duration::from_nanoseconds(100000000)), "100ms");
		assert_eq!(format!("{}", Duration::from_nanoseconds(120000000)), "120ms");
		assert_eq!(format!("{}", Duration::from_nanoseconds(123000000)), "123ms");
		assert_eq!(format!("{}", Duration::from_nanoseconds(100)), "100ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(10)), "10ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(1000)), "1us");
	}

	#[test]
	fn test_duration_display_negative() {
		assert_eq!(format!("{}", Duration::from_seconds(-30)), "-30s");
		assert_eq!(format!("{}", Duration::from_minutes(-5)), "-5m");
		assert_eq!(format!("{}", Duration::from_hours(-2)), "-2h");
		assert_eq!(format!("{}", Duration::from_days(-1)), "-1d");
	}

	#[test]
	fn test_duration_display_negative_sub_second() {
		assert_eq!(format!("{}", Duration::from_milliseconds(-500)), "-500ms");
		assert_eq!(format!("{}", Duration::from_microseconds(-100)), "-100us");
		assert_eq!(format!("{}", Duration::from_nanoseconds(-50)), "-50ns");
	}

	#[test]
	fn test_duration_display_large() {
		assert_eq!(format!("{}", Duration::from_days(1000)), "1000d");
		assert_eq!(format!("{}", Duration::from_hours(25)), "1d1h");
		assert_eq!(format!("{}", Duration::from_minutes(1500)), "1d1h");
		assert_eq!(format!("{}", Duration::from_seconds(90000)), "1d1h");
	}

	#[test]
	fn test_duration_display_edge_cases() {
		assert_eq!(format!("{}", Duration::from_nanoseconds(1)), "1ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(999999999)), "999ms999us999ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(1000000000)), "1s");
		assert_eq!(format!("{}", Duration::from_nanoseconds(60 * 1000000000)), "1m");
		assert_eq!(format!("{}", Duration::from_nanoseconds(3600 * 1000000000)), "1h");
		assert_eq!(format!("{}", Duration::from_nanoseconds(86400 * 1000000000)), "1d");
	}

	#[test]
	fn test_duration_display_abs_and_negate() {
		let d = Duration::from_seconds(-30);
		assert_eq!(format!("{}", d.abs()), "30s");

		let d = Duration::from_seconds(30);
		assert_eq!(format!("{}", d.negate()), "-30s");
	}
}
