// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::{
	cmp,
	fmt::{self, Display, Formatter, Write},
	ops,
};

use serde::{Deserialize, Serialize};

use crate::{
	error::{TemporalKind, TypeError},
	fragment::Fragment,
};

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Duration {
	months: i32,
	days: i32,
	nanos: i64,
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;
const SECONDS_PER_DAY: i64 = 86_400;
const DAYS_PER_MONTH: i64 = 30;
const SECONDS_PER_YEAR: i64 = 31_557_600;

impl Default for Duration {
	fn default() -> Self {
		Self::zero()
	}
}

impl Duration {
	fn overflow_err(message: impl Into<String>) -> TypeError {
		TypeError::Temporal {
			kind: TemporalKind::DurationOverflow {
				message: message.into(),
			},
			message: "duration overflow".to_string(),
			fragment: Fragment::None,
		}
	}

	fn mixed_sign_err(days: i32, nanos: i64) -> TypeError {
		TypeError::Temporal {
			kind: TemporalKind::DurationMixedSign {
				days,
				nanos,
			},
			message: format!(
				"duration days and nanos must share the same sign, got days={days}, nanos={nanos}"
			),
			fragment: Fragment::None,
		}
	}

	fn normalized(months: i32, days: i32, nanos: i64) -> Result<Self, Box<TypeError>> {
		let extra_days = i32::try_from(nanos / NANOS_PER_DAY)
			.map_err(|_| Box::new(Self::overflow_err("days overflow during normalization")))?;
		let nanos = nanos % NANOS_PER_DAY;
		let days = days
			.checked_add(extra_days)
			.ok_or_else(|| Box::new(Self::overflow_err("days overflow during normalization")))?;

		if (days > 0 && nanos < 0) || (days < 0 && nanos > 0) {
			return Err(Box::new(Self::mixed_sign_err(days, nanos)));
		}

		Ok(Self {
			months,
			days,
			nanos,
		})
	}

	pub fn new(months: i32, days: i32, nanos: i64) -> Result<Self, Box<TypeError>> {
		Self::normalized(months, days, nanos)
	}

	pub fn from_seconds(seconds: i64) -> Result<Self, Box<TypeError>> {
		Self::normalized(0, 0, seconds * 1_000_000_000)
	}

	pub fn from_milliseconds(milliseconds: i64) -> Result<Self, Box<TypeError>> {
		Self::normalized(0, 0, milliseconds * 1_000_000)
	}

	pub fn from_microseconds(microseconds: i64) -> Result<Self, Box<TypeError>> {
		Self::normalized(0, 0, microseconds * 1_000)
	}

	pub fn from_micros_infallible(microseconds: u64) -> Self {
		const US_PER_DAY: u64 = 86_400_000_000;
		let whole_days = microseconds / US_PER_DAY;
		let remainder_us = microseconds % US_PER_DAY;
		let days = if whole_days > i32::MAX as u64 {
			i32::MAX
		} else {
			whole_days as i32
		};
		Self {
			months: 0,
			days,
			nanos: (remainder_us * 1_000) as i64,
		}
	}

	pub fn from_nanoseconds(nanoseconds: i64) -> Result<Self, Box<TypeError>> {
		Self::normalized(0, 0, nanoseconds)
	}

	pub fn from_minutes(minutes: i64) -> Result<Self, Box<TypeError>> {
		Self::normalized(0, 0, minutes * 60 * 1_000_000_000)
	}

	pub fn from_hours(hours: i64) -> Result<Self, Box<TypeError>> {
		Self::normalized(0, 0, hours * 60 * 60 * 1_000_000_000)
	}

	pub fn from_days(days: i64) -> Result<Self, Box<TypeError>> {
		let days =
			i32::try_from(days).map_err(|_| Box::new(Self::overflow_err("days value out of i32 range")))?;
		Self::normalized(0, days, 0)
	}

	pub fn from_weeks(weeks: i64) -> Result<Self, Box<TypeError>> {
		let days = weeks.checked_mul(7).ok_or_else(|| Box::new(Self::overflow_err("weeks overflow")))?;
		let days =
			i32::try_from(days).map_err(|_| Box::new(Self::overflow_err("days value out of i32 range")))?;
		Self::normalized(0, days, 0)
	}

	pub fn from_months(months: i64) -> Result<Self, Box<TypeError>> {
		let months = i32::try_from(months)
			.map_err(|_| Box::new(Self::overflow_err("months value out of i32 range")))?;
		Self::normalized(months, 0, 0)
	}

	pub fn from_years(years: i64) -> Result<Self, Box<TypeError>> {
		let months = years.checked_mul(12).ok_or_else(|| Box::new(Self::overflow_err("years overflow")))?;
		let months = i32::try_from(months)
			.map_err(|_| Box::new(Self::overflow_err("months value out of i32 range")))?;
		Self::normalized(months, 0, 0)
	}

	pub fn zero() -> Self {
		Self {
			months: 0,
			days: 0,
			nanos: 0,
		}
	}

	fn checked_total(
		&self,
		per_year: i64,
		per_month: i64,
		per_day: i64,
		sub_day: i64,
	) -> Result<i64, Box<TypeError>> {
		let years = (self.months / 12) as i64;
		let rem_months = (self.months % 12) as i64;
		years.checked_mul(per_year)
			.and_then(|a| rem_months.checked_mul(per_month).and_then(|b| a.checked_add(b)))
			.and_then(|a| (self.days as i64).checked_mul(per_day).and_then(|b| a.checked_add(b)))
			.and_then(|a| a.checked_add(sub_day))
			.ok_or_else(|| Box::new(Self::overflow_err("duration total overflows i64")))
	}

	pub fn seconds(&self) -> Result<i64, Box<TypeError>> {
		self.checked_total(
			SECONDS_PER_YEAR,
			DAYS_PER_MONTH * SECONDS_PER_DAY,
			SECONDS_PER_DAY,
			self.nanos / 1_000_000_000,
		)
	}

	pub fn milliseconds(&self) -> Result<i64, Box<TypeError>> {
		self.checked_total(
			SECONDS_PER_YEAR * 1_000,
			DAYS_PER_MONTH * SECONDS_PER_DAY * 1_000,
			SECONDS_PER_DAY * 1_000,
			self.nanos / 1_000_000,
		)
	}

	pub fn microseconds(&self) -> Result<i64, Box<TypeError>> {
		self.checked_total(
			SECONDS_PER_YEAR * 1_000_000,
			DAYS_PER_MONTH * SECONDS_PER_DAY * 1_000_000,
			SECONDS_PER_DAY * 1_000_000,
			self.nanos / 1_000,
		)
	}

	pub fn nanoseconds(&self) -> Result<i64, Box<TypeError>> {
		self.checked_total(
			SECONDS_PER_YEAR * 1_000_000_000,
			NANOS_PER_DAY * DAYS_PER_MONTH,
			NANOS_PER_DAY,
			self.nanos,
		)
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

	pub fn as_nanos(&self) -> Result<i64, Box<TypeError>> {
		self.nanoseconds()
	}

	pub fn is_positive(&self) -> bool {
		self.months >= 0
			&& self.days >= 0 && self.nanos >= 0
			&& (self.months > 0 || self.days > 0 || self.nanos > 0)
	}

	pub fn is_negative(&self) -> bool {
		self.months <= 0
			&& self.days <= 0 && self.nanos <= 0
			&& (self.months < 0 || self.days < 0 || self.nanos < 0)
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
		match self.months.cmp(&other.months) {
			cmp::Ordering::Equal => match self.days.cmp(&other.days) {
				cmp::Ordering::Equal => self.nanos.cmp(&other.nanos),
				other_order => other_order,
			},
			other_order => other_order,
		}
	}
}

impl Duration {
	pub fn try_add(self, rhs: Self) -> Result<Self, Box<TypeError>> {
		let months = self
			.months
			.checked_add(rhs.months)
			.ok_or_else(|| Box::new(Self::overflow_err("months overflow in add")))?;
		let days = self
			.days
			.checked_add(rhs.days)
			.ok_or_else(|| Box::new(Self::overflow_err("days overflow in add")))?;
		let nanos = self
			.nanos
			.checked_add(rhs.nanos)
			.ok_or_else(|| Box::new(Self::overflow_err("nanos overflow in add")))?;
		Self::normalized(months, days, nanos)
	}

	pub fn try_sub(self, rhs: Self) -> Result<Self, Box<TypeError>> {
		let months = self
			.months
			.checked_sub(rhs.months)
			.ok_or_else(|| Box::new(Self::overflow_err("months overflow in sub")))?;
		let days = self
			.days
			.checked_sub(rhs.days)
			.ok_or_else(|| Box::new(Self::overflow_err("days overflow in sub")))?;
		let nanos = self
			.nanos
			.checked_sub(rhs.nanos)
			.ok_or_else(|| Box::new(Self::overflow_err("nanos overflow in sub")))?;
		Self::normalized(months, days, nanos)
	}

	pub fn try_mul(self, rhs: i64) -> Result<Self, Box<TypeError>> {
		let rhs_i32 = i32::try_from(rhs)
			.map_err(|_| Box::new(Self::overflow_err("multiplier out of i32 range for months/days")))?;
		let months = self
			.months
			.checked_mul(rhs_i32)
			.ok_or_else(|| Box::new(Self::overflow_err("months overflow in mul")))?;
		let days = self
			.days
			.checked_mul(rhs_i32)
			.ok_or_else(|| Box::new(Self::overflow_err("days overflow in mul")))?;
		let nanos = self
			.nanos
			.checked_mul(rhs)
			.ok_or_else(|| Box::new(Self::overflow_err("nanos overflow in mul")))?;
		Self::normalized(months, days, nanos)
	}

	fn saturating_normalized(months: i32, days: i32, nanos: i64) -> Self {
		let total = days as i128 * NANOS_PER_DAY as i128 + nanos as i128;
		let new_days = (total / NANOS_PER_DAY as i128).clamp(i32::MIN as i128, i32::MAX as i128) as i32;
		let new_nanos = (total % NANOS_PER_DAY as i128) as i64;
		Self {
			months,
			days: new_days,
			nanos: new_nanos,
		}
	}

	pub fn saturating_add(self, rhs: Self) -> Self {
		Self::saturating_normalized(
			self.months.saturating_add(rhs.months),
			self.days.saturating_add(rhs.days),
			self.nanos.saturating_add(rhs.nanos),
		)
	}

	pub fn saturating_sub(self, rhs: Self) -> Self {
		Self::saturating_normalized(
			self.months.saturating_sub(rhs.months),
			self.days.saturating_sub(rhs.days),
			self.nanos.saturating_sub(rhs.nanos),
		)
	}

	pub fn saturating_mul(self, rhs: i64) -> Self {
		let months = (self.months as i128 * rhs as i128).clamp(i32::MIN as i128, i32::MAX as i128) as i32;
		let days = (self.days as i128 * rhs as i128).clamp(i32::MIN as i128, i32::MAX as i128) as i32;
		Self::saturating_normalized(months, days, self.nanos.saturating_mul(rhs))
	}
}

impl ops::Add for Duration {
	type Output = Self;
	fn add(self, rhs: Self) -> Self {
		self.try_add(rhs).expect("duration add overflow")
	}
}

impl ops::Sub for Duration {
	type Output = Self;
	fn sub(self, rhs: Self) -> Self {
		self.try_sub(rhs).expect("duration sub overflow")
	}
}

impl ops::Mul<i64> for Duration {
	type Output = Self;
	fn mul(self, rhs: i64) -> Self {
		self.try_mul(rhs).expect("duration mul overflow")
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
	use crate::error::TemporalKind;

	fn assert_overflow(result: Result<Duration, Box<TypeError>>) {
		let err = result.expect_err("expected DurationOverflow error");
		match *err {
			TypeError::Temporal {
				kind: TemporalKind::DurationOverflow {
					..
				},
				..
			} => {}
			other => panic!("expected DurationOverflow, got: {:?}", other),
		}
	}

	fn assert_mixed_sign(result: Result<Duration, Box<TypeError>>, expected_days: i32, expected_nanos: i64) {
		let err = result.expect_err("expected DurationMixedSign error");
		match *err {
			TypeError::Temporal {
				kind: TemporalKind::DurationMixedSign {
					days,
					nanos,
				},
				..
			} => {
				assert_eq!(days, expected_days, "days mismatch");
				assert_eq!(nanos, expected_nanos, "nanos mismatch");
			}
			other => panic!("expected DurationMixedSign, got: {:?}", other),
		}
	}

	#[test]
	fn test_duration_iso_string_zero() {
		assert_eq!(Duration::zero().to_iso_string(), "PT0S");
		assert_eq!(Duration::from_seconds(0).unwrap().to_iso_string(), "PT0S");
		assert_eq!(Duration::from_nanoseconds(0).unwrap().to_iso_string(), "PT0S");
		assert_eq!(Duration::default().to_iso_string(), "PT0S");
	}

	#[test]
	fn test_duration_iso_string_seconds() {
		assert_eq!(Duration::from_seconds(1).unwrap().to_iso_string(), "PT1S");
		assert_eq!(Duration::from_seconds(30).unwrap().to_iso_string(), "PT30S");
		assert_eq!(Duration::from_seconds(59).unwrap().to_iso_string(), "PT59S");
	}

	#[test]
	fn test_duration_iso_string_minutes() {
		assert_eq!(Duration::from_minutes(1).unwrap().to_iso_string(), "PT1M");
		assert_eq!(Duration::from_minutes(30).unwrap().to_iso_string(), "PT30M");
		assert_eq!(Duration::from_minutes(59).unwrap().to_iso_string(), "PT59M");
	}

	#[test]
	fn test_duration_iso_string_hours() {
		assert_eq!(Duration::from_hours(1).unwrap().to_iso_string(), "PT1H");
		assert_eq!(Duration::from_hours(12).unwrap().to_iso_string(), "PT12H");
		assert_eq!(Duration::from_hours(23).unwrap().to_iso_string(), "PT23H");
	}

	#[test]
	fn test_duration_iso_string_days() {
		assert_eq!(Duration::from_days(1).unwrap().to_iso_string(), "P1D");
		assert_eq!(Duration::from_days(7).unwrap().to_iso_string(), "P7D");
		assert_eq!(Duration::from_days(365).unwrap().to_iso_string(), "P365D");
	}

	#[test]
	fn test_duration_iso_string_weeks() {
		assert_eq!(Duration::from_weeks(1).unwrap().to_iso_string(), "P7D");
		assert_eq!(Duration::from_weeks(2).unwrap().to_iso_string(), "P14D");
		assert_eq!(Duration::from_weeks(52).unwrap().to_iso_string(), "P364D");
	}

	#[test]
	fn test_duration_iso_string_combined_time() {
		let d = Duration::new(0, 0, (1 * 60 * 60 + 30 * 60) * 1_000_000_000).unwrap();
		assert_eq!(d.to_iso_string(), "PT1H30M");

		let d = Duration::new(0, 0, (5 * 60 + 45) * 1_000_000_000).unwrap();
		assert_eq!(d.to_iso_string(), "PT5M45S");

		let d = Duration::new(0, 0, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000).unwrap();
		assert_eq!(d.to_iso_string(), "PT2H30M45S");
	}

	#[test]
	fn test_duration_iso_string_combined_date_time() {
		assert_eq!(Duration::new(0, 1, 2 * 60 * 60 * 1_000_000_000).unwrap().to_iso_string(), "P1DT2H");
		assert_eq!(Duration::new(0, 1, 30 * 60 * 1_000_000_000).unwrap().to_iso_string(), "P1DT30M");
		assert_eq!(
			Duration::new(0, 1, (2 * 60 * 60 + 30 * 60) * 1_000_000_000).unwrap().to_iso_string(),
			"P1DT2H30M"
		);
		assert_eq!(
			Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000).unwrap().to_iso_string(),
			"P1DT2H30M45S"
		);
	}

	#[test]
	fn test_duration_iso_string_milliseconds() {
		assert_eq!(Duration::from_milliseconds(123).unwrap().to_iso_string(), "PT0.123S");
		assert_eq!(Duration::from_milliseconds(1).unwrap().to_iso_string(), "PT0.001S");
		assert_eq!(Duration::from_milliseconds(999).unwrap().to_iso_string(), "PT0.999S");
		assert_eq!(Duration::from_milliseconds(1500).unwrap().to_iso_string(), "PT1.5S");
	}

	#[test]
	fn test_duration_iso_string_microseconds() {
		assert_eq!(Duration::from_microseconds(123456).unwrap().to_iso_string(), "PT0.123456S");
		assert_eq!(Duration::from_microseconds(1).unwrap().to_iso_string(), "PT0.000001S");
		assert_eq!(Duration::from_microseconds(999999).unwrap().to_iso_string(), "PT0.999999S");
		assert_eq!(Duration::from_microseconds(1500000).unwrap().to_iso_string(), "PT1.5S");
	}

	#[test]
	fn test_duration_iso_string_nanoseconds() {
		assert_eq!(Duration::from_nanoseconds(123456789).unwrap().to_iso_string(), "PT0.123456789S");
		assert_eq!(Duration::from_nanoseconds(1).unwrap().to_iso_string(), "PT0.000000001S");
		assert_eq!(Duration::from_nanoseconds(999999999).unwrap().to_iso_string(), "PT0.999999999S");
		assert_eq!(Duration::from_nanoseconds(1500000000).unwrap().to_iso_string(), "PT1.5S");
	}

	#[test]
	fn test_duration_iso_string_fractional_seconds() {
		let d = Duration::new(0, 0, 1 * 1_000_000_000 + 500 * 1_000_000).unwrap();
		assert_eq!(d.to_iso_string(), "PT1.5S");

		let d = Duration::new(0, 0, 2 * 1_000_000_000 + 123456 * 1_000).unwrap();
		assert_eq!(d.to_iso_string(), "PT2.123456S");

		let d = Duration::new(0, 0, 3 * 1_000_000_000 + 123456789).unwrap();
		assert_eq!(d.to_iso_string(), "PT3.123456789S");
	}

	#[test]
	fn test_duration_iso_string_complex() {
		let d = Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000 + 123 * 1_000_000).unwrap();
		assert_eq!(d.to_iso_string(), "P1DT2H30M45.123S");

		let d = Duration::new(0, 7, (12 * 60 * 60 + 45 * 60 + 30) * 1_000_000_000 + 456789 * 1_000).unwrap();
		assert_eq!(d.to_iso_string(), "P7DT12H45M30.456789S");
	}

	#[test]
	fn test_duration_iso_string_trailing_zeros() {
		assert_eq!(Duration::from_nanoseconds(100000000).unwrap().to_iso_string(), "PT0.1S");
		assert_eq!(Duration::from_nanoseconds(120000000).unwrap().to_iso_string(), "PT0.12S");
		assert_eq!(Duration::from_nanoseconds(123000000).unwrap().to_iso_string(), "PT0.123S");
		assert_eq!(Duration::from_nanoseconds(123400000).unwrap().to_iso_string(), "PT0.1234S");
		assert_eq!(Duration::from_nanoseconds(123450000).unwrap().to_iso_string(), "PT0.12345S");
		assert_eq!(Duration::from_nanoseconds(123456000).unwrap().to_iso_string(), "PT0.123456S");
		assert_eq!(Duration::from_nanoseconds(123456700).unwrap().to_iso_string(), "PT0.1234567S");
		assert_eq!(Duration::from_nanoseconds(123456780).unwrap().to_iso_string(), "PT0.12345678S");
		assert_eq!(Duration::from_nanoseconds(123456789).unwrap().to_iso_string(), "PT0.123456789S");
	}

	#[test]
	fn test_duration_iso_string_negative() {
		assert_eq!(Duration::from_seconds(-30).unwrap().to_iso_string(), "PT-30S");
		assert_eq!(Duration::from_minutes(-5).unwrap().to_iso_string(), "PT-5M");
		assert_eq!(Duration::from_hours(-2).unwrap().to_iso_string(), "PT-2H");
		assert_eq!(Duration::from_days(-1).unwrap().to_iso_string(), "P-1D");
	}

	#[test]
	fn test_duration_iso_string_large() {
		assert_eq!(Duration::from_days(1000).unwrap().to_iso_string(), "P1000D");
		assert_eq!(Duration::from_hours(25).unwrap().to_iso_string(), "P1DT1H");
		assert_eq!(Duration::from_minutes(1500).unwrap().to_iso_string(), "P1DT1H");
		assert_eq!(Duration::from_seconds(90000).unwrap().to_iso_string(), "P1DT1H");
	}

	#[test]
	fn test_duration_iso_string_edge_cases() {
		assert_eq!(Duration::from_nanoseconds(1).unwrap().to_iso_string(), "PT0.000000001S");
		assert_eq!(Duration::from_nanoseconds(999999999).unwrap().to_iso_string(), "PT0.999999999S");
		assert_eq!(Duration::from_nanoseconds(1000000000).unwrap().to_iso_string(), "PT1S");
		assert_eq!(Duration::from_nanoseconds(60 * 1000000000).unwrap().to_iso_string(), "PT1M");
		assert_eq!(Duration::from_nanoseconds(3600 * 1000000000).unwrap().to_iso_string(), "PT1H");
		assert_eq!(Duration::from_nanoseconds(86400 * 1000000000).unwrap().to_iso_string(), "P1D");
	}

	#[test]
	fn test_duration_iso_string_precision() {
		assert_eq!(Duration::from_nanoseconds(100).unwrap().to_iso_string(), "PT0.0000001S");
		assert_eq!(Duration::from_nanoseconds(10).unwrap().to_iso_string(), "PT0.00000001S");
		assert_eq!(Duration::from_nanoseconds(1).unwrap().to_iso_string(), "PT0.000000001S");
	}

	#[test]
	fn test_duration_display_zero() {
		assert_eq!(format!("{}", Duration::zero()), "0s");
		assert_eq!(format!("{}", Duration::from_seconds(0).unwrap()), "0s");
		assert_eq!(format!("{}", Duration::from_nanoseconds(0).unwrap()), "0s");
		assert_eq!(format!("{}", Duration::default()), "0s");
	}

	#[test]
	fn test_duration_display_seconds_only() {
		assert_eq!(format!("{}", Duration::from_seconds(1).unwrap()), "1s");
		assert_eq!(format!("{}", Duration::from_seconds(30).unwrap()), "30s");
		assert_eq!(format!("{}", Duration::from_seconds(59).unwrap()), "59s");
	}

	#[test]
	fn test_duration_display_minutes_only() {
		assert_eq!(format!("{}", Duration::from_minutes(1).unwrap()), "1m");
		assert_eq!(format!("{}", Duration::from_minutes(30).unwrap()), "30m");
		assert_eq!(format!("{}", Duration::from_minutes(59).unwrap()), "59m");
	}

	#[test]
	fn test_duration_display_hours_only() {
		assert_eq!(format!("{}", Duration::from_hours(1).unwrap()), "1h");
		assert_eq!(format!("{}", Duration::from_hours(12).unwrap()), "12h");
		assert_eq!(format!("{}", Duration::from_hours(23).unwrap()), "23h");
	}

	#[test]
	fn test_duration_display_days_only() {
		assert_eq!(format!("{}", Duration::from_days(1).unwrap()), "1d");
		assert_eq!(format!("{}", Duration::from_days(7).unwrap()), "7d");
		assert_eq!(format!("{}", Duration::from_days(365).unwrap()), "365d");
	}

	#[test]
	fn test_duration_display_weeks_only() {
		assert_eq!(format!("{}", Duration::from_weeks(1).unwrap()), "7d");
		assert_eq!(format!("{}", Duration::from_weeks(2).unwrap()), "14d");
		assert_eq!(format!("{}", Duration::from_weeks(52).unwrap()), "364d");
	}

	#[test]
	fn test_duration_display_months_only() {
		assert_eq!(format!("{}", Duration::from_months(1).unwrap()), "1mo");
		assert_eq!(format!("{}", Duration::from_months(6).unwrap()), "6mo");
		assert_eq!(format!("{}", Duration::from_months(11).unwrap()), "11mo");
	}

	#[test]
	fn test_duration_display_years_only() {
		assert_eq!(format!("{}", Duration::from_years(1).unwrap()), "1y");
		assert_eq!(format!("{}", Duration::from_years(10).unwrap()), "10y");
		assert_eq!(format!("{}", Duration::from_years(100).unwrap()), "100y");
	}

	#[test]
	fn test_duration_display_combined_time() {
		let d = Duration::new(0, 0, (1 * 60 * 60 + 30 * 60) * 1_000_000_000).unwrap();
		assert_eq!(format!("{}", d), "1h30m");

		let d = Duration::new(0, 0, (5 * 60 + 45) * 1_000_000_000).unwrap();
		assert_eq!(format!("{}", d), "5m45s");

		let d = Duration::new(0, 0, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000).unwrap();
		assert_eq!(format!("{}", d), "2h30m45s");
	}

	#[test]
	fn test_duration_display_combined_date_time() {
		assert_eq!(format!("{}", Duration::new(0, 1, 2 * 60 * 60 * 1_000_000_000).unwrap()), "1d2h");
		assert_eq!(format!("{}", Duration::new(0, 1, 30 * 60 * 1_000_000_000).unwrap()), "1d30m");
		assert_eq!(
			format!("{}", Duration::new(0, 1, (2 * 60 * 60 + 30 * 60) * 1_000_000_000).unwrap()),
			"1d2h30m"
		);
		assert_eq!(
			format!("{}", Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000).unwrap()),
			"1d2h30m45s"
		);
	}

	#[test]
	fn test_duration_display_years_months() {
		assert_eq!(format!("{}", Duration::new(13, 0, 0).unwrap()), "1y1mo");
		assert_eq!(format!("{}", Duration::new(27, 0, 0).unwrap()), "2y3mo");
	}

	#[test]
	fn test_duration_display_full_components() {
		let nanos = (4 * 60 * 60 + 5 * 60 + 6) * 1_000_000_000i64;
		assert_eq!(format!("{}", Duration::new(14, 3, nanos).unwrap()), "1y2mo3d4h5m6s");
	}

	#[test]
	fn test_duration_display_milliseconds() {
		assert_eq!(format!("{}", Duration::from_milliseconds(123).unwrap()), "123ms");
		assert_eq!(format!("{}", Duration::from_milliseconds(1).unwrap()), "1ms");
		assert_eq!(format!("{}", Duration::from_milliseconds(999).unwrap()), "999ms");
		assert_eq!(format!("{}", Duration::from_milliseconds(1500).unwrap()), "1s500ms");
	}

	#[test]
	fn test_duration_display_microseconds() {
		assert_eq!(format!("{}", Duration::from_microseconds(123456).unwrap()), "123ms456us");
		assert_eq!(format!("{}", Duration::from_microseconds(1).unwrap()), "1us");
		assert_eq!(format!("{}", Duration::from_microseconds(999999).unwrap()), "999ms999us");
		assert_eq!(format!("{}", Duration::from_microseconds(1500000).unwrap()), "1s500ms");
	}

	#[test]
	fn test_duration_display_nanoseconds() {
		assert_eq!(format!("{}", Duration::from_nanoseconds(123456789).unwrap()), "123ms456us789ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(1).unwrap()), "1ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(999999999).unwrap()), "999ms999us999ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(1500000000).unwrap()), "1s500ms");
	}

	#[test]
	fn test_duration_display_sub_second_decomposition() {
		let d = Duration::new(0, 0, 1 * 1_000_000_000 + 500 * 1_000_000).unwrap();
		assert_eq!(format!("{}", d), "1s500ms");

		let d = Duration::new(0, 0, 2 * 1_000_000_000 + 123456 * 1_000).unwrap();
		assert_eq!(format!("{}", d), "2s123ms456us");

		let d = Duration::new(0, 0, 3 * 1_000_000_000 + 123456789).unwrap();
		assert_eq!(format!("{}", d), "3s123ms456us789ns");
	}

	#[test]
	fn test_duration_display_complex() {
		let d = Duration::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000 + 123 * 1_000_000).unwrap();
		assert_eq!(format!("{}", d), "1d2h30m45s123ms");

		let d = Duration::new(0, 7, (12 * 60 * 60 + 45 * 60 + 30) * 1_000_000_000 + 456789 * 1_000).unwrap();
		assert_eq!(format!("{}", d), "7d12h45m30s456ms789us");
	}

	#[test]
	fn test_duration_display_sub_second_only() {
		assert_eq!(format!("{}", Duration::from_nanoseconds(100000000).unwrap()), "100ms");
		assert_eq!(format!("{}", Duration::from_nanoseconds(120000000).unwrap()), "120ms");
		assert_eq!(format!("{}", Duration::from_nanoseconds(123000000).unwrap()), "123ms");
		assert_eq!(format!("{}", Duration::from_nanoseconds(100).unwrap()), "100ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(10).unwrap()), "10ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(1000).unwrap()), "1us");
	}

	#[test]
	fn test_duration_display_negative() {
		assert_eq!(format!("{}", Duration::from_seconds(-30).unwrap()), "-30s");
		assert_eq!(format!("{}", Duration::from_minutes(-5).unwrap()), "-5m");
		assert_eq!(format!("{}", Duration::from_hours(-2).unwrap()), "-2h");
		assert_eq!(format!("{}", Duration::from_days(-1).unwrap()), "-1d");
	}

	#[test]
	fn test_duration_display_negative_sub_second() {
		assert_eq!(format!("{}", Duration::from_milliseconds(-500).unwrap()), "-500ms");
		assert_eq!(format!("{}", Duration::from_microseconds(-100).unwrap()), "-100us");
		assert_eq!(format!("{}", Duration::from_nanoseconds(-50).unwrap()), "-50ns");
	}

	#[test]
	fn test_duration_display_large() {
		assert_eq!(format!("{}", Duration::from_days(1000).unwrap()), "1000d");
		assert_eq!(format!("{}", Duration::from_hours(25).unwrap()), "1d1h");
		assert_eq!(format!("{}", Duration::from_minutes(1500).unwrap()), "1d1h");
		assert_eq!(format!("{}", Duration::from_seconds(90000).unwrap()), "1d1h");
	}

	#[test]
	fn test_duration_display_edge_cases() {
		assert_eq!(format!("{}", Duration::from_nanoseconds(1).unwrap()), "1ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(999999999).unwrap()), "999ms999us999ns");
		assert_eq!(format!("{}", Duration::from_nanoseconds(1000000000).unwrap()), "1s");
		assert_eq!(format!("{}", Duration::from_nanoseconds(60 * 1000000000).unwrap()), "1m");
		assert_eq!(format!("{}", Duration::from_nanoseconds(3600 * 1000000000).unwrap()), "1h");
		assert_eq!(format!("{}", Duration::from_nanoseconds(86400 * 1000000000).unwrap()), "1d");
	}

	#[test]
	fn test_duration_display_abs_and_negate() {
		let d = Duration::from_seconds(-30).unwrap();
		assert_eq!(format!("{}", d.abs()), "30s");

		let d = Duration::from_seconds(30).unwrap();
		assert_eq!(format!("{}", d.negate()), "-30s");
	}

	#[test]
	fn test_nanos_normalize_to_days() {
		let d = Duration::new(0, 0, 86_400_000_000_000).unwrap();
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.get_nanos(), 0);
	}

	#[test]
	fn test_nanos_normalize_to_days_with_remainder() {
		let d = Duration::new(0, 0, 86_400_000_000_000 + 1_000_000_000).unwrap();
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.get_nanos(), 1_000_000_000);
	}

	#[test]
	fn test_nanos_normalize_negative() {
		let d = Duration::new(0, 0, -86_400_000_000_000).unwrap();
		assert_eq!(d.get_days(), -1);
		assert_eq!(d.get_nanos(), 0);
	}

	#[test]
	fn test_normalized_equality() {
		let d1 = Duration::new(0, 0, 86_400_000_000_000).unwrap();
		let d2 = Duration::new(0, 1, 0).unwrap();
		assert_eq!(d1, d2);
	}

	#[test]
	fn test_normalized_ordering() {
		let d1 = Duration::new(0, 0, 86_400_000_000_000 + 1).unwrap();
		let d2 = Duration::new(0, 1, 0).unwrap();
		assert!(d1 > d2);
	}

	// Months may differ in sign from days/nanos (months are variable-length).
	// Days and nanos must share the same sign (they are commensurable).

	#[test]
	fn test_mixed_sign_months_days_allowed() {
		let d = Duration::new(1, -15, 0).unwrap();
		assert_eq!(d.get_months(), 1);
		assert_eq!(d.get_days(), -15);
	}

	#[test]
	fn test_mixed_sign_months_nanos_allowed() {
		let d = Duration::new(-1, 0, 1_000_000_000).unwrap();
		assert_eq!(d.get_months(), -1);
		assert_eq!(d.get_nanos(), 1_000_000_000);
	}

	#[test]
	fn test_mixed_sign_days_positive_nanos_negative() {
		assert_mixed_sign(Duration::new(0, 1, -1), 1, -1);
	}

	#[test]
	fn test_mixed_sign_days_negative_nanos_positive() {
		assert_mixed_sign(Duration::new(0, -1, 1), -1, 1);
	}

	#[test]
	fn test_is_positive_negative_mutually_exclusive() {
		let durations = [
			Duration::new(1, 0, 0).unwrap(),
			Duration::new(0, 1, 0).unwrap(),
			Duration::new(0, 0, 1).unwrap(),
			Duration::new(-1, 0, 0).unwrap(),
			Duration::new(0, -1, 0).unwrap(),
			Duration::new(0, 0, -1).unwrap(),
			Duration::new(1, 1, 1).unwrap(),
			Duration::new(-1, -1, -1).unwrap(),
			Duration::new(1, -15, 0).unwrap(), // mixed months/days
			Duration::new(-1, 15, 0).unwrap(), // mixed months/days
			Duration::zero(),
		];
		for d in durations {
			assert!(
				!(d.is_positive() && d.is_negative()),
				"Duration {:?} is both positive and negative",
				d
			);
		}
	}

	#[test]
	fn test_mixed_months_days_is_neither_positive_nor_negative() {
		let d = Duration::new(1, -15, 0).unwrap();
		assert!(!d.is_positive());
		assert!(!d.is_negative());
	}

	#[test]
	fn test_from_days_overflow() {
		assert_overflow(Duration::from_days(i32::MAX as i64 + 1));
	}

	#[test]
	fn test_months_positive_days_negative_ok() {
		let d = Duration::new(1, -15, 0).unwrap();
		assert_eq!(d.get_months(), 1);
		assert_eq!(d.get_days(), -15);
		assert_eq!(d.get_nanos(), 0);
	}

	#[test]
	fn test_months_negative_days_positive_ok() {
		let d = Duration::new(-1, 15, 0).unwrap();
		assert_eq!(d.get_months(), -1);
		assert_eq!(d.get_days(), 15);
	}

	#[test]
	fn test_months_positive_nanos_negative_ok() {
		let d = Duration::new(1, 0, -1_000_000_000).unwrap();
		assert_eq!(d.get_months(), 1);
		assert_eq!(d.get_nanos(), -1_000_000_000);
	}

	#[test]
	fn test_months_negative_nanos_positive_ok() {
		let d = Duration::new(-1, 0, 1_000_000_000).unwrap();
		assert_eq!(d.get_months(), -1);
		assert_eq!(d.get_nanos(), 1_000_000_000);
	}

	#[test]
	fn test_months_positive_days_negative_nanos_negative_ok() {
		let d = Duration::new(2, -3, -1_000_000_000).unwrap();
		assert_eq!(d.get_months(), 2);
		assert_eq!(d.get_days(), -3);
		assert_eq!(d.get_nanos(), -1_000_000_000);
	}

	#[test]
	fn test_months_negative_days_positive_nanos_positive_ok() {
		let d = Duration::new(-2, 3, 1_000_000_000).unwrap();
		assert_eq!(d.get_months(), -2);
		assert_eq!(d.get_days(), 3);
		assert_eq!(d.get_nanos(), 1_000_000_000);
	}

	#[test]
	fn test_days_positive_nanos_negative_with_months_err() {
		assert_mixed_sign(Duration::new(5, 1, -1), 1, -1);
	}

	#[test]
	fn test_days_negative_nanos_positive_with_months_err() {
		assert_mixed_sign(Duration::new(-5, -1, 1), -1, 1);
	}

	#[test]
	fn test_nanos_normalization_causes_days_nanos_mixed_sign_err() {
		// 2 days of nanos + 1 extra, with days=-3 → after normalization days=-1, nanos=1
		assert_mixed_sign(Duration::new(0, -3, 2 * 86_400_000_000_000 + 1), -1, 1);
	}

	#[test]
	fn test_positive_months_negative_days_is_neither() {
		let d = Duration::new(1, -15, 0).unwrap();
		assert!(!d.is_positive());
		assert!(!d.is_negative());
	}

	#[test]
	fn test_negative_months_positive_days_is_neither() {
		let d = Duration::new(-1, 15, 0).unwrap();
		assert!(!d.is_positive());
		assert!(!d.is_negative());
	}

	#[test]
	fn test_positive_months_negative_days_negative_nanos_is_neither() {
		let d = Duration::new(2, -3, -1_000_000_000).unwrap();
		assert!(!d.is_positive());
		assert!(!d.is_negative());
	}

	#[test]
	fn test_all_positive_is_positive() {
		let d = Duration::new(1, 2, 3).unwrap();
		assert!(d.is_positive());
		assert!(!d.is_negative());
	}

	#[test]
	fn test_all_negative_is_negative() {
		let d = Duration::new(-1, -2, -3).unwrap();
		assert!(!d.is_positive());
		assert!(d.is_negative());
	}

	#[test]
	fn test_zero_is_neither_positive_nor_negative() {
		assert!(!Duration::zero().is_positive());
		assert!(!Duration::zero().is_negative());
	}

	#[test]
	fn test_only_months_positive() {
		let d = Duration::new(1, 0, 0).unwrap();
		assert!(d.is_positive());
	}

	#[test]
	fn test_only_days_negative() {
		let d = Duration::new(0, -1, 0).unwrap();
		assert!(d.is_negative());
	}

	#[test]
	fn test_normalization_nanos_into_negative_days() {
		let d = Duration::new(-5, 0, -2 * 86_400_000_000_000).unwrap();
		assert_eq!(d.get_months(), -5);
		assert_eq!(d.get_days(), -2);
		assert_eq!(d.get_nanos(), 0);
	}

	#[test]
	fn test_normalization_nanos_into_days_with_mixed_months() {
		let d = Duration::new(3, 1, 86_400_000_000_000 + 500_000_000).unwrap();
		assert_eq!(d.get_months(), 3);
		assert_eq!(d.get_days(), 2);
		assert_eq!(d.get_nanos(), 500_000_000);
	}

	#[test]
	fn test_try_sub_month_minus_days() {
		let a = Duration::new(1, 0, 0).unwrap();
		let b = Duration::new(0, 15, 0).unwrap();
		let result = a.try_sub(b).unwrap();
		assert_eq!(result.get_months(), 1);
		assert_eq!(result.get_days(), -15);
	}

	#[test]
	fn test_try_sub_day_minus_month() {
		let a = Duration::new(0, 1, 0).unwrap();
		let b = Duration::new(1, 0, 0).unwrap();
		let result = a.try_sub(b).unwrap();
		assert_eq!(result.get_months(), -1);
		assert_eq!(result.get_days(), 1);
	}

	#[test]
	fn test_try_add_mixed_months_days() {
		let a = Duration::new(2, -10, 0).unwrap();
		let b = Duration::new(-1, -5, 0).unwrap();
		let result = a.try_add(b).unwrap();
		assert_eq!(result.get_months(), 1);
		assert_eq!(result.get_days(), -15);
	}

	#[test]
	fn test_try_sub_days_nanos_mixed_sign_err() {
		let a = Duration::new(0, 1, 0).unwrap();
		let b = Duration::new(0, 0, 1).unwrap();
		// 1 day - 1 nano = days=1, nanos=-1 → mixed days/nanos sign error
		assert_mixed_sign(a.try_sub(b), 1, -1);
	}

	#[test]
	fn test_try_mul_preserves_mixed_months() {
		let d = Duration::new(1, -3, 0).unwrap();
		let result = d.try_mul(2).unwrap();
		assert_eq!(result.get_months(), 2);
		assert_eq!(result.get_days(), -6);
	}

	#[test]
	fn test_from_days_underflow() {
		assert_overflow(Duration::from_days(i32::MIN as i64 - 1));
	}

	#[test]
	fn test_from_months_overflow() {
		assert_overflow(Duration::from_months(i32::MAX as i64 + 1));
	}

	#[test]
	fn test_from_years_overflow() {
		assert_overflow(Duration::from_years(i32::MAX as i64 / 12 + 1));
	}

	#[test]
	fn test_from_weeks_overflow() {
		assert_overflow(Duration::from_weeks(i32::MAX as i64 / 7 + 1));
	}

	#[test]
	fn test_mul_months_truncation() {
		let d = Duration::from_months(1).unwrap();
		assert_overflow(d.try_mul(i32::MAX as i64 + 1));
	}

	#[test]
	fn test_mul_days_truncation() {
		let d = Duration::from_days(1).unwrap();
		assert_overflow(d.try_mul(i32::MAX as i64 + 1));
	}

	fn assert_total_overflow(result: Result<i64, Box<TypeError>>) {
		let err = result.expect_err("expected DurationOverflow error");
		match *err {
			TypeError::Temporal {
				kind: TemporalKind::DurationOverflow {
					..
				},
				..
			} => {}
			other => panic!("expected DurationOverflow, got: {:?}", other),
		}
	}

	#[test]
	fn test_total_seconds_roundtrips_across_day_boundary() {
		// from_seconds(90_000) normalizes to days=1 + 3600s; .seconds() must report
		// the full 90_000, not just the sub-day remainder. This is the core bug:
		// before the fix the days field was ignored and this returned 3600.
		let d = Duration::from_seconds(90_000).unwrap();
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.seconds().unwrap(), 90_000);
	}

	#[test]
	fn test_total_milliseconds_roundtrips_across_day_boundary() {
		let d = Duration::from_milliseconds(90_000_000).unwrap();
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.milliseconds().unwrap(), 90_000_000);
	}

	#[test]
	fn test_total_microseconds_roundtrips_across_day_boundary() {
		let d = Duration::from_microseconds(90_000_000_000).unwrap();
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.microseconds().unwrap(), 90_000_000_000);
	}

	#[test]
	fn test_total_nanoseconds_roundtrips_across_day_boundary() {
		let d = Duration::from_nanoseconds(90_000_000_000_000).unwrap();
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.nanoseconds().unwrap(), 90_000_000_000_000);
		assert_eq!(d.as_nanos().unwrap(), 90_000_000_000_000);
	}

	#[test]
	fn test_total_from_minutes_crossing_day() {
		// 1500 minutes = 90_000 s = 1 day + 3600 s; the day must be counted.
		let d = Duration::from_minutes(1_500).unwrap();
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.seconds().unwrap(), 90_000);
	}

	#[test]
	fn test_total_from_hours_crossing_day() {
		// from_hours(25) was the motivating example: it normalizes to days=1 and
		// must report 90_000 s, not 3600.
		let d = Duration::from_hours(25).unwrap();
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.seconds().unwrap(), 90_000);
	}

	#[test]
	fn test_total_from_days_counts_days() {
		let d = Duration::from_days(3).unwrap();
		assert_eq!(d.seconds().unwrap(), 3 * 86_400);
		assert_eq!(d.nanoseconds().unwrap(), 3 * NANOS_PER_DAY);
	}

	#[test]
	fn test_total_from_weeks_counts_days() {
		let d = Duration::from_weeks(2).unwrap();
		assert_eq!(d.get_days(), 14);
		assert_eq!(d.seconds().unwrap(), 14 * 86_400);
	}

	#[test]
	fn test_total_from_months_uses_thirty_days() {
		// Residual months (< 12) count as 30 days each.
		let d = Duration::from_months(5).unwrap();
		assert_eq!(d.get_months(), 5);
		assert_eq!(d.seconds().unwrap(), 5 * 30 * 86_400);
	}

	#[test]
	fn test_total_from_years_uses_three_six_five_quarter_days() {
		// Whole years count as 365.25 days each (Postgres EXTRACT(EPOCH) convention),
		// stored as 12 months. A bare-days duration of 365 days is intentionally
		// different from one year, so the two must NOT be equal.
		let one_year = Duration::from_years(1).unwrap();
		assert_eq!(one_year.get_months(), 12);
		assert_eq!(one_year.seconds().unwrap(), 31_557_600);

		let three_sixty_five_days = Duration::from_days(365).unwrap();
		assert_eq!(three_sixty_five_days.seconds().unwrap(), 31_536_000);
		assert_ne!(one_year.seconds().unwrap(), three_sixty_five_days.seconds().unwrap());

		assert_eq!(Duration::from_years(2).unwrap().seconds().unwrap(), 2 * 31_557_600);
	}

	#[test]
	fn test_total_months_split_into_years_and_residual() {
		// 13 months = 1 whole year (365.25d) + 1 residual month (30d).
		let d = Duration::from_months(13).unwrap();
		assert_eq!(d.seconds().unwrap(), 31_557_600 + 30 * 86_400);
	}

	#[test]
	fn test_total_new_combined_mixed_sign() {
		// months and days may carry opposite signs (months are variable-length and
		// are not commensurable with days/nanos). The total must accumulate them with
		// their signs: +1 month (30d) minus 5 days.
		let d = Duration::new(1, -5, 0).unwrap();
		assert_eq!(d.seconds().unwrap(), 30 * 86_400 - 5 * 86_400);
	}

	#[test]
	fn test_total_from_micros_infallible_roundtrips() {
		// from_micros_infallible distributes whole days into the days field; the
		// microsecond total must reconstruct the original input.
		let micros: u64 = 90_000_000_000;
		let d = Duration::from_micros_infallible(micros);
		assert_eq!(d.get_days(), 1);
		assert_eq!(d.microseconds().unwrap(), micros as i64);
	}

	#[test]
	fn test_total_zero_is_zero_in_every_unit() {
		let d = Duration::zero();
		assert_eq!(d.seconds().unwrap(), 0);
		assert_eq!(d.milliseconds().unwrap(), 0);
		assert_eq!(d.microseconds().unwrap(), 0);
		assert_eq!(d.nanoseconds().unwrap(), 0);
		assert_eq!(d.as_nanos().unwrap(), 0);
	}

	#[test]
	fn test_total_overflow_fails_loud_per_unit() {
		// A huge day count overflows i64 nanoseconds but not i64 seconds. Overflow
		// must surface as an error rather than wrapping silently, and each unit is
		// checked independently so seconds stays valid where nanoseconds cannot.
		let d = Duration::new(0, i32::MAX, 0).unwrap();
		assert_eq!(d.seconds().unwrap(), i32::MAX as i64 * 86_400);
		assert_total_overflow(d.nanoseconds());
		assert_total_overflow(d.as_nanos());
	}

	#[test]
	fn saturating_add_matches_try_add_in_range() {
		// In range, saturating arithmetic must agree exactly with the checked
		// (try_*) path; saturation only kicks in at the i32/i64 boundaries.
		let a = Duration::new(1, 2, 3_000_000_000).unwrap();
		let b = Duration::new(0, 1, 1_000_000_000).unwrap();
		assert_eq!(a.saturating_add(b), a.try_add(b).unwrap());
		assert_eq!(a.saturating_sub(b), a.try_sub(b).unwrap());
	}

	#[test]
	fn saturating_add_clamps_months_overflow() {
		// months past i32::MAX clamp to i32::MAX instead of panicking/erroring.
		let d = Duration::new(i32::MAX, 0, 0).unwrap().saturating_add(Duration::new(1000, 0, 0).unwrap());
		assert_eq!(d.get_months(), i32::MAX);
	}

	#[test]
	fn saturating_add_clamps_days_overflow() {
		// days past i32::MAX clamp to i32::MAX.
		let d = Duration::new(0, i32::MAX, 0).unwrap().saturating_add(Duration::new(0, 1000, 0).unwrap());
		assert_eq!(d.get_days(), i32::MAX);
	}

	#[test]
	fn saturating_sub_carries_mixed_sign() {
		// 1 day minus 1 nanosecond = 23:59:59.999999999. This is exactly the
		// mixed days/nanos sign case that try_sub REJECTS (days=1, nanos=-1);
		// saturating arithmetic carries it into a single same-sign value instead.
		let d = Duration::new(0, 1, 0).unwrap().saturating_sub(Duration::new(0, 0, 1).unwrap());
		assert_eq!(d.get_days(), 0);
		assert_eq!(d.get_nanos(), 86_399_999_999_999);
	}

	#[test]
	fn saturating_mul_scales_and_clamps() {
		// In range, scaling matches the equivalent constructed duration.
		assert_eq!(Duration::from_seconds(1).unwrap().saturating_mul(60), Duration::from_seconds(60).unwrap());
		// Out of range, the day product clamps to i32::MAX.
		assert_eq!(Duration::new(0, i32::MAX, 0).unwrap().saturating_mul(2).get_days(), i32::MAX);
	}
}
