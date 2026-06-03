// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Display, Formatter},
	ops::{Add, Rem, Sub},
	str::FromStr,
};

use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

use crate::{
	clock::ClockNow,
	error::{Error, TemporalKind, TypeError},
	fragment::Fragment,
	value::{date::Date, duration::Duration, temporal::parse::datetime::parse_datetime, time::Time},
};

const NANOS_PER_SECOND: u64 = 1_000_000_000;
const NANOS_PER_MILLI: u64 = 1_000_000;
const NANOS_PER_DAY: u64 = 86_400 * NANOS_PER_SECOND;

pub static CREATED_AT_COLUMN_NAME: &str = "created_at";
pub static UPDATED_AT_COLUMN_NAME: &str = "updated_at";

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct DateTime {
	nanos: u64,
}

impl DateTime {
	pub fn new(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
		let date = Date::new(year, month, day)?;
		let time = Time::new(hour, min, sec, nano)?;

		let days = date.to_days_since_epoch();
		if days < 0 {
			return None;
		}

		let nanos = (days as u64).checked_mul(NANOS_PER_DAY)?.checked_add(time.to_nanos_since_midnight())?;
		Some(Self {
			nanos,
		})
	}

	pub fn from_ymd_hms(
		year: i32,
		month: u32,
		day: u32,
		hour: u32,
		min: u32,
		sec: u32,
	) -> Result<Self, Box<TypeError>> {
		Self::new(year, month, day, hour, min, sec, 0).ok_or_else(|| {
			Box::new(Self::overflow_err(format!(
				"invalid datetime: {}-{:02}-{:02} {:02}:{:02}:{:02}",
				year, month, day, hour, min, sec
			)))
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

	pub fn from_nanos(nanos: u64) -> Self {
		Self {
			nanos,
		}
	}

	pub fn to_nanos(&self) -> u64 {
		self.nanos
	}

	pub fn from_timestamp(timestamp: i64) -> Result<Self, Box<TypeError>> {
		if timestamp < 0 {
			return Err(Box::new(Self::overflow_err(format!(
				"DateTime does not support timestamps before Unix epoch: {}",
				timestamp
			))));
		}
		let nanos = (timestamp as u64).checked_mul(NANOS_PER_SECOND).ok_or_else(|| {
			Box::new(Self::overflow_err(format!("timestamp {} overflows DateTime range", timestamp)))
		})?;
		Ok(Self {
			nanos,
		})
	}

	pub fn from_timestamp_millis(millis: u64) -> Result<Self, Box<TypeError>> {
		let nanos = millis.checked_mul(NANOS_PER_MILLI).ok_or_else(|| {
			Box::new(Self::overflow_err(format!("timestamp_millis {} overflows DateTime range", millis)))
		})?;
		Ok(Self {
			nanos,
		})
	}

	pub fn from_timestamp_nanos(nanos: u128) -> Result<Self, Box<TypeError>> {
		let nanos = u64::try_from(nanos).map_err(|_| {
			Box::new(Self::overflow_err(format!("timestamp_nanos {} overflows u64 DateTime range", nanos)))
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

	pub fn timestamp_nanos(&self) -> Result<i64, Box<TypeError>> {
		i64::try_from(self.nanos).map_err(|_| Box::new(Self::overflow_err("DateTime nanos exceeds i64::MAX")))
	}

	pub fn try_date(&self) -> Result<Date, Box<TypeError>> {
		let days_u64 = self.nanos / NANOS_PER_DAY;
		let days = i32::try_from(days_u64)
			.map_err(|_| Box::new(Self::overflow_err("DateTime nanos too large for date extraction")))?;
		Date::from_days_since_epoch(days)
			.ok_or_else(|| Box::new(Self::overflow_err("DateTime days out of range for Date")))
	}

	pub fn date(&self) -> Date {
		self.try_date().expect("DateTime nanos too large for date extraction")
	}

	pub fn time(&self) -> Time {
		let nanos_in_day = self.nanos % NANOS_PER_DAY;
		Time::from_nanos_since_midnight(nanos_in_day).unwrap()
	}

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

	pub fn add_duration(&self, dur: &Duration) -> Result<Self, Box<TypeError>> {
		let date = self.date();
		let time = self.time();
		let mut year = date.year();
		let mut month = date.month() as i32;
		let mut day = date.day();

		let total_months = month + dur.get_months();
		year += (total_months - 1).div_euclid(12);
		month = (total_months - 1).rem_euclid(12) + 1;

		let max_day = Date::days_in_month(year, month as u32);
		if day > max_day {
			day = max_day;
		}

		let base_date = Date::new(year, month as u32, day).ok_or_else(|| {
			Box::new(Self::overflow_err(format!(
				"invalid date after adding duration: {}-{:02}-{:02}",
				year, month, day
			)))
		})?;
		let base_days = base_date.to_days_since_epoch() as i64 + dur.get_days() as i64;
		let time_nanos = time.to_nanos_since_midnight() as i64 + dur.get_nanos();

		let total_nanos = base_days as i128 * 86_400_000_000_000i128 + time_nanos as i128;

		if total_nanos < 0 {
			return Err(Box::new(Self::overflow_err("result is before Unix epoch")));
		}

		let nanos = u64::try_from(total_nanos)
			.map_err(|_| Box::new(Self::overflow_err("result exceeds DateTime range")))?;
		Ok(Self {
			nanos,
		})
	}
}

impl DateTime {
	pub fn saturating_add(self, rhs: Duration) -> DateTime {
		let total = rhs.as_nanos().unwrap_or(if rhs.is_negative() {
			i64::MIN
		} else {
			i64::MAX
		});
		let nanos = (self.to_nanos() as i128 + total as i128).clamp(0, u64::MAX as i128);
		DateTime::from_nanos(nanos as u64)
	}

	pub fn saturating_sub(self, rhs: Duration) -> DateTime {
		let total = rhs.as_nanos().unwrap_or(if rhs.is_negative() {
			i64::MIN
		} else {
			i64::MAX
		});
		let nanos = (self.to_nanos() as i128 - total as i128).clamp(0, u64::MAX as i128);
		DateTime::from_nanos(nanos as u64)
	}

	pub fn saturating_duration_since(self, earlier: DateTime) -> Duration {
		let diff = (self.to_nanos() as i128 - earlier.to_nanos() as i128)
			.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
		Duration::from_nanoseconds(diff).unwrap_or_else(|_| Duration::zero())
	}
}

impl Add<Duration> for DateTime {
	type Output = DateTime;

	#[inline]
	fn add(self, rhs: Duration) -> DateTime {
		let total = rhs.as_nanos().expect("duration exceeds i64 nanoseconds");
		let nanos = self.to_nanos() as i128 + total as i128;
		DateTime::from_nanos(u64::try_from(nanos).expect("datetime addition out of range"))
	}
}

impl Sub<Duration> for DateTime {
	type Output = DateTime;

	#[inline]
	fn sub(self, rhs: Duration) -> DateTime {
		let total = rhs.as_nanos().expect("duration exceeds i64 nanoseconds");
		let nanos = self.to_nanos() as i128 - total as i128;
		DateTime::from_nanos(u64::try_from(nanos).expect("datetime subtraction out of range"))
	}
}

impl Sub<DateTime> for DateTime {
	type Output = Duration;

	#[inline]
	fn sub(self, rhs: DateTime) -> Duration {
		let diff = self.to_nanos() as i128 - rhs.to_nanos() as i128;
		Duration::from_nanoseconds(i64::try_from(diff).expect("datetime difference exceeds i64 nanoseconds"))
			.expect("datetime difference out of duration range")
	}
}

impl Rem<Duration> for DateTime {
	type Output = Duration;

	#[inline]
	fn rem(self, rhs: Duration) -> Duration {
		let total = rhs.as_nanos().expect("duration exceeds i64 nanoseconds");
		let total = u64::try_from(total).expect("duration must be positive for windowing");
		Duration::from_nanoseconds((self.to_nanos() % total) as i64)
			.expect("datetime remainder out of duration range")
	}
}

impl Display for DateTime {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let date = self.date();
		let time = self.time();

		write!(f, "{}T{}Z", date, time)
	}
}

impl Serialize for DateTime {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.nanos)
	}
}

struct DateTimeVisitor;

impl<'de> Visitor<'de> for DateTimeVisitor {
	type Value = DateTime;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("a datetime as nanoseconds since the Unix epoch (u64)")
	}

	fn visit_u64<E>(self, value: u64) -> Result<DateTime, E>
	where
		E: de::Error,
	{
		Ok(DateTime::from_nanos(value))
	}
}

impl<'de> Deserialize<'de> for DateTime {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_u64(DateTimeVisitor)
	}
}

impl DateTime {
	pub fn now<C: ClockNow>(clock: &C) -> Self {
		Self::from_nanos(clock.now_nanos())
	}
}

impl FromStr for DateTime {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		parse_datetime(Fragment::internal(s.trim()))
	}
}

#[cfg(test)]
pub mod tests {
	use std::fmt::Debug;

	use postcard::{from_bytes, to_allocvec};
	use serde_json::{from_str, to_string};

	use crate::{
		error::{TemporalKind, TypeError},
		value::{datetime::DateTime, duration::Duration},
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
		// Wire format is the raw nanos-since-epoch integer, not an ISO-8601 string.
		assert_eq!(json, datetime.to_nanos().to_string());

		let recovered: DateTime = from_str(&json).unwrap();
		assert_eq!(datetime, recovered);
	}

	#[test]
	fn test_serde_postcard_roundtrip_preserves_all_components() {
		// Binary (postcard) is the hot CDC path; every date/time component (incl. sub-second nanos)
		// must survive the integer encoding so CDC consumers reconstruct the exact instant.
		for (y, mo, d, h, mi, s, n) in [
			(1970u32 as i32, 1u32, 1u32, 0u32, 0u32, 0u32, 0u32),
			(2024, 3, 15, 14, 30, 45, 123456789),
			(1999, 12, 31, 23, 59, 59, 999999999),
			(2024, 3, 15, 14, 30, 45, 1),
		] {
			let dt = DateTime::new(y, mo, d, h, mi, s, n).unwrap();
			let bytes = to_allocvec(&dt).unwrap();
			let recovered: DateTime = from_bytes(&bytes).unwrap();
			assert_eq!(dt, recovered);
			assert_eq!(recovered.year(), y);
			assert_eq!(recovered.month(), mo);
			assert_eq!(recovered.day(), d);
			assert_eq!(recovered.hour(), h);
			assert_eq!(recovered.minute(), mi);
			assert_eq!(recovered.second(), s);
			assert_eq!(recovered.nanosecond(), n);
		}
	}

	fn assert_datetime_overflow<T: Debug>(result: Result<T, Box<TypeError>>) {
		let err = result.expect_err("expected DateTimeOverflow error");
		match *err {
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

	#[test]
	fn add_and_sub_duration_operators() {
		let dt = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let minute = Duration::from_seconds(60).unwrap();
		assert_eq!(dt + minute, DateTime::from_ymd_hms(2024, 1, 15, 10, 31, 25).unwrap());
		assert_eq!(dt - minute, DateTime::from_ymd_hms(2024, 1, 15, 10, 29, 25).unwrap());
	}

	#[test]
	fn sub_datetime_yields_duration() {
		let a = DateTime::from_ymd_hms(2024, 1, 15, 10, 31, 0).unwrap();
		let b = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap();
		assert_eq!(a - b, Duration::from_seconds(60).unwrap());
	}

	#[test]
	fn rem_duration_aligns_to_window_boundary() {
		let dt = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let minute = Duration::from_seconds(60).unwrap();
		// 25 seconds past the minute boundary.
		assert_eq!(dt % minute, Duration::from_seconds(25).unwrap());
		// The bucket-start computation `coord - (coord % dur)` lands on the boundary.
		assert_eq!(dt - (dt % minute), DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap());

		// A 1s window leaves the second boundary in place (sub-minute correctness).
		let second = Duration::from_seconds(1).unwrap();
		assert_eq!(dt % second, Duration::from_seconds(0).unwrap());
	}

	#[test]
	fn saturating_sub_below_epoch_clamps_to_epoch() {
		// A rolling-window cutoff that would fall before 1970 must clamp to the
		// epoch rather than panic the u64-nanos conversion (the chaos failure mode).
		let epoch = DateTime::from_nanos(0);
		assert_eq!(epoch.saturating_sub(Duration::from_seconds(1).unwrap()), epoch);

		let early = DateTime::from_timestamp(5).unwrap();
		assert_eq!(early.saturating_sub(Duration::from_seconds(10_000).unwrap()), epoch);
	}

	#[test]
	fn saturating_add_above_max_clamps_to_max() {
		// Overflow past the representable u64-nanos range clamps to the max instant.
		let near_max = DateTime::from_nanos(u64::MAX - 1);
		assert_eq!(near_max.saturating_add(Duration::from_days(1).unwrap()), DateTime::from_nanos(u64::MAX));
	}

	#[test]
	fn saturating_add_sub_match_operators_in_range() {
		// In range, the saturating ops agree with the panicking +/- operators.
		let dt = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let minute = Duration::from_seconds(60).unwrap();
		assert_eq!(dt.saturating_add(minute), dt + minute);
		assert_eq!(dt.saturating_sub(minute), dt - minute);
	}

	#[test]
	fn saturating_duration_since_normal_and_clamped() {
		let a = DateTime::from_ymd_hms(2024, 1, 15, 10, 31, 0).unwrap();
		let b = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap();
		assert_eq!(a.saturating_duration_since(b), Duration::from_seconds(60).unwrap());
		// Reversed order is a negative duration that still fits i64 (not clamped).
		assert_eq!(b.saturating_duration_since(a), Duration::from_seconds(-60).unwrap());
		// A gap wider than i64 nanoseconds clamps instead of panicking.
		let clamped = DateTime::from_nanos(u64::MAX).saturating_duration_since(DateTime::from_nanos(0));
		assert_eq!(clamped.as_nanos().unwrap(), i64::MAX);
	}
}

#[cfg(test)]
mod now_tests {
	use super::DateTime;
	use crate::clock::testing::TestClock;

	#[test]
	fn now_reads_the_clock() {
		// Civil "now" is sourced from the (mockable) clock, so DST/tests control it.
		let clock = TestClock::from_millis(1500);
		assert_eq!(DateTime::now(&clock), DateTime::from_nanos(1_500_000_000));
	}

	#[test]
	fn from_str_round_trips_display() {
		let dt = DateTime::from_nanos(1_700_000_000_000_000_000);
		let parsed: DateTime = dt.to_string().parse().unwrap();
		assert_eq!(parsed, dt);
	}

	#[test]
	fn from_str_rejects_garbage() {
		assert!("not a datetime".parse::<DateTime>().is_err());
	}
}
