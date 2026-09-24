// SPDX-License-Identifier: Apache-2.0
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
	error::{Error, TemporalKind, TypeError},
	fragment::Fragment,
	value::{date::Date, duration::Duration, temporal::parse::datetime::parse_datetime, time::Time},
};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;
const ORDER_SIGN_BIT: u64 = 1 << 63;

pub static CREATED_AT_COLUMN_NAME: &str = "created_at";
pub static UPDATED_AT_COLUMN_NAME: &str = "updated_at";
pub static TIME_COLUMN_NAME: &str = "time";

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct DateTime {
	nanos: i64,
}

impl DateTime {
	pub fn new(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
		let date = Date::new(year, month, day)?;
		let time = Time::new(hour, min, sec, nano)?;

		let days = date.to_days_since_epoch();
		let nanos = days as i128 * NANOS_PER_DAY as i128 + time.to_nanos_since_midnight() as i128;
		Some(Self {
			nanos: i64::try_from(nanos).ok()?,
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

	pub fn to_order(&self) -> u64 {
		(self.nanos as u64) ^ ORDER_SIGN_BIT
	}

	pub fn from_order(order: u64) -> Self {
		Self {
			nanos: (order ^ ORDER_SIGN_BIT) as i64,
		}
	}

	pub fn from_nanos(nanos: i64) -> Self {
		Self {
			nanos,
		}
	}

	pub fn to_nanos(&self) -> i64 {
		self.nanos
	}

	pub fn from_epoch_secs(secs: i64) -> Result<Self, Box<TypeError>> {
		let nanos = secs.checked_mul(NANOS_PER_SECOND).ok_or_else(|| {
			Box::new(Self::overflow_err(format!("{} seconds overflows DateTime range", secs)))
		})?;
		Ok(Self {
			nanos,
		})
	}

	pub fn from_epoch_millis(millis: i64) -> Result<Self, Box<TypeError>> {
		let nanos = millis.checked_mul(NANOS_PER_MILLI).ok_or_else(|| {
			Box::new(Self::overflow_err(format!("{} milliseconds overflows DateTime range", millis)))
		})?;
		Ok(Self {
			nanos,
		})
	}

	pub fn to_epoch_secs(&self) -> i64 {
		self.nanos.div_euclid(NANOS_PER_SECOND)
	}

	pub fn to_epoch_millis(&self) -> i64 {
		self.nanos.div_euclid(NANOS_PER_MILLI)
	}

	pub fn try_date(&self) -> Result<Date, Box<TypeError>> {
		let days = self.nanos.div_euclid(NANOS_PER_DAY);
		let days = i32::try_from(days)
			.map_err(|_| Box::new(Self::overflow_err("DateTime overflows Date range")))?;
		Date::from_days_since_epoch(days)
			.ok_or_else(|| Box::new(Self::overflow_err("DateTime overflows Date range")))
	}

	pub fn date(&self) -> Date {
		self.try_date().expect("DateTime overflows Date range")
	}

	pub fn time(&self) -> Time {
		Time::from_nanos_since_midnight(self.nanos.rem_euclid(NANOS_PER_DAY) as u64).unwrap()
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
				"invalid datetime after adding duration: {}-{:02}-{:02}",
				year, month, day
			)))
		})?;
		let base_days = base_date.to_days_since_epoch() as i64 + dur.get_days() as i64;
		let time_nanos = time.to_nanos_since_midnight() as i64 + dur.get_nanos();

		let total_nanos = base_days as i128 * 86_400_000_000_000i128 + time_nanos as i128;

		let nanos = i64::try_from(total_nanos)
			.map_err(|_| Box::new(Self::overflow_err("the result overflows DateTime range")))?;
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
		let nanos = (self.to_nanos() as i128 + total as i128).clamp(i64::MIN as i128, i64::MAX as i128);
		DateTime::from_nanos(nanos as i64)
	}

	pub fn saturating_sub(self, rhs: Duration) -> DateTime {
		let total = rhs.as_nanos().unwrap_or(if rhs.is_negative() {
			i64::MIN
		} else {
			i64::MAX
		});
		let nanos = (self.to_nanos() as i128 - total as i128).clamp(i64::MIN as i128, i64::MAX as i128);
		DateTime::from_nanos(nanos as i64)
	}

	pub fn checked_add(self, rhs: Duration) -> Option<DateTime> {
		let total = rhs.as_nanos().ok()?;
		let nanos = self.to_nanos() as i128 + total as i128;
		i64::try_from(nanos).ok().map(DateTime::from_nanos)
	}

	pub fn checked_sub(self, rhs: Duration) -> Option<DateTime> {
		let total = rhs.as_nanos().ok()?;
		let nanos = self.to_nanos() as i128 - total as i128;
		i64::try_from(nanos).ok().map(DateTime::from_nanos)
	}

	pub fn saturating_duration_since(self, earlier: DateTime) -> Duration {
		let diff = (self.to_nanos() as i128 - earlier.to_nanos() as i128)
			.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
		Duration::from_nanoseconds(diff).unwrap_or_else(|_| Duration::zero())
	}
}

impl DateTime {
	pub const ALIGNMENT: usize = 8;

	pub const EPOCH: DateTime = DateTime {
		nanos: 0,
	};

	pub const MIN: DateTime = DateTime {
		nanos: i64::MIN,
	};

	pub const MAX: DateTime = DateTime {
		nanos: i64::MAX,
	};

	pub fn is_epoch(&self) -> bool {
		self.nanos == 0
	}

	pub fn from_millis(millis: i64) -> Self {
		Self {
			nanos: millis.saturating_mul(NANOS_PER_MILLI),
		}
	}

	pub fn to_millis(&self) -> i64 {
		self.nanos.div_euclid(NANOS_PER_MILLI)
	}

	pub fn to_micros(&self) -> i64 {
		self.nanos.div_euclid(NANOS_PER_MICRO)
	}

	pub fn to_secs(&self) -> i64 {
		self.nanos.div_euclid(NANOS_PER_SECOND)
	}

	pub fn saturating_add_millis(self, millis: u64) -> DateTime {
		let delta = i64::try_from(millis).unwrap_or(i64::MAX).saturating_mul(NANOS_PER_MILLI);
		DateTime::from_nanos(self.nanos.saturating_add(delta))
	}

	pub fn saturating_sub_millis(self, millis: u64) -> DateTime {
		let delta = i64::try_from(millis).unwrap_or(i64::MAX).saturating_mul(NANOS_PER_MILLI);
		DateTime::from_nanos(self.nanos.saturating_sub(delta))
	}

	pub fn floor_to_millis(self, millis: u64) -> DateTime {
		let width = i64::try_from(millis).unwrap_or(i64::MAX).saturating_mul(NANOS_PER_MILLI);
		if width == 0 {
			return self;
		}
		DateTime::from_nanos(self.nanos.saturating_sub(self.nanos.rem_euclid(width)))
	}
}

impl Add<Duration> for DateTime {
	type Output = DateTime;

	#[inline]
	fn add(self, rhs: Duration) -> DateTime {
		let total = rhs.as_nanos().expect("duration exceeds i64 nanoseconds");
		let nanos = self.to_nanos() as i128 + total as i128;
		DateTime::from_nanos(i64::try_from(nanos).expect("datetime addition out of range"))
	}
}

impl Sub<Duration> for DateTime {
	type Output = DateTime;

	#[inline]
	fn sub(self, rhs: Duration) -> DateTime {
		let total = rhs.as_nanos().expect("duration exceeds i64 nanoseconds");
		let nanos = self.to_nanos() as i128 - total as i128;
		DateTime::from_nanos(i64::try_from(nanos).expect("datetime subtraction out of range"))
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
		Duration::from_nanoseconds(self.nanos.rem_euclid(total as i64))
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
		serializer.serialize_i64(self.nanos)
	}
}

struct DateTimeVisitor;

impl<'de> Visitor<'de> for DateTimeVisitor {
	type Value = DateTime;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("a datetime as signed nanoseconds since the Unix epoch (i64)")
	}

	fn visit_i64<E>(self, value: i64) -> Result<DateTime, E>
	where
		E: de::Error,
	{
		Ok(DateTime::from_nanos(value))
	}

	fn visit_u64<E>(self, value: u64) -> Result<DateTime, E>
	where
		E: de::Error,
	{
		i64::try_from(value).map(DateTime::from_nanos).map_err(|_| E::custom("datetime out of range"))
	}
}

impl<'de> Deserialize<'de> for DateTime {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_i64(DateTimeVisitor)
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
		let datetime = DateTime::new(2024, 3, 15, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T00:00:00.000000000Z");

		let datetime = DateTime::new(2024, 3, 15, 23, 59, 59, 999999999).unwrap();
		assert_eq!(format!("{}", datetime), "2024-03-15T23:59:59.999999999Z");

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
		let datetime = DateTime::new(2000, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2000-01-01T00:00:00.000000000Z");

		let datetime = DateTime::new(2100, 1, 1, 0, 0, 0, 0).unwrap();
		assert_eq!(format!("{}", datetime), "2100-01-01T00:00:00.000000000Z");

		let datetime = DateTime::new(2262, 4, 11, 23, 47, 16, 854775807).unwrap();
		assert_eq!(format!("{}", datetime), "2262-04-11T23:47:16.854775807Z");
		assert_eq!(datetime, DateTime::MAX);

		assert!(DateTime::new(2262, 4, 11, 23, 47, 16, 854775808).is_none());
		assert!(DateTime::new(9999, 12, 31, 23, 59, 59, 999999999).is_none());
	}

	#[test]
	fn test_datetime_accepts_pre_epoch_down_to_min() {
		assert_eq!(DateTime::new(1969, 12, 31, 23, 59, 59, 999999999), Some(DateTime::from_nanos(-1)));

		assert!(DateTime::new(1900, 1, 1, 0, 0, 0, 0).is_some());

		assert_eq!(DateTime::new(1677, 9, 21, 0, 12, 43, 145224192), Some(DateTime::MIN));
		assert!(DateTime::new(1677, 9, 21, 0, 12, 43, 145224191).is_none());

		assert_eq!(DateTime::from_epoch_secs(-1).unwrap().to_nanos(), -1_000_000_000);
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
	fn test_datetime_display_from_epoch_secs() {
		let datetime = DateTime::from_epoch_secs(0).unwrap();
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:00.000000000Z");

		let datetime = DateTime::from_epoch_secs(1234567890).unwrap();
		assert_eq!(format!("{}", datetime), "2009-02-13T23:31:30.000000000Z");
	}

	#[test]
	fn test_datetime_display_from_epoch_millis() {
		let datetime = DateTime::from_epoch_millis(1234567890123).unwrap();
		assert_eq!(format!("{}", datetime), "2009-02-13T23:31:30.123000000Z");

		let datetime = DateTime::from_epoch_millis(0).unwrap();
		assert_eq!(format!("{}", datetime), "1970-01-01T00:00:00.000000000Z");
	}

	#[test]
	fn test_datetime_bits_roundtrip_preserves_every_component() {
		// every key encoding and the serde impl go through to_order, so a lossy leg moves stored instants
		// unseen
		let cases = [
			DateTime::new(1970, 1, 1, 0, 0, 0, 0).unwrap(),
			DateTime::new(2024, 3, 15, 14, 30, 45, 123456789).unwrap(),
			DateTime::new(2000, 2, 29, 23, 59, 59, 999999999).unwrap(),
			DateTime::MAX,
			DateTime::MIN,
			DateTime::from_nanos(-1),
		];

		for datetime in cases {
			let recovered = DateTime::from_order(datetime.to_order());

			assert_eq!(datetime, recovered);
			assert_eq!(datetime.nanosecond(), recovered.nanosecond(), "sub-second precision must survive");
		}
	}

	#[test]
	fn test_datetime_bits_are_monotonic_in_instant_order() {
		// key encodings sort on the raw bits, so a disagreeing order would fire timers out of sequence
		let ordered = [
			DateTime::MIN,
			DateTime::from_nanos(-1),
			DateTime::new(1970, 1, 1, 0, 0, 0, 0).unwrap(),
			DateTime::new(1970, 1, 1, 0, 0, 0, 1).unwrap(),
			DateTime::new(2024, 3, 15, 14, 30, 45, 123456789).unwrap(),
			DateTime::new(2024, 3, 15, 14, 30, 45, 123456790).unwrap(),
			DateTime::MAX,
		];

		for pair in ordered.windows(2) {
			let (lo, hi) = (pair[0], pair[1]);
			assert!(lo < hi, "fixture must be ordered");
			assert!(lo.to_order() < hi.to_order(), "bit order must follow instant order");
		}
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

		let pre_epoch = DateTime::new(1900, 6, 15, 12, 34, 56, 789000000).unwrap();
		let json = to_string(&pre_epoch).unwrap();
		assert_eq!(json, pre_epoch.to_nanos().to_string());

		let recovered: DateTime = from_str(&json).unwrap();
		assert_eq!(pre_epoch, recovered);
	}

	#[test]
	fn test_serde_postcard_roundtrip_preserves_all_components() {
		// Postcard is the CDC wire format; sub-second nanos must survive it or consumers
		// reconstruct the wrong instant.
		for (y, mo, d, h, mi, s, n) in [
			(1970u32 as i32, 1u32, 1u32, 0u32, 0u32, 0u32, 0u32),
			(2024, 3, 15, 14, 30, 45, 123456789),
			(1999, 12, 31, 23, 59, 59, 999999999),
			(2024, 3, 15, 14, 30, 45, 1),
			(1900, 6, 15, 12, 34, 56, 789000000),
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
	fn test_from_epoch_secs_large_value_overflow() {
		assert_datetime_overflow(DateTime::from_epoch_secs(i64::MAX));
	}

	#[test]
	fn test_from_epoch_secs_negative_overflow() {
		assert!(DateTime::from_epoch_secs(-1).is_ok());
		assert_datetime_overflow(DateTime::from_epoch_secs(i64::MIN));
	}

	#[test]
	fn test_from_epoch_millis_overflow() {
		assert_datetime_overflow(DateTime::from_epoch_millis(i64::MAX));
	}

	#[test]
	fn test_from_epoch_millis_boundary_ok() {
		let dt = DateTime::from_epoch_millis(1_700_000_000_000).unwrap();
		assert!(dt.to_nanos() > 0);
	}

	#[test]
	fn test_try_date_max_nanos_ok() {
		let date = DateTime::MAX.try_date().unwrap();
		assert_eq!(date.year(), 2262);

		let date = DateTime::MIN.try_date().unwrap();
		assert_eq!(date.year(), 1677);
	}

	#[test]
	fn test_add_duration_overflow() {
		let dt = DateTime::from_nanos(i64::MAX - 1);
		let dur = Duration::from_days(1).unwrap();
		assert_datetime_overflow(dt.add_duration(&dur));
	}

	#[test]
	fn test_add_duration_before_epoch() {
		let epoch = DateTime::new(1970, 1, 1, 0, 0, 0, 0).unwrap();
		let dur = Duration::from_seconds(-1).unwrap();
		assert_eq!(epoch.add_duration(&dur).unwrap(), DateTime::from_nanos(-1_000_000_000));

		assert_datetime_overflow(DateTime::MIN.add_duration(&dur));
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
		// Window bucket starts are computed as `coord - (coord % width)`.
		let dt = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let minute = Duration::from_seconds(60).unwrap();
		assert_eq!(dt % minute, Duration::from_seconds(25).unwrap());
		assert_eq!(dt - (dt % minute), DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap());

		let second = Duration::from_seconds(1).unwrap();
		assert_eq!(dt % second, Duration::from_seconds(0).unwrap());
	}

	#[test]
	fn saturating_sub_clamps_at_min() {
		// A cutoff below DateTime::MIN must clamp to MIN, not overflow.
		let epoch = DateTime::from_nanos(0);
		assert_eq!(
			epoch.saturating_sub(Duration::from_seconds(1).unwrap()),
			DateTime::from_nanos(-1_000_000_000)
		);

		assert_eq!(DateTime::MIN.saturating_sub(Duration::from_seconds(1).unwrap()), DateTime::MIN);
	}

	#[test]
	fn checked_sub_gives_a_pre_epoch_cutoff_and_none_only_below_min() {
		// now=1s, ttl=3s gives a pre-epoch cutoff, not None; only DateTime::MIN has no representable cutoff.
		let now = DateTime::from_epoch_secs(1).unwrap();
		assert_eq!(
			now.checked_sub(Duration::from_seconds(3).unwrap()),
			Some(DateTime::from_nanos(-2_000_000_000))
		);
		assert_eq!(DateTime::MIN.checked_sub(Duration::from_seconds(1).unwrap()), None);
	}

	#[test]
	fn checked_sub_matches_subtraction_when_in_range() {
		let now = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let minute = Duration::from_seconds(60).unwrap();
		assert_eq!(now.checked_sub(minute), Some(now - minute));
	}

	#[test]
	fn saturating_add_above_max_clamps_to_max() {
		// Overflow past the representable range clamps to the max instant.
		let near_max = DateTime::from_nanos(i64::MAX - 1);
		assert_eq!(near_max.saturating_add(Duration::from_days(1).unwrap()), DateTime::from_nanos(i64::MAX));
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
		// A gap wider than i64 nanoseconds must clamp rather than panic; a reversed pair is a
		// negative duration, not a clamp.
		let a = DateTime::from_ymd_hms(2024, 1, 15, 10, 31, 0).unwrap();
		let b = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap();
		assert_eq!(a.saturating_duration_since(b), Duration::from_seconds(60).unwrap());
		assert_eq!(b.saturating_duration_since(a), Duration::from_seconds(-60).unwrap());
		let clamped = DateTime::MAX.saturating_duration_since(DateTime::MIN);
		assert_eq!(clamped.as_nanos().unwrap(), i64::MAX);
	}

	#[test]
	fn checked_add_matches_addition_when_in_range() {
		let now = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let minute = Duration::from_seconds(60).unwrap();
		assert_eq!(now.checked_add(minute), Some(now + minute));
	}

	#[test]
	fn checked_add_returns_none_past_the_representable_range() {
		// An expiry past the end of the range must be None; wrapping yields a small instant
		// that reads as already expired.
		let near_max = DateTime::from_nanos(i64::MAX - 1);
		assert_eq!(near_max.checked_add(Duration::from_days(1).unwrap()), None);
	}

	#[test]
	fn checked_sub_of_a_negative_duration_cannot_wrap_past_the_range() {
		// Subtracting a negative duration moves forward, so checked_sub must check the upper
		// bound too.
		let near_max = DateTime::from_nanos(i64::MAX - 1);
		assert_eq!(near_max.checked_sub(Duration::from_days(-1).unwrap()), None);
	}

	#[test]
	fn floor_to_millis_reproduces_the_millis_truncated_bucket_boundary() {
		// THE test for the nanos migration. Bucket starts are computed today by truncating the
		// instant to millis, taking the remainder against a millis-wide window, and multiplying
		// back up. Doing the same arithmetic in nanos must land on the identical boundary, or a
		// row near an edge silently changes bucket and every downstream aggregate moves with it.
		// The instant deliberately carries sub-millisecond digits, which is where the two paths
		// would diverge if the flooring were done in the wrong order.
		// Mutation: round instead of floor, or truncate the instant to millis first, and the two
		// paths disagree.
		let nanos = 1_700_000_123_456_789i64;
		let window_ms = 1_000u64;

		let ts_ms = nanos / 1_000_000;
		let legacy_bucket_start_nanos = (ts_ms - ts_ms % window_ms as i64) * 1_000_000;

		assert_eq!(
			DateTime::from_nanos(nanos).floor_to_millis(window_ms),
			DateTime::from_nanos(legacy_bucket_start_nanos)
		);
	}

	#[test]
	fn floor_to_millis_agrees_with_the_rem_duration_operator() {
		// floor_to_millis replaces `coord - (coord % width)` at the window call sites, so the
		// two forms must agree exactly.
		let dt = DateTime::from_nanos(1_700_000_123_456_789);
		let width_ms = 60_000u64;
		let width = Duration::from_milliseconds(width_ms as i64).unwrap();

		assert_eq!(dt.floor_to_millis(width_ms), dt - (dt % width));
	}

	#[test]
	fn floor_to_millis_keeps_a_boundary_instant_where_it_is() {
		// An instant exactly on a boundary belongs to the bucket it opens, not the one before.
		let width_ms = 1_000u64;
		let boundary = DateTime::from_nanos(2_000_000_000);

		assert_eq!(
			DateTime::from_nanos(1_999_999_999).floor_to_millis(width_ms),
			DateTime::from_nanos(1_000_000_000)
		);
		assert_eq!(boundary.floor_to_millis(width_ms), boundary);
		assert_eq!(DateTime::from_nanos(2_000_000_001).floor_to_millis(width_ms), boundary);
	}

	#[test]
	fn floor_to_millis_of_a_zero_width_grid_is_the_instant_itself() {
		// Zero width is rejected where windows are defined; this helper must still stay total
		// rather than dividing by zero inside a flow tick, and identity cannot fabricate a bucket.
		let dt = DateTime::from_nanos(1_700_000_123_456_789);
		assert_eq!(dt.floor_to_millis(0), dt);
	}

	#[test]
	fn saturating_sub_millis_clamps_at_min() {
		// A cutoff below DateTime::MIN must clamp to MIN, not wrap.
		assert_eq!(DateTime::EPOCH.saturating_sub_millis(30_000), DateTime::from_nanos(-30_000_000_000));
		assert_eq!(DateTime::MIN.saturating_sub_millis(1), DateTime::MIN);
	}

	#[test]
	fn saturating_add_millis_clamps_at_the_maximum() {
		// Wrapping addition would turn an expiry past the range into a past instant, which
		// reads as already expired.
		assert_eq!(DateTime::MAX.saturating_add_millis(1), DateTime::MAX);
		assert_eq!(DateTime::from_nanos(1_000_000).saturating_add_millis(1), DateTime::from_nanos(2_000_000));
	}

	#[test]
	fn millis_conversions_round_trip_and_truncate_in_one_direction_only() {
		// Widening millis to nanos is lossless, narrowing truncates, so converting sites must
		// converge on nanos - a round trip through millis drops sub-millisecond digits for good.
		assert_eq!(DateTime::from_millis(1_500).to_millis(), 1_500);
		assert_eq!(DateTime::from_millis(1_500), DateTime::from_nanos(1_500_000_000));

		let precise = DateTime::from_nanos(1_500_000_999);
		assert_eq!(precise.to_millis(), 1_500);
		assert_eq!(DateTime::from_millis(precise.to_millis()), DateTime::from_nanos(1_500_000_000));
	}

	#[test]
	fn coarser_unit_accessors_floor() {
		// Must match `to_nanos() / N` truncation exactly; rounding shifts a boundary instant into the next
		// unit.
		let dt = DateTime::from_nanos(1_700_000_123_456_789);

		assert_eq!(dt.to_secs(), 1_700_000);
		assert_eq!(dt.to_micros(), 1_700_000_123_456);
		assert_eq!(dt.to_millis(), 1_700_000_123);

		assert_eq!(dt.to_secs(), dt.to_nanos() / 1_000_000_000);
		assert_eq!(dt.to_micros(), dt.to_nanos() / 1_000);

		assert_eq!(DateTime::from_nanos(1_999_999_999).to_secs(), 1);
		assert_eq!(DateTime::from_nanos(1_999).to_micros(), 1);
		assert_eq!(DateTime::EPOCH.to_secs(), 0);
		assert_eq!(DateTime::EPOCH.to_micros(), 0);

		assert_eq!(DateTime::from_nanos(-1).to_secs(), -1);
		assert_eq!(DateTime::from_nanos(-1).to_millis(), -1);
		assert_eq!(DateTime::from_nanos(-1).to_micros(), -1);
	}

	#[test]
	fn from_millis_saturates_where_from_epoch_millis_errors() {
		// from_millis is the infallible form callers want; an input large enough to overflow is
		// roughly 584 million years, so clamping is safe and saves an unwrap at every call site.
		assert_eq!(DateTime::from_millis(1_500), DateTime::from_epoch_millis(1_500).unwrap());
		assert!(DateTime::from_epoch_millis(i64::MAX).is_err());
		assert_eq!(DateTime::from_millis(i64::MAX), DateTime::MAX);
		assert_eq!(DateTime::from_millis(i64::MIN), DateTime::MIN);
	}

	#[test]
	fn from_epoch_secs_reads_its_argument_as_whole_seconds() {
		// Chain timestamps arrive in seconds; a millis-scaled constructor would be off by 1000x silently.
		assert_eq!(DateTime::from_epoch_secs(1).unwrap().to_nanos(), 1_000_000_000);
		assert_eq!(DateTime::from_epoch_secs(0).unwrap(), DateTime::EPOCH);
		assert_eq!(
			DateTime::from_epoch_secs(1_234_567_890).unwrap(),
			DateTime::from_epoch_millis(1_234_567_890_000).unwrap()
		);
	}

	#[test]
	fn the_epoch_constant_is_the_zero_instant() {
		// Watermarks hydrate to the epoch to mean "nothing seen yet".
		assert_eq!(DateTime::EPOCH, DateTime::from_nanos(0));
		assert_eq!(DateTime::EPOCH, DateTime::default());
		assert!(DateTime::EPOCH.is_epoch());
		assert!(!DateTime::from_nanos(1).is_epoch());
	}

	#[test]
	fn a_pre_epoch_instant_round_trips_every_component() {
		// A negative nanos value must decompose back to its own date/time, not wrap to a bogus post-epoch one.
		let datetime = DateTime::new(1900, 6, 15, 12, 34, 56, 789000001).unwrap();
		assert!(datetime.to_nanos() < 0);
		assert_eq!(datetime.year(), 1900);
		assert_eq!(datetime.month(), 6);
		assert_eq!(datetime.day(), 15);
		assert_eq!(datetime.hour(), 12);
		assert_eq!(datetime.minute(), 34);
		assert_eq!(datetime.second(), 56);
		assert_eq!(datetime.nanosecond(), 789000001);
	}

	#[test]
	fn min_and_max_display_the_representable_range_boundaries() {
		// The Display impl must agree with the range DateTime::new accepts, or an error message quoting MIN/MAX
		// lies.
		assert_eq!(format!("{}", DateTime::MIN), "1677-09-21T00:12:43.145224192Z");
		assert_eq!(format!("{}", DateTime::MAX), "2262-04-11T23:47:16.854775807Z");
	}

	#[test]
	fn a_pre_epoch_instant_floors_and_remainders_toward_the_earlier_boundary() {
		// rem_euclid/div_euclid give floor semantics; truncating toward zero would put -1ns in the wrong
		// second.
		let dt = DateTime::from_nanos(-1);
		assert_eq!(dt % Duration::from_seconds(1).unwrap(), Duration::from_nanoseconds(999_999_999).unwrap());
		assert_eq!(dt.floor_to_millis(1_000), DateTime::from_nanos(-1_000_000_000));
	}
}

#[cfg(test)]
mod now_tests {
	use super::DateTime;
	use crate::clock::{ClockNow, testing::TestClock};

	#[test]
	fn now_reads_the_clock() {
		// "now" comes from the injected clock so tests stay deterministic.
		let clock = TestClock::from_millis(1500);
		assert_eq!(clock.now(), DateTime::from_nanos(1_500_000_000));
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
