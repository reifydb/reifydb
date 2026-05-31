// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::{mem::take, ops::Deref};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::value::{
	Value,
	date::Date,
	datetime::DateTime,
	duration::Duration,
	time::Time,
	try_from::{FromValueError, TryFromValue},
};

macro_rules! iso_wrapper {
	($iso:ident, $inner:ty) => {
		#[repr(transparent)]
		#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
		pub struct $iso(pub $inner);

		impl From<$inner> for $iso {
			fn from(value: $inner) -> Self {
				Self(value)
			}
		}

		impl From<$iso> for $inner {
			fn from(value: $iso) -> Self {
				value.0
			}
		}

		impl Deref for $iso {
			type Target = $inner;

			fn deref(&self) -> &Self::Target {
				&self.0
			}
		}

		impl TryFromValue for $iso {
			fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
				<$inner>::try_from_value(value).map($iso)
			}
		}
	};
}

iso_wrapper!(IsoDate, Date);
iso_wrapper!(IsoTime, Time);
iso_wrapper!(IsoDateTime, DateTime);
iso_wrapper!(IsoDuration, Duration);

impl Serialize for IsoDate {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(&self.0.to_string())
	}
}

impl Serialize for IsoTime {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(&self.0.to_string())
	}
}

impl Serialize for IsoDateTime {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(&self.0.to_string())
	}
}

impl Serialize for IsoDuration {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(&self.0.to_iso_string())
	}
}

impl<'de> Deserialize<'de> for IsoDate {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let raw = String::deserialize(deserializer)?;
		parse_date(&raw).map(IsoDate).map_err(de::Error::custom)
	}
}

impl<'de> Deserialize<'de> for IsoTime {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let raw = String::deserialize(deserializer)?;
		parse_time(&raw).map(IsoTime).map_err(de::Error::custom)
	}
}

impl<'de> Deserialize<'de> for IsoDateTime {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let raw = String::deserialize(deserializer)?;
		parse_datetime(&raw).map(IsoDateTime).map_err(de::Error::custom)
	}
}

impl<'de> Deserialize<'de> for IsoDuration {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let raw = String::deserialize(deserializer)?;
		parse_duration(&raw).map(IsoDuration).map_err(de::Error::custom)
	}
}

fn parse_date_parts(s: &str) -> Result<(i32, u32, u32), String> {
	let (sign, rest) = match s.strip_prefix('-') {
		Some(rest) => (-1, rest),
		None => (1, s),
	};
	let mut parts = rest.split('-');
	let year = parts.next().unwrap_or_default();
	let month = parts.next().ok_or_else(|| format!("missing month in date '{s}'"))?;
	let day = parts.next().ok_or_else(|| format!("missing day in date '{s}'"))?;
	if parts.next().is_some() {
		return Err(format!("unexpected extra component in date '{s}'"));
	}
	let year: i32 = year.parse().map_err(|_| format!("invalid year in date '{s}'"))?;
	let month: u32 = month.parse().map_err(|_| format!("invalid month in date '{s}'"))?;
	let day: u32 = day.parse().map_err(|_| format!("invalid day in date '{s}'"))?;
	Ok((sign * year, month, day))
}

fn parse_time_parts(s: &str) -> Result<(u32, u32, u32, u32), String> {
	let mut parts = s.split(':');
	let hour = parts.next().unwrap_or_default();
	let minute = parts.next().ok_or_else(|| format!("missing minute in time '{s}'"))?;
	let second = parts.next().ok_or_else(|| format!("missing second in time '{s}'"))?;
	if parts.next().is_some() {
		return Err(format!("unexpected extra component in time '{s}'"));
	}
	let hour: u32 = hour.parse().map_err(|_| format!("invalid hour in time '{s}'"))?;
	let minute: u32 = minute.parse().map_err(|_| format!("invalid minute in time '{s}'"))?;
	let (second_str, fraction) = match second.split_once('.') {
		Some((a, b)) => (a, b),
		None => (second, ""),
	};
	let second: u32 = second_str.parse().map_err(|_| format!("invalid second in time '{s}'"))?;
	let nano = parse_fraction_nanos(fraction).ok_or_else(|| format!("invalid fractional second in time '{s}'"))?;
	Ok((hour, minute, second, nano))
}

fn parse_fraction_nanos(fraction: &str) -> Option<u32> {
	if fraction.is_empty() {
		return Some(0);
	}
	if !fraction.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	let mut padded = fraction.to_string();
	padded.truncate(9);
	while padded.len() < 9 {
		padded.push('0');
	}
	padded.parse().ok()
}

fn parse_date(s: &str) -> Result<Date, String> {
	let (year, month, day) = parse_date_parts(s)?;
	Date::from_ymd(year, month, day).map_err(|e| e.to_string())
}

fn parse_time(s: &str) -> Result<Time, String> {
	let (hour, minute, second, nano) = parse_time_parts(s)?;
	Time::from_hms_nano(hour, minute, second, nano).map_err(|e| e.to_string())
}

fn parse_datetime(s: &str) -> Result<DateTime, String> {
	let body = s.strip_suffix('Z').unwrap_or(s);
	let (date_part, time_part) =
		body.split_once('T').ok_or_else(|| format!("missing 'T' separator in datetime '{s}'"))?;
	let (year, month, day) = parse_date_parts(date_part)?;
	let (hour, minute, second, nano) = parse_time_parts(time_part)?;
	DateTime::new(year, month, day, hour, minute, second, nano).ok_or_else(|| format!("invalid datetime '{s}'"))
}

fn parse_duration(s: &str) -> Result<Duration, String> {
	let rest = s.strip_prefix('P').ok_or_else(|| format!("duration must start with 'P': '{s}'"))?;
	let (date_part, time_part) = match rest.split_once('T') {
		Some((date, time)) => (date, time),
		None => (rest, ""),
	};

	let mut years = 0i64;
	let mut months = 0i64;
	let mut days = 0i64;
	for (number, unit) in split_components(date_part)? {
		let value: i64 = number.parse().map_err(|_| format!("invalid duration number '{number}'"))?;
		match unit {
			'Y' => years = value,
			'M' => months = value,
			'D' => days = value,
			other => return Err(format!("invalid duration date unit '{other}'")),
		}
	}

	let mut hours = 0i64;
	let mut minutes = 0i64;
	let mut seconds = 0i64;
	let mut fraction_nanos = 0i64;
	for (number, unit) in split_components(time_part)? {
		match unit {
			'H' => hours = number.parse().map_err(|_| format!("invalid duration hours '{number}'"))?,
			'M' => minutes = number.parse().map_err(|_| format!("invalid duration minutes '{number}'"))?,
			'S' => {
				let negative = number.starts_with('-');
				let (second_str, fraction) = match number.split_once('.') {
					Some((a, b)) => (a, b),
					None => (number.as_str(), ""),
				};
				seconds = second_str
					.parse()
					.map_err(|_| format!("invalid duration seconds '{number}'"))?;
				let magnitude = parse_fraction_nanos(fraction)
					.ok_or_else(|| format!("invalid duration fraction '{number}'"))?
					as i64;
				fraction_nanos = if negative {
					-magnitude
				} else {
					magnitude
				};
			}
			other => return Err(format!("invalid duration time unit '{other}'")),
		}
	}

	let total_months =
		i32::try_from(years * 12 + months).map_err(|_| format!("duration months out of range in '{s}'"))?;
	let total_days = i32::try_from(days).map_err(|_| format!("duration days out of range in '{s}'"))?;
	let nanos = (hours * 3600 + minutes * 60 + seconds) * 1_000_000_000 + fraction_nanos;
	Duration::new(total_months, total_days, nanos).map_err(|e| e.to_string())
}

fn split_components(s: &str) -> Result<Vec<(String, char)>, String> {
	let mut components = Vec::new();
	let mut number = String::new();
	for ch in s.chars() {
		if ch.is_ascii_digit() || ch == '-' || ch == '.' {
			number.push(ch);
		} else if ch.is_ascii_alphabetic() {
			if number.is_empty() {
				return Err(format!("missing number before '{ch}'"));
			}
			components.push((take(&mut number), ch));
		} else {
			return Err(format!("unexpected character '{ch}' in duration"));
		}
	}
	if !number.is_empty() {
		return Err("trailing number without unit in duration".to_string());
	}
	Ok(components)
}

#[cfg(test)]
mod tests {
	use serde_json::{from_value, json, to_value};

	use super::*;

	fn datetime() -> DateTime {
		DateTime::from_timestamp(1_700_000_000).unwrap()
	}

	#[test]
	fn serializes_each_wrapper_as_iso8601() {
		let date = IsoDate(Date::from_ymd(2024, 3, 15).unwrap());
		assert_eq!(to_value(date).unwrap(), json!("2024-03-15"));

		let time = IsoTime(Time::from_hms_nano(14, 30, 15, 123_456_789).unwrap());
		assert_eq!(to_value(time).unwrap(), json!("14:30:15.123456789"));

		let dt = IsoDateTime(datetime());
		let json = to_value(dt).unwrap();
		let rendered = json.as_str().unwrap();
		assert!(rendered.contains('T') && rendered.ends_with('Z'), "got {rendered}");

		assert_eq!(to_value(IsoDuration(Duration::from_seconds(90).unwrap())).unwrap(), json!("PT1M30S"));
		assert_eq!(
			to_value(IsoDuration(Duration::new(14, 3, 3_661_000_000_000).unwrap())).unwrap(),
			json!("P1Y2M3DT1H1M1S")
		);
		assert_eq!(to_value(IsoDuration(Duration::zero())).unwrap(), json!("PT0S"));
	}

	#[test]
	fn round_trips_through_serde() {
		let values: Vec<IsoDate> = vec![IsoDate(Date::from_ymd(2024, 3, 15).unwrap())];
		for v in values {
			let json = to_value(v).unwrap();
			assert_eq!(from_value::<IsoDate>(json).unwrap(), v);
		}

		let time = IsoTime(Time::from_hms_nano(14, 30, 15, 123_456_789).unwrap());
		assert_eq!(from_value::<IsoTime>(to_value(time).unwrap()).unwrap(), time);

		let dt = IsoDateTime(datetime());
		assert_eq!(from_value::<IsoDateTime>(to_value(dt).unwrap()).unwrap(), dt);

		for d in [
			IsoDuration(Duration::from_seconds(90).unwrap()),
			IsoDuration(Duration::new(14, 3, 3_661_000_000_000).unwrap()),
			IsoDuration(Duration::zero()),
		] {
			assert_eq!(from_value::<IsoDuration>(to_value(d).unwrap()).unwrap(), d);
		}
	}

	#[test]
	fn try_from_value_delegates_to_inner_and_rejects_mismatch() {
		let dt = datetime();
		assert_eq!(IsoDateTime::try_from_value(&Value::DateTime(dt)).unwrap(), IsoDateTime(dt));

		let err = IsoDateTime::try_from_value(&Value::Date(Date::from_ymd(2024, 3, 15).unwrap())).unwrap_err();
		assert!(matches!(err, FromValueError::TypeMismatch { .. }), "a Date is not a DateTime");
	}

	#[test]
	fn deserialize_rejects_malformed_input_without_panicking() {
		assert!(from_value::<IsoDate>(json!("not-a-date")).is_err());
		assert!(from_value::<IsoDuration>(json!("90s")).is_err());
		assert!(from_value::<IsoDateTime>(json!("2024-03-15")).is_err());
	}
}
