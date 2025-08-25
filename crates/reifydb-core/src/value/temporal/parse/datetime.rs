// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{date::parse_date, time::parse_time};
use crate::{
	DateTime, Error, interface::fragment::IntoFragment,
	result::error::diagnostic::temporal, return_error,
};

pub fn parse_datetime(fragment: impl IntoFragment) -> Result<DateTime, Error> {
	let owned_fragment = fragment.into_fragment();
	let parts: Vec<&str> = owned_fragment.value().split('T').collect();
	if parts.len() != 2 {
		return_error!(temporal::invalid_datetime_format(
			owned_fragment
		));
	}

	// Create sub-fragments for the date and time parts with proper position
	let date_offset = 0;
	let date_fragment =
		owned_fragment.sub_fragment(date_offset, parts[0].len());
	let time_offset = parts[0].len() + 1; // +1 for the 'T' separator
	let time_fragment =
		owned_fragment.sub_fragment(time_offset, parts[1].len());

	let date = parse_date(date_fragment)?;
	let time = parse_time(time_fragment)?;

	Ok(DateTime::new(
		date.year(),
		date.month(),
		date.day(),
		time.hour(),
		time.minute(),
		time.second(),
		time.nanosecond(),
	)
	.unwrap()) // safe because date and time already checked
}

#[cfg(test)]
mod tests {
	use super::parse_datetime;
	use crate::interface::fragment::OwnedFragment;

	#[test]
	fn test_basic() {
		let fragment = OwnedFragment::testing("2024-03-15T14:30:00");
		let datetime = parse_datetime(fragment).unwrap();
		assert_eq!(
			datetime.to_string(),
			"2024-03-15T14:30:00.000000000Z"
		);
	}

	#[test]
	fn test_with_timezone_z() {
		let fragment = OwnedFragment::testing("2024-03-15T14:30:00Z");
		let datetime = parse_datetime(fragment).unwrap();
		assert_eq!(
			datetime.to_string(),
			"2024-03-15T14:30:00.000000000Z"
		);
	}

	#[test]
	fn test_with_milliseconds() {
		let fragment =
			OwnedFragment::testing("2024-03-15T14:30:00.123Z");
		let datetime = parse_datetime(fragment).unwrap();
		assert_eq!(
			datetime.to_string(),
			"2024-03-15T14:30:00.123000000Z"
		);
	}

	#[test]
	fn test_with_microseconds() {
		let fragment =
			OwnedFragment::testing("2024-03-15T14:30:00.123456Z");
		let datetime = parse_datetime(fragment).unwrap();
		assert_eq!(
			datetime.to_string(),
			"2024-03-15T14:30:00.123456000Z"
		);
	}

	#[test]
	fn test_with_nanoseconds() {
		let fragment = OwnedFragment::testing(
			"2024-03-15T14:30:00.123456789Z",
		);
		let datetime = parse_datetime(fragment).unwrap();
		assert_eq!(
			datetime.to_string(),
			"2024-03-15T14:30:00.123456789Z"
		);
	}

	#[test]
	fn test_leap_year() {
		let fragment = OwnedFragment::testing("2024-02-29T00:00:00");
		let datetime = parse_datetime(fragment).unwrap();
		assert_eq!(
			datetime.to_string(),
			"2024-02-29T00:00:00.000000000Z"
		);
	}

	#[test]
	fn test_boundaries() {
		let fragment = OwnedFragment::testing("2000-01-01T00:00:00");
		let datetime = parse_datetime(fragment).unwrap();
		assert_eq!(
			datetime.to_string(),
			"2000-01-01T00:00:00.000000000Z"
		);

		let fragment = OwnedFragment::testing("2024-12-31T23:59:59");
		let datetime = parse_datetime(fragment).unwrap();
		assert_eq!(
			datetime.to_string(),
			"2024-12-31T23:59:59.000000000Z"
		);
	}

	#[test]
	fn test_invalid_format() {
		let fragment = OwnedFragment::testing("2024-03-15");
		let err = parse_datetime(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_002");
	}

	#[test]
	fn test_invalid_date_format() {
		let fragment = OwnedFragment::testing("2024-03T14:30:00");
		let err = parse_datetime(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_001");
	}

	#[test]
	fn test_invalid_time_format() {
		let fragment = OwnedFragment::testing("2024-03-15T14:30");
		let err = parse_datetime(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_003");
	}

	#[test]
	fn test_invalid_year() {
		let fragment = OwnedFragment::testing("invalid-03-15T14:30:00");
		let err = parse_datetime(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");
	}

	#[test]
	fn test_invalid_date_values() {
		let fragment = OwnedFragment::testing("2024-13-32T23:30:40");
		let err = parse_datetime(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_012");
	}

	#[test]
	fn test_invalid_time_value() {
		let fragment = OwnedFragment::testing("2024-09-09T30:70:80");
		let err = parse_datetime(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_013");
	}

	#[test]
	fn test_invalid_fractional_seconds() {
		let fragment =
			OwnedFragment::testing("2024-03-15T14:30:00.123.456");
		let err = parse_datetime(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_011");
	}
}
