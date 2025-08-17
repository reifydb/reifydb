// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	interface::fragment::Fragment, result::error::diagnostic::temporal, return_error,
	Date, Error,
};

pub fn parse_date(fragment: impl Fragment) -> Result<Date, Error> {
	let value = fragment.value();
	let parts: Vec<&str> = value.split('-').collect();

	if parts.len() != 3 {
		return_error!(temporal::invalid_date_format(fragment));
	}

	// Check for empty parts
	let year_str = parts[0].trim();
	let month_str = parts[1].trim();
	let day_str = parts[2].trim();

	if year_str.is_empty() || month_str.is_empty() || day_str.is_empty() {
		return_error!(temporal::empty_date_component(fragment.clone()));
	}

	if year_str.len() != 4 {
		return_error!(temporal::invalid_year(fragment.clone()));
	}

	let year = year_str
		.parse::<i32>()
		.map_err(|_| Error(temporal::invalid_year(fragment.clone())))?;

	if month_str.len() != 2 {
		return_error!(temporal::invalid_month(fragment.clone()));
	}

	let month = month_str.parse::<u32>().map_err(|_| {
		Error(temporal::invalid_month(fragment.clone()))
	})?;
	if day_str.len() != 2 {
		return_error!(temporal::invalid_day(fragment.clone()));
	}

	let day = day_str
		.parse::<u32>()
		.map_err(|_| Error(temporal::invalid_day(fragment.clone())))?;

	Date::new(year, month, day)
		.ok_or_else(|| Error(temporal::invalid_date_values(fragment)))
}

#[cfg(test)]
mod tests {
	use super::parse_date;
	use crate::interface::fragment::OwnedFragment;

	#[test]
	fn test_basic() {
		let span = OwnedFragment::testing("2024-03-15");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-03-15");
	}

	#[test]
	fn test_leap_year() {
		let span = OwnedFragment::testing("2024-02-29");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-02-29");
	}

	#[test]
	fn test_boundaries() {
		let span = OwnedFragment::testing("2000-01-01");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2000-01-01");

		let span = OwnedFragment::testing("2024-12-31");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-12-31");
	}

	#[test]
	fn test_invalid_format() {
		let span = OwnedFragment::testing("2024-03");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_001");
	}

	#[test]
	fn test_invalid_year() {
		let span = OwnedFragment::testing("abcd-03-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");
	}

	#[test]
	fn test_invalid_month() {
		let span = OwnedFragment::testing("2024-invalid-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");
	}

	#[test]
	fn test_invalid_day() {
		let span = OwnedFragment::testing("2024-03-invalid");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");
	}

	#[test]
	fn test_invalid_date_values() {
		let span = OwnedFragment::testing("2024-13-32");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_012");
	}

	#[test]
	fn test_four_digit_year() {
		// Test 2-digit year
		let span = OwnedFragment::testing("24-03-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test 3-digit year
		let span = OwnedFragment::testing("024-03-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test 5-digit year
		let span = OwnedFragment::testing("20240-03-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test year with leading zeros (still 4 digits, should work)
		let span = OwnedFragment::testing("0024-03-15");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "0024-03-15");
	}

	#[test]
	fn test_two_digit_month() {
		// Test 1-digit month
		let span = OwnedFragment::testing("2024-3-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test 3-digit month
		let span = OwnedFragment::testing("2024-003-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test month with leading zeros (still 2 digits, should work)
		let span = OwnedFragment::testing("2024-03-15");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-03-15");

		// Test month with non-digits
		let span = OwnedFragment::testing("2024-0a-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test month with spaces
		let span = OwnedFragment::testing("2024- 3-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");
	}

	#[test]
	fn test_two_digit_day() {
		// Test 1-digit day
		let span = OwnedFragment::testing("2024-03-5");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test 3-digit day
		let span = OwnedFragment::testing("2024-03-015");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test day with leading zeros (still 2 digits, should work)
		let span = OwnedFragment::testing("2024-03-05");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-03-05");

		// Test day with non-digits
		let span = OwnedFragment::testing("2024-03-1a");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test day with spaces
		let span = OwnedFragment::testing("2024-03- 5");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");
	}
}
