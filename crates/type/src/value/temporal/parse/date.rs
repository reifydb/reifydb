// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{Date, Error, Fragment, error::diagnostic::temporal, return_error};

pub fn parse_date(fragment: Fragment) -> Result<Date, Error> {
	let fragment = fragment;
	let value = fragment.text();
	let parts: Vec<&str> = value.split('-').collect();

	if parts.len() != 3 {
		return_error!(temporal::invalid_date_format(fragment));
	}

	// Check for empty parts and calculate positions
	let year_str = parts[0].trim();
	let month_str = parts[1].trim();
	let day_str = parts[2].trim();

	let mut offset = 0;
	if year_str.is_empty() {
		let year_frag = fragment.sub_fragment(offset, parts[0].len());
		return_error!(temporal::empty_date_component(year_frag));
	}
	offset += parts[0].len() + 1; // +1 for dash

	if month_str.is_empty() {
		let month_frag = fragment.sub_fragment(offset, parts[1].len());
		return_error!(temporal::empty_date_component(month_frag));
	}
	offset += parts[1].len() + 1; // +1 for dash

	if day_str.is_empty() {
		let day_frag = fragment.sub_fragment(offset, parts[2].len());
		return_error!(temporal::empty_date_component(day_frag));
	}

	// Reset offset for further validation
	offset = 0;

	if year_str.len() != 4 {
		let year_frag = fragment.sub_fragment(offset, parts[0].len());
		return_error!(temporal::invalid_year(year_frag));
	}

	let year = year_str.parse::<i32>().map_err(|_| {
		let year_frag = fragment.sub_fragment(offset, parts[0].len());
		Error(temporal::invalid_year(year_frag))
	})?;
	offset += parts[0].len() + 1; // +1 for dash

	if month_str.len() != 2 {
		let month_frag = fragment.sub_fragment(offset, parts[1].len());
		return_error!(temporal::invalid_month(month_frag));
	}

	let month = month_str.parse::<u32>().map_err(|_| {
		let month_frag = fragment.sub_fragment(offset, parts[1].len());
		Error(temporal::invalid_month(month_frag))
	})?;
	offset += parts[1].len() + 1; // +1 for dash

	if day_str.len() != 2 {
		let day_frag = fragment.sub_fragment(offset, parts[2].len());
		return_error!(temporal::invalid_day(day_frag));
	}

	let day = day_str.parse::<u32>().map_err(|_| {
		let day_frag = fragment.sub_fragment(offset, parts[2].len());
		Error(temporal::invalid_day(day_frag))
	})?;

	Date::new(year, month, day).ok_or_else(|| Error(temporal::invalid_date_values(fragment)))
}

#[cfg(test)]
mod tests {
	use super::parse_date;
	use crate::Fragment;

	#[test]
	fn test_basic() {
		let fragment = Fragment::testing("2024-03-15");
		let date = parse_date(fragment).unwrap();
		assert_eq!(date.to_string(), "2024-03-15");
	}

	#[test]
	fn test_leap_year() {
		let fragment = Fragment::testing("2024-02-29");
		let date = parse_date(fragment).unwrap();
		assert_eq!(date.to_string(), "2024-02-29");
	}

	#[test]
	fn test_boundaries() {
		let fragment = Fragment::testing("2000-01-01");
		let date = parse_date(fragment).unwrap();
		assert_eq!(date.to_string(), "2000-01-01");

		let fragment = Fragment::testing("2024-12-31");
		let date = parse_date(fragment).unwrap();
		assert_eq!(date.to_string(), "2024-12-31");
	}

	#[test]
	fn test_invalid_format() {
		let fragment = Fragment::testing("2024-03");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_001");
	}

	#[test]
	fn test_invalid_year() {
		let fragment = Fragment::testing("abcd-03-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");
	}

	#[test]
	fn test_invalid_month() {
		let fragment = Fragment::testing("2024-invalid-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");
	}

	#[test]
	fn test_invalid_day() {
		let fragment = Fragment::testing("2024-03-invalid");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");
	}

	#[test]
	fn test_invalid_date_values() {
		let fragment = Fragment::testing("2024-13-32");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_012");
	}

	#[test]
	fn test_four_digit_year() {
		// Test 2-digit year
		let fragment = Fragment::testing("24-03-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test 3-digit year
		let fragment = Fragment::testing("024-03-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test 5-digit year
		let fragment = Fragment::testing("20240-03-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test year with leading zeros (still 4 digits, should work)
		let fragment = Fragment::testing("0024-03-15");
		let date = parse_date(fragment).unwrap();
		assert_eq!(date.to_string(), "0024-03-15");
	}

	#[test]
	fn test_two_digit_month() {
		// Test 1-digit month
		let fragment = Fragment::testing("2024-3-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test 3-digit month
		let fragment = Fragment::testing("2024-003-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test month with leading zeros (still 2 digits, should work)
		let fragment = Fragment::testing("2024-03-15");
		let date = parse_date(fragment).unwrap();
		assert_eq!(date.to_string(), "2024-03-15");

		// Test month with non-digits
		let fragment = Fragment::testing("2024-0a-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test month with spaces
		let fragment = Fragment::testing("2024- 3-15");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");
	}

	#[test]
	fn test_two_digit_day() {
		// Test 1-digit day
		let fragment = Fragment::testing("2024-03-5");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test 3-digit day
		let fragment = Fragment::testing("2024-03-015");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test day with leading zeros (still 2 digits, should work)
		let fragment = Fragment::testing("2024-03-05");
		let date = parse_date(fragment).unwrap();
		assert_eq!(date.to_string(), "2024-03-05");

		// Test day with non-digits
		let fragment = Fragment::testing("2024-03-1a");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test day with spaces
		let fragment = Fragment::testing("2024-03- 5");
		let err = parse_date(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");
	}
}
