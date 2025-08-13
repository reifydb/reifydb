// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Date, Error, Span, result::error::diagnostic::temporal, return_error,
};

pub fn parse_date(span: impl Span) -> Result<Date, Error> {
	let span_parts = span.split('-');
	if span_parts.len() != 3 {
		return_error!(temporal::invalid_date_format(span.to_owned()));
	}

	// Check for empty parts
	if span_parts[0].trimmed_fragment().is_empty() {
		return_error!(temporal::empty_date_component(
			span_parts[0].clone()
		));
	}
	if span_parts[1].trimmed_fragment().is_empty() {
		return_error!(temporal::empty_date_component(
			span_parts[1].clone()
		));
	}
	if span_parts[2].trimmed_fragment().is_empty() {
		return_error!(temporal::empty_date_component(
			span_parts[2].clone()
		));
	}

	let year_str = span_parts[0].trimmed_fragment();
	if year_str.len() != 4 {
		return_error!(temporal::invalid_year(span_parts[0].clone()));
	}

	let year = year_str.parse::<i32>().map_err(|_| {
		Error(temporal::invalid_year(span_parts[0].clone()))
	})?;

	let month_str = span_parts[1].trimmed_fragment();
	if month_str.len() != 2 {
		return_error!(temporal::invalid_month(span_parts[1].clone()));
	}

	let month = month_str.parse::<u32>().map_err(|_| {
		Error(temporal::invalid_month(span_parts[1].clone()))
	})?;

	let day_str = span_parts[2].trimmed_fragment();
	if day_str.len() != 2 {
		return_error!(temporal::invalid_day(span_parts[2].clone()));
	}

	let day = day_str.parse::<u32>().map_err(|_| {
		Error(temporal::invalid_day(span_parts[2].clone()))
	})?;

	Date::new(year, month, day).ok_or_else(|| {
		Error(temporal::invalid_date_values(span.to_owned()))
	})
}

#[cfg(test)]
mod tests {
	use super::parse_date;
	use crate::OwnedSpan;

	#[test]
	fn test_basic() {
		let span = OwnedSpan::testing("2024-03-15");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-03-15");
	}

	#[test]
	fn test_leap_year() {
		let span = OwnedSpan::testing("2024-02-29");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-02-29");
	}

	#[test]
	fn test_boundaries() {
		let span = OwnedSpan::testing("2000-01-01");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2000-01-01");

		let span = OwnedSpan::testing("2024-12-31");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-12-31");
	}

	#[test]
	fn test_invalid_format() {
		let span = OwnedSpan::testing("2024-03");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_001");
	}

	#[test]
	fn test_invalid_year() {
		let span = OwnedSpan::testing("abcd-03-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");
	}

	#[test]
	fn test_invalid_month() {
		let span = OwnedSpan::testing("2024-invalid-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");
	}

	#[test]
	fn test_invalid_day() {
		let span = OwnedSpan::testing("2024-03-invalid");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");
	}

	#[test]
	fn test_invalid_date_values() {
		let span = OwnedSpan::testing("2024-13-32");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_012");
	}

	#[test]
	fn test_four_digit_year() {
		// Test 2-digit year
		let span = OwnedSpan::testing("24-03-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test 3-digit year
		let span = OwnedSpan::testing("024-03-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test 5-digit year
		let span = OwnedSpan::testing("20240-03-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");

		// Test year with leading zeros (still 4 digits, should work)
		let span = OwnedSpan::testing("0024-03-15");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "0024-03-15");
	}

	#[test]
	fn test_two_digit_month() {
		// Test 1-digit month
		let span = OwnedSpan::testing("2024-3-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test 3-digit month
		let span = OwnedSpan::testing("2024-003-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test month with leading zeros (still 2 digits, should work)
		let span = OwnedSpan::testing("2024-03-15");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-03-15");

		// Test month with non-digits
		let span = OwnedSpan::testing("2024-0a-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");

		// Test month with spaces
		let span = OwnedSpan::testing("2024- 3-15");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_006");
	}

	#[test]
	fn test_two_digit_day() {
		// Test 1-digit day
		let span = OwnedSpan::testing("2024-03-5");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test 3-digit day
		let span = OwnedSpan::testing("2024-03-015");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test day with leading zeros (still 2 digits, should work)
		let span = OwnedSpan::testing("2024-03-05");
		let date = parse_date(span).unwrap();
		assert_eq!(date.to_string(), "2024-03-05");

		// Test day with non-digits
		let span = OwnedSpan::testing("2024-03-1a");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");

		// Test day with spaces
		let span = OwnedSpan::testing("2024-03- 5");
		let err = parse_date(span).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_007");
	}
}
