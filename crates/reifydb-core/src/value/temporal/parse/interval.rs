// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Error, Interval, result::error::diagnostic::temporal,
	interface::fragment::{BorrowedFragment, Fragment},
	return_error,
};

pub fn parse_interval(fragment: impl Fragment) -> Result<Interval, Error> {
	let fragment_value = fragment.value();
	// Parse ISO 8601 duration format (P1D, PT2H30M, P1Y2M3DT4H5M6S)

	if fragment_value.len() == 1 || !fragment_value.starts_with('P') || fragment_value == "PT"
	{
		return_error!(temporal::invalid_interval_format(
			fragment.clone()
		));
	}

	let mut chars = fragment_value.chars().skip(1); // Skip 'P'
	let mut months = 0i32;
	let mut days = 0i32;
	let mut nanos = 0i64;
	let mut current_number = String::new();
	let mut in_time_part = false;
	let mut current_position = 1; // Start after 'P'

	while let Some(c) = chars.next() {
		match c {
			'T' => {
				in_time_part = true;
				current_position += 1;
			}
			'0'..='9' => {
				current_number.push(c);
				current_position += 1;
			}
			'Y' => {
				if in_time_part {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::invalid_unit_in_context(BorrowedFragment::new_internal(unit_char), 'Y', true));
				}
				if current_number.is_empty() {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::incomplete_interval_specification(BorrowedFragment::new_internal(unit_char)));
				}
				let years: i32 = current_number.parse().map_err(|_| {
                    let start = current_position - current_number.len();
                    let number_str = &fragment_value[start..current_position];
                    Error(temporal::invalid_interval_component_value(BorrowedFragment::new_internal(number_str), 'Y'))
                })?;
				months += years * 12; // Exact: store as months
				current_number.clear();
				current_position += 1;
			}
			'M' => {
				if current_number.is_empty() {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::incomplete_interval_specification(BorrowedFragment::new_internal(unit_char)));
				}
				let value: i64 = current_number.parse().map_err(|_| {
                    let start = current_position - current_number.len();
                    let number_str = &fragment_value[start..current_position];
                    Error(temporal::invalid_interval_component_value(BorrowedFragment::new_internal(number_str), 'M'))
                })?;
				if in_time_part {
					nanos += value * 60 * 1_000_000_000; // Minutes
				} else {
					months += value as i32; // Months (exact)
				}
				current_number.clear();
				current_position += 1;
			}
			'W' => {
				if in_time_part {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::invalid_unit_in_context(BorrowedFragment::new_internal(unit_char), 'W', true));
				}
				if current_number.is_empty() {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::incomplete_interval_specification(BorrowedFragment::new_internal(unit_char)));
				}
				let weeks: i32 = current_number.parse().map_err(|_| {
                    let start = current_position - current_number.len();
                    let number_str = &fragment_value[start..current_position];
                    Error(temporal::invalid_interval_component_value(BorrowedFragment::new_internal(number_str), 'W'))
                })?;
				days += weeks * 7;
				current_number.clear();
				current_position += 1;
			}
			'D' => {
				if in_time_part {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::invalid_unit_in_context(BorrowedFragment::new_internal(unit_char), 'D', true));
				}
				if current_number.is_empty() {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::incomplete_interval_specification(BorrowedFragment::new_internal(unit_char)));
				}
				let day_value: i32 = current_number.parse().map_err(|_| {
                    let start = current_position - current_number.len();
                    let number_str = &fragment_value[start..current_position];
                    Error(temporal::invalid_interval_component_value(BorrowedFragment::new_internal(number_str), 'D'))
                })?;
				days += day_value;
				current_number.clear();
				current_position += 1;
			}
			'H' => {
				if !in_time_part {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::invalid_unit_in_context(BorrowedFragment::new_internal(unit_char), 'H', false));
				}
				if current_number.is_empty() {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::incomplete_interval_specification(BorrowedFragment::new_internal(unit_char)));
				}
				let hours: i64 = current_number.parse().map_err(|_| {
                    let start = current_position - current_number.len();
                    let number_str = &fragment_value[start..current_position];
                    Error(temporal::invalid_interval_component_value(BorrowedFragment::new_internal(number_str), 'H'))
                })?;
				nanos += hours * 60 * 60 * 1_000_000_000;
				current_number.clear();
				current_position += 1;
			}
			'S' => {
				if !in_time_part {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::invalid_unit_in_context(BorrowedFragment::new_internal(unit_char), 'S', false));
				}
				if current_number.is_empty() {
					let unit_char = &fragment_value[current_position..current_position + 1];
					return_error!(temporal::incomplete_interval_specification(BorrowedFragment::new_internal(unit_char)));
				}
				let seconds: i64 = current_number.parse().map_err(|_| {
                    let start = current_position - current_number.len();
                    let number_str = &fragment_value[start..current_position];
                    Error(temporal::invalid_interval_component_value(BorrowedFragment::new_internal(number_str), 'S'))
                })?;
				nanos += seconds * 1_000_000_000;
				current_number.clear();
				current_position += 1;
			}
			_ => {
				let char_str = &fragment_value[current_position..current_position + 1];
				return_error!(
					temporal::invalid_interval_character(
						BorrowedFragment::new_internal(char_str)
					)
				);
			}
		}
	}

	if !current_number.is_empty() {
		let start = current_position - current_number.len();
		let number_str = &fragment_value[start..current_position];
		return_error!(temporal::incomplete_interval_specification(
			BorrowedFragment::new_internal(number_str)
		));
	}

	Ok(Interval::new(months, days, nanos))
}

#[cfg(test)]
mod tests {
	use super::parse_interval;
	use crate::interface::fragment::OwnedFragment;

	#[test]
	fn test_days() {
		let fragment = OwnedFragment::testing("P1D");
		let interval = parse_interval(fragment).unwrap();
		// 1 day = 1 day, 0 nanos
		assert_eq!(interval.get_days(), 1);
		assert_eq!(interval.get_nanos(), 0);
	}

	#[test]
	fn test_time_hours_minutes() {
		let fragment = OwnedFragment::testing("PT2H30M");
		let interval = parse_interval(fragment).unwrap();
		// 2 hours 30 minutes = (2 * 60 * 60 + 30 * 60) * 1_000_000_000
		// nanos
		assert_eq!(
			interval.get_nanos(),
			(2 * 60 * 60 + 30 * 60) * 1_000_000_000
		);
	}

	#[test]
	fn test_complex() {
		let fragment = OwnedFragment::testing("P1DT2H30M");
		let interval = parse_interval(fragment).unwrap();
		// 1 day + 2 hours + 30 minutes
		let expected_nanos = (2 * 60 * 60 + 30 * 60) * 1_000_000_000;
		assert_eq!(interval.get_days(), 1);
		assert_eq!(interval.get_nanos(), expected_nanos);
	}

	#[test]
	fn test_seconds_only() {
		let fragment = OwnedFragment::testing("PT45S");
		let interval = parse_interval(fragment).unwrap();
		assert_eq!(interval.get_nanos(), 45 * 1_000_000_000);
	}

	#[test]
	fn test_minutes_only() {
		let fragment = OwnedFragment::testing("PT5M");
		let interval = parse_interval(fragment).unwrap();
		assert_eq!(interval.get_nanos(), 5 * 60 * 1_000_000_000);
	}

	#[test]
	fn test_hours_only() {
		let fragment = OwnedFragment::testing("PT1H");
		let interval = parse_interval(fragment).unwrap();
		assert_eq!(interval.get_nanos(), 60 * 60 * 1_000_000_000);
	}

	#[test]
	fn test_weeks() {
		let fragment = OwnedFragment::testing("P1W");
		let interval = parse_interval(fragment).unwrap();
		assert_eq!(interval.get_days(), 7);
		assert_eq!(interval.get_nanos(), 0);
	}

	#[test]
	fn test_years() {
		let fragment = OwnedFragment::testing("P1Y");
		let interval = parse_interval(fragment).unwrap();
		assert_eq!(interval.get_months(), 12);
		assert_eq!(interval.get_days(), 0);
		assert_eq!(interval.get_nanos(), 0);
	}

	#[test]
	fn test_months() {
		let fragment = OwnedFragment::testing("P1M");
		let interval = parse_interval(fragment).unwrap();
		assert_eq!(interval.get_months(), 1);
		assert_eq!(interval.get_days(), 0);
		assert_eq!(interval.get_nanos(), 0);
	}

	#[test]
	fn test_full_format() {
		let fragment = OwnedFragment::testing("P1Y2M3DT4H5M6S");
		let interval = parse_interval(fragment).unwrap();
		let expected_months = 12 + 2; // 1 year + 2 months
		let expected_days = 3;
		let expected_nanos = 4 * 60 * 60 * 1_000_000_000 +    // 4 hours
                            5 * 60 * 1_000_000_000 +          // 5 minutes
                            6 * 1_000_000_000; // 6 seconds
		assert_eq!(interval.get_months(), expected_months);
		assert_eq!(interval.get_days(), expected_days);
		assert_eq!(interval.get_nanos(), expected_nanos);
	}

	#[test]
	fn test_invalid_format() {
		let fragment = OwnedFragment::testing("invalid");
		let err = parse_interval(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_004");
	}

	#[test]
	fn test_invalid_character() {
		let fragment = OwnedFragment::testing("P1X");
		let err = parse_interval(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_014");
	}

	#[test]
	fn test_years_in_time_part() {
		let fragment = OwnedFragment::testing("PTY");
		let err = parse_interval(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_weeks_in_time_part() {
		let fragment = OwnedFragment::testing("PTW");
		let err = parse_interval(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_days_in_time_part() {
		let fragment = OwnedFragment::testing("PTD");
		let err = parse_interval(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_hours_in_date_part() {
		let fragment = OwnedFragment::testing("P1H");
		let err = parse_interval(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_seconds_in_date_part() {
		let fragment = OwnedFragment::testing("P1S");
		let err = parse_interval(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_incomplete_specification() {
		let fragment = OwnedFragment::testing("P1");
		let err = parse_interval(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_015");
	}
}
