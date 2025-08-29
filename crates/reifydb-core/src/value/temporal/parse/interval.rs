// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Error, Interval, interface::fragment::IntoFragment,
	result::error::diagnostic::temporal, return_error,
};

pub fn parse_interval<'a>(
	fragment: impl IntoFragment<'a>,
) -> Result<Interval, Error> {
	let fragment = fragment.into_fragment();
	let fragment_value = fragment.text();
	// Parse ISO 8601 duration format (P1D, PT2H30M, P1Y2M3DT4H5M6S)

	if fragment_value.len() == 1
		|| !fragment_value.starts_with('P')
		|| fragment_value == "PT"
	{
		return_error!(temporal::invalid_interval_format(fragment));
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
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::invalid_unit_in_context(unit_frag, 'Y', true));
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::incomplete_interval_specification(unit_frag));
				}
				let years: i32 = current_number
					.parse()
					.map_err(|_| {
						let start = current_position
							- current_number.len();
						let number_frag = fragment
							.sub_fragment(
								start,
								current_number
									.len(),
							);
						Error(temporal::invalid_interval_component_value(number_frag, 'Y'))
					})?;
				months += years * 12; // Exact: store as months
				current_number.clear();
				current_position += 1;
			}
			'M' => {
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::incomplete_interval_specification(unit_frag));
				}
				let value: i64 = current_number
					.parse()
					.map_err(|_| {
						let start = current_position
							- current_number.len();
						let number_frag = fragment
							.sub_fragment(
								start,
								current_number
									.len(),
							);
						Error(temporal::invalid_interval_component_value(number_frag, 'M'))
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
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::invalid_unit_in_context(unit_frag, 'W', true));
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::incomplete_interval_specification(unit_frag));
				}
				let weeks: i32 = current_number
					.parse()
					.map_err(|_| {
						let start = current_position
							- current_number.len();
						let number_frag = fragment
							.sub_fragment(
								start,
								current_number
									.len(),
							);
						Error(temporal::invalid_interval_component_value(number_frag, 'W'))
					})?;
				days += weeks * 7;
				current_number.clear();
				current_position += 1;
			}
			'D' => {
				if in_time_part {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::invalid_unit_in_context(unit_frag, 'D', true));
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::incomplete_interval_specification(unit_frag));
				}
				let day_value: i32 = current_number
					.parse()
					.map_err(|_| {
						let start = current_position
							- current_number.len();
						let number_frag = fragment
							.sub_fragment(
								start,
								current_number
									.len(),
							);
						Error(temporal::invalid_interval_component_value(number_frag, 'D'))
					})?;
				days += day_value;
				current_number.clear();
				current_position += 1;
			}
			'H' => {
				if !in_time_part {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::invalid_unit_in_context(unit_frag, 'H', false));
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::incomplete_interval_specification(unit_frag));
				}
				let hours: i64 = current_number
					.parse()
					.map_err(|_| {
						let start = current_position
							- current_number.len();
						let number_frag = fragment
							.sub_fragment(
								start,
								current_number
									.len(),
							);
						Error(temporal::invalid_interval_component_value(number_frag, 'H'))
					})?;
				nanos += hours * 60 * 60 * 1_000_000_000;
				current_number.clear();
				current_position += 1;
			}
			'S' => {
				if !in_time_part {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::invalid_unit_in_context(unit_frag, 'S', false));
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(
						current_position,
						1,
					);
					return_error!(temporal::incomplete_interval_specification(unit_frag));
				}
				let seconds: i64 = current_number
					.parse()
					.map_err(|_| {
						let start = current_position
							- current_number.len();
						let number_frag = fragment
							.sub_fragment(
								start,
								current_number
									.len(),
							);
						Error(temporal::invalid_interval_component_value(number_frag, 'S'))
					})?;
				nanos += seconds * 1_000_000_000;
				current_number.clear();
				current_position += 1;
			}
			_ => {
				let char_frag = fragment
					.sub_fragment(current_position, 1);
				return_error!(
					temporal::invalid_interval_character(
						char_frag
					)
				);
			}
		}
	}

	if !current_number.is_empty() {
		let start = current_position - current_number.len();
		let number_frag =
			fragment.sub_fragment(start, current_number.len());
		return_error!(temporal::incomplete_interval_specification(
			number_frag
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
	fn test_comptokenize() {
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
