// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{
	error::{Error, TemporalKind, TypeError},
	fragment::Fragment,
	value::Duration,
};

fn validate_component_order(
	component: char,
	seen: &mut std::collections::HashSet<char>,
	last_order: &mut u8,
	current_order: u8,
	fragment: Fragment,
	position: usize,
) -> Result<(), Error> {
	let key = if component == 'M' {
		if *last_order > 0 {
			'M'
		} else {
			'M'
		}
	} else {
		component
	};

	if seen.contains(&key) {
		let frag = fragment.sub_fragment(position, 1);
		return Err(TypeError::Temporal {
			kind: TemporalKind::DuplicateDurationComponent {
				component,
			},
			message: format!("duplicate duration component '{}'", component),
			fragment: frag,
		}
		.into());
	}

	if current_order <= *last_order {
		let frag = fragment.sub_fragment(position, 1);
		return Err(TypeError::Temporal {
			kind: TemporalKind::OutOfOrderDurationComponent {
				component,
			},
			message: format!("duration component '{}' is out of order", component),
			fragment: frag,
		}
		.into());
	}

	seen.insert(key);
	*last_order = current_order;
	Ok(())
}

pub fn parse_duration(fragment: Fragment) -> Result<Duration, Error> {
	let fragment = fragment;
	let fragment_value = fragment.text();
	// Parse ISO 8601 duration format (P1D, PT2H30M, P1Y2M3DT4H5M6S)

	if fragment_value.len() == 1 || !fragment_value.starts_with('P') || fragment_value == "PT" {
		return Err(TypeError::Temporal {
			kind: TemporalKind::InvalidDurationFormat,
			message: "invalid duration format".into(),
			fragment,
		}
		.into());
	}

	let mut chars = fragment_value.chars().skip(1); // Skip 'P'
	let mut months = 0i32;
	let mut days = 0i32;
	let mut nanos = 0i64;
	let mut current_number = String::new();
	let mut in_time_part = false;
	let mut current_position = 1; // Start after 'P'

	let mut seen_date_components = std::collections::HashSet::new();
	let mut seen_time_components = std::collections::HashSet::new();
	let mut last_date_component_order = 0u8;
	let mut last_time_component_order = 0u8;

	while let Some(c) = chars.next() {
		match c {
			'T' => {
				in_time_part = true;
				current_position += 1;
			}
			'0'..='9' | '.' => {
				current_number.push(c);
				current_position += 1;
			}
			'Y' => {
				if in_time_part {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidUnitInContext {
							unit: 'Y',
							in_time_part: true,
						},
						message: format!("invalid unit '{}' in {}", 'Y', "time part (after T)"),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::IncompleteDurationSpecification,
						message: "incomplete duration specification".into(),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.contains('.') {
					let start = current_position - current_number.len();
					let dot_pos = start + current_number.find('.').unwrap();
					let char_frag = fragment.sub_fragment(dot_pos, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidDurationCharacter,
						message: format!(
							"invalid character in duration '{}'",
							char_frag.text()
						),
						fragment: char_frag,
					}
					.into());
				}

				validate_component_order(
					'Y',
					&mut seen_date_components,
					&mut last_date_component_order,
					1,
					fragment.clone(),
					current_position,
				)?;

				let years: i32 = current_number.parse().map_err(|_| {
					let start = current_position - current_number.len();
					let number_frag = fragment.sub_fragment(start, current_number.len());
					let err: Error = TypeError::Temporal {
						kind: TemporalKind::InvalidDurationComponentValue {
							unit: 'Y',
						},
						message: format!("invalid year value '{}'", number_frag.text()),
						fragment: number_frag,
					}
					.into();
					err
				})?;
				months += years * 12;
				current_number.clear();
				current_position += 1;
			}
			'M' => {
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::IncompleteDurationSpecification,
						message: "incomplete duration specification".into(),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.contains('.') {
					let start = current_position - current_number.len();
					let dot_pos = start + current_number.find('.').unwrap();
					let char_frag = fragment.sub_fragment(dot_pos, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidDurationCharacter,
						message: format!(
							"invalid character in duration '{}'",
							char_frag.text()
						),
						fragment: char_frag,
					}
					.into());
				}

				if in_time_part {
					validate_component_order(
						'M',
						&mut seen_time_components,
						&mut last_time_component_order,
						2,
						fragment.clone(),
						current_position,
					)?;
				} else {
					validate_component_order(
						'M',
						&mut seen_date_components,
						&mut last_date_component_order,
						2,
						fragment.clone(),
						current_position,
					)?;
				}

				let value: i64 = current_number.parse().map_err(|_| {
					let start = current_position - current_number.len();
					let number_frag = fragment.sub_fragment(start, current_number.len());
					let err: Error = TypeError::Temporal {
						kind: TemporalKind::InvalidDurationComponentValue {
							unit: 'M',
						},
						message: format!("invalid month/minute value '{}'", number_frag.text()),
						fragment: number_frag,
					}
					.into();
					err
				})?;
				if in_time_part {
					nanos += value * 60 * 1_000_000_000;
				} else {
					months += value as i32;
				}
				current_number.clear();
				current_position += 1;
			}
			'W' => {
				if in_time_part {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidUnitInContext {
							unit: 'W',
							in_time_part: true,
						},
						message: format!("invalid unit '{}' in {}", 'W', "time part (after T)"),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::IncompleteDurationSpecification,
						message: "incomplete duration specification".into(),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.contains('.') {
					let start = current_position - current_number.len();
					let dot_pos = start + current_number.find('.').unwrap();
					let char_frag = fragment.sub_fragment(dot_pos, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidDurationCharacter,
						message: format!(
							"invalid character in duration '{}'",
							char_frag.text()
						),
						fragment: char_frag,
					}
					.into());
				}

				validate_component_order(
					'W',
					&mut seen_date_components,
					&mut last_date_component_order,
					3,
					fragment.clone(),
					current_position,
				)?;

				let weeks: i32 = current_number.parse().map_err(|_| {
					let start = current_position - current_number.len();
					let number_frag = fragment.sub_fragment(start, current_number.len());
					let err: Error = TypeError::Temporal {
						kind: TemporalKind::InvalidDurationComponentValue {
							unit: 'W',
						},
						message: format!("invalid week value '{}'", number_frag.text()),
						fragment: number_frag,
					}
					.into();
					err
				})?;
				days += weeks * 7;
				current_number.clear();
				current_position += 1;
			}
			'D' => {
				if in_time_part {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidUnitInContext {
							unit: 'D',
							in_time_part: true,
						},
						message: format!("invalid unit '{}' in {}", 'D', "time part (after T)"),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::IncompleteDurationSpecification,
						message: "incomplete duration specification".into(),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.contains('.') {
					let start = current_position - current_number.len();
					let dot_pos = start + current_number.find('.').unwrap();
					let char_frag = fragment.sub_fragment(dot_pos, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidDurationCharacter,
						message: format!(
							"invalid character in duration '{}'",
							char_frag.text()
						),
						fragment: char_frag,
					}
					.into());
				}

				validate_component_order(
					'D',
					&mut seen_date_components,
					&mut last_date_component_order,
					4,
					fragment.clone(),
					current_position,
				)?;

				let day_value: i32 = current_number.parse().map_err(|_| {
					let start = current_position - current_number.len();
					let number_frag = fragment.sub_fragment(start, current_number.len());
					let err: Error = TypeError::Temporal {
						kind: TemporalKind::InvalidDurationComponentValue {
							unit: 'D',
						},
						message: format!("invalid day value '{}'", number_frag.text()),
						fragment: number_frag,
					}
					.into();
					err
				})?;
				days += day_value;
				current_number.clear();
				current_position += 1;
			}
			'H' => {
				if !in_time_part {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidUnitInContext {
							unit: 'H',
							in_time_part: false,
						},
						message: format!(
							"invalid unit '{}' in {}",
							'H', "date part (before T)"
						),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::IncompleteDurationSpecification,
						message: "incomplete duration specification".into(),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.contains('.') {
					let start = current_position - current_number.len();
					let dot_pos = start + current_number.find('.').unwrap();
					let char_frag = fragment.sub_fragment(dot_pos, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidDurationCharacter,
						message: format!(
							"invalid character in duration '{}'",
							char_frag.text()
						),
						fragment: char_frag,
					}
					.into());
				}

				validate_component_order(
					'H',
					&mut seen_time_components,
					&mut last_time_component_order,
					1,
					fragment.clone(),
					current_position,
				)?;

				let hours: i64 = current_number.parse().map_err(|_| {
					let start = current_position - current_number.len();
					let number_frag = fragment.sub_fragment(start, current_number.len());
					let err: Error = TypeError::Temporal {
						kind: TemporalKind::InvalidDurationComponentValue {
							unit: 'H',
						},
						message: format!("invalid hour value '{}'", number_frag.text()),
						fragment: number_frag,
					}
					.into();
					err
				})?;
				nanos += hours * 60 * 60 * 1_000_000_000;
				current_number.clear();
				current_position += 1;
			}
			'S' => {
				if !in_time_part {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::InvalidUnitInContext {
							unit: 'S',
							in_time_part: false,
						},
						message: format!(
							"invalid unit '{}' in {}",
							'S', "date part (before T)"
						),
						fragment: unit_frag,
					}
					.into());
				}
				if current_number.is_empty() {
					let unit_frag = fragment.sub_fragment(current_position, 1);
					return Err(TypeError::Temporal {
						kind: TemporalKind::IncompleteDurationSpecification,
						message: "incomplete duration specification".into(),
						fragment: unit_frag,
					}
					.into());
				}

				validate_component_order(
					'S',
					&mut seen_time_components,
					&mut last_time_component_order,
					3,
					fragment.clone(),
					current_position,
				)?;

				if current_number.contains('.') {
					let seconds_float: f64 = current_number.parse().map_err(|_| {
						let start = current_position - current_number.len();
						let number_frag = fragment.sub_fragment(start, current_number.len());
						let err: Error = TypeError::Temporal {
							kind: TemporalKind::InvalidDurationComponentValue {
								unit: 'S',
							},
							message: format!(
								"invalid second value '{}'",
								number_frag.text()
							),
							fragment: number_frag,
						}
						.into();
						err
					})?;
					nanos += (seconds_float * 1_000_000_000.0) as i64;
				} else {
					let seconds: i64 = current_number.parse().map_err(|_| {
						let start = current_position - current_number.len();
						let number_frag = fragment.sub_fragment(start, current_number.len());
						let err: Error = TypeError::Temporal {
							kind: TemporalKind::InvalidDurationComponentValue {
								unit: 'S',
							},
							message: format!(
								"invalid second value '{}'",
								number_frag.text()
							),
							fragment: number_frag,
						}
						.into();
						err
					})?;
					nanos += seconds * 1_000_000_000;
				}

				current_number.clear();
				current_position += 1;
			}
			_ => {
				let char_frag = fragment.sub_fragment(current_position, 1);
				return Err(TypeError::Temporal {
					kind: TemporalKind::InvalidDurationCharacter,
					message: format!("invalid character in duration '{}'", char_frag.text()),
					fragment: char_frag,
				}
				.into());
			}
		}
	}

	if !current_number.is_empty() {
		let start = current_position - current_number.len();
		let number_frag = fragment.sub_fragment(start, current_number.len());
		return Err(TypeError::Temporal {
			kind: TemporalKind::IncompleteDurationSpecification,
			message: "incomplete duration specification".into(),
			fragment: number_frag,
		}
		.into());
	}

	Ok(Duration::new(months, days, nanos))
}

#[cfg(test)]
pub mod tests {
	use super::parse_duration;
	use crate::fragment::Fragment;

	#[test]
	fn test_days() {
		let fragment = Fragment::testing("P1D");
		let duration = parse_duration(fragment).unwrap();
		// 1 day = 1 day, 0 nanos
		assert_eq!(duration.get_days(), 1);
		assert_eq!(duration.get_nanos(), 0);
	}

	#[test]
	fn test_time_hours_minutes() {
		let fragment = Fragment::testing("PT2H30M");
		let duration = parse_duration(fragment).unwrap();
		// 2 hours 30 minutes = (2 * 60 * 60 + 30 * 60) * 1_000_000_000
		// nanos
		assert_eq!(duration.get_nanos(), (2 * 60 * 60 + 30 * 60) * 1_000_000_000);
	}

	#[test]
	fn test_comptokenize() {
		let fragment = Fragment::testing("P1DT2H30M");
		let duration = parse_duration(fragment).unwrap();
		// 1 day + 2 hours + 30 minutes
		let expected_nanos = (2 * 60 * 60 + 30 * 60) * 1_000_000_000;
		assert_eq!(duration.get_days(), 1);
		assert_eq!(duration.get_nanos(), expected_nanos);
	}

	#[test]
	fn test_seconds_only() {
		let fragment = Fragment::testing("PT45S");
		let duration = parse_duration(fragment).unwrap();
		assert_eq!(duration.get_nanos(), 45 * 1_000_000_000);
	}

	#[test]
	fn test_minutes_only() {
		let fragment = Fragment::testing("PT5M");
		let duration = parse_duration(fragment).unwrap();
		assert_eq!(duration.get_nanos(), 5 * 60 * 1_000_000_000);
	}

	#[test]
	fn test_hours_only() {
		let fragment = Fragment::testing("PT1H");
		let duration = parse_duration(fragment).unwrap();
		assert_eq!(duration.get_nanos(), 60 * 60 * 1_000_000_000);
	}

	#[test]
	fn test_weeks() {
		let fragment = Fragment::testing("P1W");
		let duration = parse_duration(fragment).unwrap();
		assert_eq!(duration.get_days(), 7);
		assert_eq!(duration.get_nanos(), 0);
	}

	#[test]
	fn test_years() {
		let fragment = Fragment::testing("P1Y");
		let duration = parse_duration(fragment).unwrap();
		assert_eq!(duration.get_months(), 12);
		assert_eq!(duration.get_days(), 0);
		assert_eq!(duration.get_nanos(), 0);
	}

	#[test]
	fn test_months() {
		let fragment = Fragment::testing("P1M");
		let duration = parse_duration(fragment).unwrap();
		assert_eq!(duration.get_months(), 1);
		assert_eq!(duration.get_days(), 0);
		assert_eq!(duration.get_nanos(), 0);
	}

	#[test]
	fn test_full_format() {
		let fragment = Fragment::testing("P1Y2M3DT4H5M6S");
		let duration = parse_duration(fragment).unwrap();
		let expected_months = 12 + 2; // 1 year + 2 months
		let expected_days = 3;
		let expected_nanos = 4 * 60 * 60 * 1_000_000_000 +    // 4 hours
                            5 * 60 * 1_000_000_000 +          // 5 minutes
                            6 * 1_000_000_000; // 6 seconds
		assert_eq!(duration.get_months(), expected_months);
		assert_eq!(duration.get_days(), expected_days);
		assert_eq!(duration.get_nanos(), expected_nanos);
	}

	#[test]
	fn test_invalid_format() {
		let fragment = Fragment::testing("invalid");
		let err = parse_duration(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_004");
	}

	#[test]
	fn test_invalid_character() {
		let fragment = Fragment::testing("P1X");
		let err = parse_duration(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_014");
	}

	#[test]
	fn test_years_in_time_part() {
		let fragment = Fragment::testing("PTY");
		let err = parse_duration(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_weeks_in_time_part() {
		let fragment = Fragment::testing("PTW");
		let err = parse_duration(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_days_in_time_part() {
		let fragment = Fragment::testing("PTD");
		let err = parse_duration(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_hours_in_date_part() {
		let fragment = Fragment::testing("P1H");
		let err = parse_duration(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_seconds_in_date_part() {
		let fragment = Fragment::testing("P1S");
		let err = parse_duration(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_016");
	}

	#[test]
	fn test_incomplete_specification() {
		let fragment = Fragment::testing("P1");
		let err = parse_duration(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_015");
	}
}
