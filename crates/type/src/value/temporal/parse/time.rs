// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{
	error::{Error, TemporalKind, TypeError},
	fragment::Fragment,
	value::Time,
};

pub fn parse_time(fragment: Fragment) -> Result<Time, Error> {
	// Parse time in format HH:MM:SS[.sss[sss[sss]]][Z]
	let fragment = fragment;
	let fragment_value = fragment.text();
	let mut time_str = fragment_value;

	// Check for lowercase 'z' (invalid - must be uppercase 'Z')
	if time_str.ends_with('z') {
		// Calculate position of second component to report error
		let parts: Vec<&str> = time_str.split(':').collect();
		if parts.len() == 3 {
			let hours_len = parts[0].len();
			let minutes_len = parts[1].len();
			let offset = hours_len + 1 + minutes_len + 1; // +1 for each colon
			let second_len = parts[2].len();
			let sub_frag = fragment.sub_fragment(offset, second_len);
			return Err(TypeError::Temporal {
				kind: TemporalKind::InvalidSecond,
				message: format!("invalid second value '{}'", sub_frag.text()),
				fragment: sub_frag,
			}
			.into());
		}
		// If format is wrong, fall through to normal error handling
	}

	if time_str.ends_with('Z') {
		time_str = &time_str[..time_str.len() - 1];
	}

	// Split the time string to check format
	let time_fragment_parts: Vec<&str> = time_str.split(':').collect();

	if time_fragment_parts.len() != 3 {
		return Err(TypeError::Temporal {
			kind: TemporalKind::InvalidTimeFormat,
			message: "invalid time format".into(),
			fragment,
		}
		.into());
	}

	// Check for empty time parts first (before format validation)
	let mut offset = 0;
	if time_fragment_parts[0].trim().is_empty() {
		let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[0].len());
		return Err(TypeError::Temporal {
			kind: TemporalKind::EmptyTimeComponent,
			message: "empty time component".into(),
			fragment: sub_frag,
		}
		.into());
	}
	offset += time_fragment_parts[0].len() + 1; // +1 for the colon

	if time_fragment_parts[1].trim().is_empty() {
		let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[1].len());
		return Err(TypeError::Temporal {
			kind: TemporalKind::EmptyTimeComponent,
			message: "empty time component".into(),
			fragment: sub_frag,
		}
		.into());
	}
	offset += time_fragment_parts[1].len() + 1; // +1 for the colon

	if time_fragment_parts[2].trim().is_empty() {
		let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[2].len());
		return Err(TypeError::Temporal {
			kind: TemporalKind::EmptyTimeComponent,
			message: "empty time component".into(),
			fragment: sub_frag,
		}
		.into());
	}

	// Validate exactly 2 digits for HH:MM:SS format
	offset = 0;

	// Validate hour part (exactly 2 digits)
	if time_fragment_parts[0].len() != 2 {
		let frag = fragment.sub_fragment(offset, time_fragment_parts[0].len());
		return Err(TypeError::Temporal {
			kind: TemporalKind::InvalidTimeComponentFormat {
				component: "hour".to_string(),
			},
			message: format!("invalid {} format '{}'", "hour", frag.text()),
			fragment: frag,
		}
		.into());
	}
	offset += time_fragment_parts[0].len() + 1; // +1 for the colon

	// Validate minute part (exactly 2 digits)
	if time_fragment_parts[1].len() != 2 {
		let frag = fragment.sub_fragment(offset, time_fragment_parts[1].len());
		return Err(TypeError::Temporal {
			kind: TemporalKind::InvalidTimeComponentFormat {
				component: "minute".to_string(),
			},
			message: format!("invalid {} format '{}'", "minute", frag.text()),
			fragment: frag,
		}
		.into());
	}
	offset += time_fragment_parts[1].len() + 1; // +1 for the colon

	// Validate second part (exactly 2 digits before any fractional part)
	let second_base = time_fragment_parts[2].split('.').next().unwrap();
	if second_base.len() != 2 {
		let frag = fragment.sub_fragment(offset, second_base.len());
		return Err(TypeError::Temporal {
			kind: TemporalKind::InvalidTimeComponentFormat {
				component: "second".to_string(),
			},
			message: format!("invalid {} format '{}'", "second", frag.text()),
			fragment: frag,
		}
		.into());
	}

	// Reset offset calculation for parsing components
	offset = 0;
	let hour = time_fragment_parts[0].trim().parse::<u32>().map_err(|_| {
		let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[0].len());
		let err: Error = TypeError::Temporal {
			kind: TemporalKind::InvalidHour,
			message: format!("invalid hour value '{}'", sub_frag.text()),
			fragment: sub_frag,
		}
		.into();
		err
	})?;
	offset += time_fragment_parts[0].len() + 1;

	let minute = time_fragment_parts[1].trim().parse::<u32>().map_err(|_| {
		let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[1].len());
		let err: Error = TypeError::Temporal {
			kind: TemporalKind::InvalidMinute,
			message: format!("invalid minute value '{}'", sub_frag.text()),
			fragment: sub_frag,
		}
		.into();
		err
	})?;
	offset += time_fragment_parts[1].len() + 1;

	// Parse seconds and optional fractional seconds
	let seconds_with_fraction = time_fragment_parts[2].trim();
	let (second, nanosecond) = if seconds_with_fraction.contains('.') {
		let second_parts: Vec<&str> = seconds_with_fraction.split('.').collect();
		if second_parts.len() != 2 {
			let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[2].len());
			return Err(TypeError::Temporal {
				kind: TemporalKind::InvalidFractionalSeconds,
				message: format!("invalid fractional seconds value '{}'", sub_frag.text()),
				fragment: sub_frag,
			}
			.into());
		}

		let second = second_parts[0].parse::<u32>().map_err(|_| {
			let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[2].len());
			let err: Error = TypeError::Temporal {
				kind: TemporalKind::InvalidSecond,
				message: format!("invalid second value '{}'", sub_frag.text()),
				fragment: sub_frag,
			}
			.into();
			err
		})?;
		let fraction_str = second_parts[1];

		// Pad or truncate fractional seconds to 9 digits (nanoseconds)
		let padded_fraction = if fraction_str.len() < 9 {
			format!("{:0<9}", fraction_str)
		} else {
			fraction_str[..9].to_string()
		};

		let nanosecond = padded_fraction.parse::<u32>().map_err(|_| {
			let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[2].len());
			let err: Error = TypeError::Temporal {
				kind: TemporalKind::InvalidFractionalSeconds,
				message: format!("invalid fractional seconds value '{}'", sub_frag.text()),
				fragment: sub_frag,
			}
			.into();
			err
		})?;
		(second, nanosecond)
	} else {
		let second = seconds_with_fraction.parse::<u32>().map_err(|_| {
			let sub_frag = fragment.sub_fragment(offset, time_fragment_parts[2].len());
			let err: Error = TypeError::Temporal {
				kind: TemporalKind::InvalidSecond,
				message: format!("invalid second value '{}'", sub_frag.text()),
				fragment: sub_frag,
			}
			.into();
			err
		})?;
		(second, 0)
	};

	Time::new(hour, minute, second, nanosecond).ok_or_else(|| {
		let err: Error = TypeError::Temporal {
			kind: TemporalKind::InvalidTimeValues,
			message: "invalid time values".into(),
			fragment,
		}
		.into();
		err
	})
}

#[cfg(test)]
pub mod tests {
	use super::parse_time;
	use crate::fragment::Fragment;

	#[test]
	fn test_basic() {
		let fragment = Fragment::testing("14:30:00");
		let time = parse_time(fragment).unwrap();
		assert_eq!(time.to_string(), "14:30:00.000000000");
	}

	#[test]
	fn test_with_timezone_z() {
		let fragment = Fragment::testing("14:30:00Z");
		let time = parse_time(fragment).unwrap();
		assert_eq!(time.to_string(), "14:30:00.000000000");
	}

	#[test]
	fn test_with_milliseconds() {
		let fragment = Fragment::testing("14:30:00.123");
		let time = parse_time(fragment).unwrap();
		assert_eq!(time.to_string(), "14:30:00.123000000");
	}

	#[test]
	fn test_with_microseconds() {
		let fragment = Fragment::testing("14:30:00.123456");
		let time = parse_time(fragment).unwrap();
		assert_eq!(time.to_string(), "14:30:00.123456000");
	}

	#[test]
	fn test_with_nanoseconds() {
		let fragment = Fragment::testing("14:30:00.123456789");
		let time = parse_time(fragment).unwrap();
		assert_eq!(time.to_string(), "14:30:00.123456789");
	}

	#[test]
	fn test_with_utc_timezone() {
		let fragment = Fragment::testing("14:30:00Z");
		let time = parse_time(fragment).unwrap();
		assert_eq!(time.to_string(), "14:30:00.000000000");
	}

	#[test]
	fn test_boundaries() {
		let fragment = Fragment::testing("00:00:00");
		let time = parse_time(fragment).unwrap();
		assert_eq!(time.to_string(), "00:00:00.000000000");

		let fragment = Fragment::testing("23:59:59");
		let time = parse_time(fragment).unwrap();
		assert_eq!(time.to_string(), "23:59:59.000000000");
	}

	#[test]
	fn test_invalid_format() {
		let fragment = Fragment::testing("14:30");
		let err = parse_time(fragment).unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_003");
	}

	#[test]
	fn test_invalid_hour() {
		let fragment = Fragment::testing("invalid:30:00");
		let result = parse_time(fragment);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");
	}

	#[test]
	fn test_invalid_minute() {
		let fragment = Fragment::testing("14:invalid:00");
		let result = parse_time(fragment);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");
	}

	#[test]
	fn test_invalid_second() {
		let fragment = Fragment::testing("14:30:invalid");
		let result = parse_time(fragment);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_005");
	}

	#[test]
	fn test_invalid_time_values() {
		let fragment = Fragment::testing("25:70:80");
		let result = parse_time(fragment);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_013");
	}

	#[test]
	fn test_invalid_fractional_seconds() {
		let fragment = Fragment::testing("14:30:00.123.456");
		let result = parse_time(fragment);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_011");
	}

	#[test]
	fn test_lowercase_z_rejected() {
		let fragment = Fragment::testing("14:30:00z");
		let result = parse_time(fragment);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TEMPORAL_010"); // invalid_second
	}
}
