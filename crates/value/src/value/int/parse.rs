// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use crate::{
	error::{Error, TypeError},
	fragment::Fragment,
	value::{
		decimal::unscaled::ParseError,
		int::{Int, parse_error},
		value_type::ValueType,
	},
};

pub fn parse_int(fragment: Fragment) -> Result<Int, Error> {
	let raw_value = fragment.text();

	let needs_trimming = raw_value.as_bytes().first().is_some_and(|&b| b.is_ascii_whitespace())
		|| raw_value.as_bytes().last().is_some_and(|&b| b.is_ascii_whitespace());
	let has_underscores = raw_value.as_bytes().contains(&b'_');

	let value = match (needs_trimming, has_underscores) {
		(false, false) => Cow::Borrowed(raw_value),

		(true, false) => Cow::Borrowed(raw_value.trim()),
		(false, true) => Cow::Owned(raw_value.replace('_', "")),
		(true, true) => Cow::Owned(raw_value.trim().replace('_', "")),
	};

	if value.is_empty() {
		return Err(TypeError::InvalidNumberFormat {
			target: ValueType::INT,
			fragment,
		}
		.into());
	}

	match Int::parse(&value) {
		Ok(int) => Ok(int),
		Err(ParseError::Invalid) if value.parse::<f64>().is_ok_and(f64::is_infinite) => {
			Err(parse_error(ParseError::OutOfRange, fragment))
		}
		Err(error) => Err(parse_error(error, fragment)),
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_parse_int_valid_zero() {
		assert_eq!(parse_int(Fragment::testing("0")).unwrap(), Int::zero());
	}

	#[test]
	fn test_parse_int_valid_positive() {
		let result = parse_int(Fragment::testing("12345")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_int_valid_negative() {
		let result = parse_int(Fragment::testing("-12345")).unwrap();
		assert_eq!(format!("{}", result), "-12345");
	}

	#[test]
	fn test_parse_int_large_positive() {
		let large_num = "123456789012345678901234567890";
		let result = parse_int(Fragment::testing(large_num)).unwrap();
		assert_eq!(format!("{}", result), large_num);
	}

	#[test]
	fn test_parse_int_large_negative() {
		let large_num = "-123456789012345678901234567890";
		let result = parse_int(Fragment::testing(large_num)).unwrap();
		assert_eq!(format!("{}", result), large_num);
	}

	#[test]
	fn test_parse_int_scientific_notation() {
		let result = parse_int(Fragment::testing("1e5")).unwrap();
		assert_eq!(format!("{}", result), "100000");
	}

	#[test]
	fn test_parse_int_scientific_negative() {
		let result = parse_int(Fragment::testing("-1.5e3")).unwrap();
		assert_eq!(format!("{}", result), "-1500");
	}

	#[test]
	fn test_parse_int_float_truncation() {
		let result = parse_int(Fragment::testing("123.789")).unwrap();
		assert_eq!(format!("{}", result), "123");
	}

	#[test]
	fn test_parse_int_float_truncation_negative() {
		let result = parse_int(Fragment::testing("-123.789")).unwrap();
		assert_eq!(format!("{}", result), "-123");
	}

	#[test]
	fn test_parse_int_with_underscores() {
		let result = parse_int(Fragment::testing("1_234_567")).unwrap();
		assert_eq!(format!("{}", result), "1234567");
	}

	#[test]
	fn test_parse_int_with_leading_space() {
		let result = parse_int(Fragment::testing(" 12345")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_int_with_trailing_space() {
		let result = parse_int(Fragment::testing("12345 ")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_int_with_both_spaces() {
		let result = parse_int(Fragment::testing(" -12345 ")).unwrap();
		assert_eq!(format!("{}", result), "-12345");
	}

	#[test]
	fn test_parse_int_invalid_empty() {
		assert!(parse_int(Fragment::testing("")).is_err());
	}

	#[test]
	fn test_parse_int_invalid_whitespace() {
		assert!(parse_int(Fragment::testing("   ")).is_err());
	}

	#[test]
	fn test_parse_int_invalid_text() {
		assert!(parse_int(Fragment::testing("abc")).is_err());
	}

	#[test]
	fn test_parse_int_invalid_multiple_dots() {
		assert!(parse_int(Fragment::testing("1.2.3")).is_err());
	}

	#[test]
	fn test_parse_int_infinity() {
		assert!(parse_int(Fragment::testing("inf")).is_err());
	}

	#[test]
	fn test_parse_int_negative_infinity() {
		assert!(parse_int(Fragment::testing("-inf")).is_err());
	}

	#[test]
	fn parse_int_refuses_seventy_seven_digits_as_out_of_range() {
		// A 77 digit int does not fit any int column, so it must be a range error and not a wrap.
		assert_eq!(parse_int(Fragment::testing("9".repeat(77))).unwrap_err().code, "NUMBER_002");
		assert_eq!(parse_int(Fragment::testing("-1e76")).unwrap_err().code, "NUMBER_002");
		assert_eq!(parse_int(Fragment::testing("inf")).unwrap_err().code, "NUMBER_002");
		assert_eq!(parse_int(Fragment::testing("abc")).unwrap_err().code, "NUMBER_001");
		assert_eq!(parse_int(Fragment::testing("9".repeat(76))).unwrap(), Int::MAX);
		assert_eq!(
			parse_int(Fragment::testing("-99.9e74")).unwrap().to_string(),
			format!("-999{}", "0".repeat(73))
		);
	}
}
