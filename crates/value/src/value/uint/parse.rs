// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use crate::{
	error::{Error, TypeError},
	fragment::Fragment,
	value::{
		decimal::unscaled::ParseError,
		uint::{Uint, parse_error},
		value_type::ValueType,
	},
};

pub fn parse_uint(fragment: Fragment) -> Result<Uint, Error> {
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
			target: ValueType::UINT,
			fragment,
		}
		.into());
	}

	match Uint::parse(&value) {
		Ok(uint) => Ok(uint),
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
	fn test_parse_uint_valid_zero() {
		assert_eq!(parse_uint(Fragment::testing("0")).unwrap(), Uint::zero());
	}

	#[test]
	fn test_parse_uint_valid_positive() {
		let result = parse_uint(Fragment::testing("12345")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_uint_large_positive() {
		let large_num = "123456789012345678901234567890";
		let result = parse_uint(Fragment::testing(large_num)).unwrap();
		assert_eq!(format!("{}", result), large_num);
	}

	#[test]
	fn test_parse_uint_scientific_notation() {
		let result = parse_uint(Fragment::testing("1e5")).unwrap();
		assert_eq!(format!("{}", result), "100000");
	}

	#[test]
	fn test_parse_uint_scientific_decimal() {
		let result = parse_uint(Fragment::testing("2.5e3")).unwrap();
		assert_eq!(format!("{}", result), "2500");
	}

	#[test]
	fn test_parse_uint_float_truncation() {
		let result = parse_uint(Fragment::testing("123.789")).unwrap();
		assert_eq!(format!("{}", result), "123");
	}

	#[test]
	fn test_parse_uint_float_truncation_zero() {
		let result = parse_uint(Fragment::testing("0.999")).unwrap();
		assert_eq!(format!("{}", result), "0");
	}

	#[test]
	fn test_parse_uint_with_underscores() {
		let result = parse_uint(Fragment::testing("1_234_567")).unwrap();
		assert_eq!(format!("{}", result), "1234567");
	}

	#[test]
	fn test_parse_uint_with_leading_space() {
		let result = parse_uint(Fragment::testing(" 12345")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_uint_with_trailing_space() {
		let result = parse_uint(Fragment::testing("12345 ")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_uint_with_both_spaces() {
		let result = parse_uint(Fragment::testing(" 12345 ")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_uint_negative_integer() {
		assert!(parse_uint(Fragment::testing("-12345")).is_err());
	}

	#[test]
	fn test_parse_uint_negative_float() {
		assert!(parse_uint(Fragment::testing("-123.45")).is_err());
	}

	#[test]
	fn test_parse_uint_negative_scientific() {
		assert!(parse_uint(Fragment::testing("-1e5")).is_err());
	}

	#[test]
	fn test_parse_uint_negative_zero_float() {
		// "-0" has no negative magnitude to reject, so it parses as zero rather than erroring.
		let result = parse_uint(Fragment::testing("-0.0")).unwrap();
		assert_eq!(format!("{}", result), "0");
	}

	#[test]
	fn test_parse_uint_invalid_empty() {
		assert!(parse_uint(Fragment::testing("")).is_err());
	}

	#[test]
	fn test_parse_uint_invalid_whitespace() {
		assert!(parse_uint(Fragment::testing("   ")).is_err());
	}

	#[test]
	fn test_parse_uint_invalid_text() {
		assert!(parse_uint(Fragment::testing("abc")).is_err());
	}

	#[test]
	fn test_parse_uint_invalid_multiple_dots() {
		assert!(parse_uint(Fragment::testing("1.2.3")).is_err());
	}

	#[test]
	fn test_parse_uint_infinity() {
		assert!(parse_uint(Fragment::testing("inf")).is_err());
	}

	#[test]
	fn test_parse_uint_negative_infinity() {
		assert!(parse_uint(Fragment::testing("-inf")).is_err());
	}

	#[test]
	fn parse_uint_refuses_negatives_and_seventy_seven_digits_as_out_of_range() {
		// Both are values a uint column cannot hold, so both must be range errors and never wrap.
		assert_eq!(parse_uint(Fragment::testing("-1")).unwrap_err().code, "NUMBER_002");
		assert_eq!(parse_uint(Fragment::testing("-1.5e3")).unwrap_err().code, "NUMBER_002");
		assert_eq!(parse_uint(Fragment::testing("9".repeat(77))).unwrap_err().code, "NUMBER_002");
		assert_eq!(parse_uint(Fragment::testing("-inf")).unwrap_err().code, "NUMBER_002");
		assert_eq!(parse_uint(Fragment::testing("abc")).unwrap_err().code, "NUMBER_001");
		assert_eq!(parse_uint(Fragment::testing("-0.9")).unwrap(), Uint::zero());
		assert_eq!(parse_uint(Fragment::testing("9".repeat(76))).unwrap(), Uint::MAX);
	}
}
