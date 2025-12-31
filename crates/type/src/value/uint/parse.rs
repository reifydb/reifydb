// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::borrow::Cow;

use num_bigint::BigInt;

use crate::{
	Error, Fragment, Type, err,
	error::diagnostic::number::{invalid_number_format, number_out_of_range},
	return_error,
	value::uint::Uint,
};

pub fn parse_uint(fragment: Fragment) -> Result<Uint, Error> {
	// Fragment is already owned, no conversion needed
	let raw_value = fragment.text();

	// Fast path: check if we need any string processing
	let needs_trimming = raw_value.as_bytes().first().map_or(false, |&b| b.is_ascii_whitespace())
		|| raw_value.as_bytes().last().map_or(false, |&b| b.is_ascii_whitespace());

	let has_underscores = raw_value.as_bytes().contains(&b'_');

	let value = match (needs_trimming, has_underscores) {
		(false, false) => Cow::Borrowed(raw_value), // Fast path -
		// no processing
		// needed
		(true, false) => Cow::Borrowed(raw_value.trim()),
		(false, true) => Cow::Owned(raw_value.replace('_', "")),
		(true, true) => Cow::Owned(raw_value.trim().replace('_', "")),
	};

	if value.is_empty() {
		return_error!(invalid_number_format(fragment, Type::Uint));
	}

	// Check for negative sign early, but allow -0.0 case to be handled by
	// float parsing
	if value.starts_with('-') && value != "-0.0" && value != "-0" {
		// Quick check for other obvious negative values
		if let Ok(bigint) = value.parse::<BigInt>() {
			if bigint.sign() == num_bigint::Sign::Minus {
				return_error!(number_out_of_range(fragment, Type::Uint, None));
			}
		}
		// For non-BigInt parseable values, let float parsing handle it
	}

	// Try parsing as BigInt first
	match value.parse::<BigInt>() {
		Ok(v) => {
			// Double check that the BigInt is non-negative (should
			// be guaranteed by the prefix check)
			if v.sign() == num_bigint::Sign::Minus {
				return_error!(number_out_of_range(fragment, Type::Uint, None));
			}
			Ok(Uint::from(v))
		}
		Err(_) => {
			// If BigInt parsing fails, try parsing as f64 for
			// scientific notation and truncation
			if let Ok(f) = value.parse::<f64>() {
				if f.is_infinite() {
					err!(number_out_of_range(fragment, Type::Uint, None))
				} else {
					let truncated = f.trunc();
					// Handle negative zero and other
					// negative values
					if truncated < 0.0 && truncated != -0.0 {
						return_error!(number_out_of_range(fragment, Type::Uint, None));
					}
					// Convert the truncated float to
					// BigInt, treating -0.0 as 0.0
					let abs_truncated = if truncated == -0.0 {
						0.0
					} else {
						truncated
					};
					if let Ok(bigint) = format!("{:.0}", abs_truncated).parse::<BigInt>() {
						Ok(Uint::from(bigint))
					} else {
						err!(invalid_number_format(fragment, Type::Uint))
					}
				}
			} else {
				// Check if it contains a minus sign to provide
				// better error message
				if value.contains('-') {
					err!(number_out_of_range(fragment, Type::Uint, None))
				} else {
					err!(invalid_number_format(fragment, Type::Uint))
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Fragment;

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
		// This should be handled gracefully - negative zero should
		// become positive zero
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
}
