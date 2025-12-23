// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{
	Error, err,
	error::diagnostic::boolean::{empty_boolean_value, invalid_boolean_format, invalid_number_boolean},
	fragment::Fragment,
	return_error,
};

pub fn parse_bool(fragment: Fragment) -> Result<bool, Error> {
	// Fragment is already owned, no conversion needed
	let value = fragment.text().trim();

	if value.is_empty() {
		return_error!(empty_boolean_value(fragment));
	}

	// Fast path: byte-level matching for common cases
	match value.as_bytes() {
		b"true" | b"TRUE" | b"True" => return Ok(true),
		b"false" | b"FALSE" | b"False" => return Ok(false),
		b"1" | b"1.0" => return Ok(true),
		b"0" | b"0.0" => return Ok(false),
		_ => {}
	}

	// Slow path: case-insensitive matching for mixed case
	match value.len() {
		4 if value.eq_ignore_ascii_case("true") => Ok(true),
		5 if value.eq_ignore_ascii_case("false") => Ok(false),
		3 if value == "1.0" => Ok(true),
		3 if value == "0.0" => Ok(false),
		_ => {
			// Check if the value contains numbers - if so, use
			// numeric boolean diagnostic
			if value.as_bytes().iter().any(|&b| b.is_ascii_digit()) {
				err!(invalid_number_boolean(fragment))
			} else {
				err!(invalid_boolean_format(fragment))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Fragment;

	#[test]
	fn test_valid_true() {
		assert_eq!(parse_bool(Fragment::testing("true")), Ok(true));
	}

	#[test]
	fn test_valid_false() {
		assert_eq!(parse_bool(Fragment::testing("false")), Ok(false));
	}

	#[test]
	fn test_valid_true_with_spaces() {
		assert_eq!(parse_bool(Fragment::testing("  true  ")), Ok(true));
	}

	#[test]
	fn test_valid_false_with_spaces() {
		assert_eq!(parse_bool(Fragment::testing("  false  ")), Ok(false));
	}

	#[test]
	fn test_case_mismatch_true() {
		assert_eq!(parse_bool(Fragment::testing("True")), Ok(true));
		assert_eq!(parse_bool(Fragment::testing("TRUE")), Ok(true));
		assert_eq!(parse_bool(Fragment::testing("tRuE")), Ok(true));
	}

	#[test]
	fn test_case_mismatch_false() {
		assert_eq!(parse_bool(Fragment::testing("False")), Ok(false));
		assert_eq!(parse_bool(Fragment::testing("FALSE")), Ok(false));
		assert_eq!(parse_bool(Fragment::testing("fAlSe")), Ok(false));
	}

	#[test]
	fn test_valid_numeric_boolean() {
		assert_eq!(parse_bool(Fragment::testing("1")), Ok(true));
		assert_eq!(parse_bool(Fragment::testing("0")), Ok(false));
		assert_eq!(parse_bool(Fragment::testing("1.0")), Ok(true));
		assert_eq!(parse_bool(Fragment::testing("0.0")), Ok(false));
	}

	#[test]
	fn test_invalid_numeric_boolean() {
		assert!(parse_bool(Fragment::testing("2")).is_err());
		assert!(parse_bool(Fragment::testing("1.5")).is_err());
		assert!(parse_bool(Fragment::testing("0.5")).is_err());
		assert!(parse_bool(Fragment::testing("-1")).is_err());
		assert!(parse_bool(Fragment::testing("100")).is_err());
	}

	#[test]
	fn test_empty_boolean_value() {
		assert!(parse_bool(Fragment::testing("")).is_err());
		assert!(parse_bool(Fragment::testing("   ")).is_err());
	}

	#[test]
	fn test_ambiguous_boolean_value() {
		assert!(parse_bool(Fragment::testing("yes")).is_err());
		assert!(parse_bool(Fragment::testing("no")).is_err());
		assert!(parse_bool(Fragment::testing("y")).is_err());
		assert!(parse_bool(Fragment::testing("n")).is_err());
		assert!(parse_bool(Fragment::testing("on")).is_err());
		assert!(parse_bool(Fragment::testing("off")).is_err());
		assert!(parse_bool(Fragment::testing("t")).is_err());
		assert!(parse_bool(Fragment::testing("f")).is_err());
	}

	#[test]
	fn test_invalid_boolean_format() {
		assert!(parse_bool(Fragment::testing("invalid")).is_err());
		assert!(parse_bool(Fragment::testing("123")).is_err());
		assert!(parse_bool(Fragment::testing("abc")).is_err());
		assert!(parse_bool(Fragment::testing("maybe")).is_err());
	}

	#[test]
	fn test_case_insensitive_ambiguous() {
		assert!(parse_bool(Fragment::testing("Yes")).is_err());
		assert!(parse_bool(Fragment::testing("NO")).is_err());
		assert!(parse_bool(Fragment::testing("On")).is_err());
		assert!(parse_bool(Fragment::testing("OFF")).is_err());
	}
}
