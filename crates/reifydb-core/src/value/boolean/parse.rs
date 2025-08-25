// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Error, err,
	interface::fragment::IntoFragment,
	result::error::diagnostic::boolean::{
		empty_boolean_value, invalid_boolean_format,
		invalid_number_boolean,
	},
	return_error,
};

pub fn parse_bool<'a>(fragment: impl IntoFragment<'a>) -> Result<bool, Error> {
	let owned_fragment = fragment.into_fragment().into_owned();
	let value = owned_fragment.value().trim();

	if value.is_empty() {
		return_error!(empty_boolean_value(owned_fragment.clone()));
	}

	match value.to_lowercase().as_str() {
		"true" => Ok(true),
		"false" => Ok(false),
		"1" | "1.0" => Ok(true),
		"0" | "0.0" => Ok(false),
		_ => {
			// Check if the value contains numbers - if so, use
			// numeric boolean diagnostic
			if value.chars().any(|c| c.is_ascii_digit()) {
				err!(invalid_number_boolean(owned_fragment))
			} else {
				err!(invalid_boolean_format(owned_fragment))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::interface::fragment::OwnedFragment;

	#[test]
	fn test_valid_true() {
		assert_eq!(
			parse_bool(OwnedFragment::testing("true")),
			Ok(true)
		);
	}

	#[test]
	fn test_valid_false() {
		assert_eq!(
			parse_bool(OwnedFragment::testing("false")),
			Ok(false)
		);
	}

	#[test]
	fn test_valid_true_with_spaces() {
		assert_eq!(
			parse_bool(OwnedFragment::testing("  true  ")),
			Ok(true)
		);
	}

	#[test]
	fn test_valid_false_with_spaces() {
		assert_eq!(
			parse_bool(OwnedFragment::testing("  false  ")),
			Ok(false)
		);
	}

	#[test]
	fn test_case_mismatch_true() {
		assert_eq!(
			parse_bool(OwnedFragment::testing("True")),
			Ok(true)
		);
		assert_eq!(
			parse_bool(OwnedFragment::testing("TRUE")),
			Ok(true)
		);
		assert_eq!(
			parse_bool(OwnedFragment::testing("tRuE")),
			Ok(true)
		);
	}

	#[test]
	fn test_case_mismatch_false() {
		assert_eq!(
			parse_bool(OwnedFragment::testing("False")),
			Ok(false)
		);
		assert_eq!(
			parse_bool(OwnedFragment::testing("FALSE")),
			Ok(false)
		);
		assert_eq!(
			parse_bool(OwnedFragment::testing("fAlSe")),
			Ok(false)
		);
	}

	#[test]
	fn test_valid_numeric_boolean() {
		assert_eq!(parse_bool(OwnedFragment::testing("1")), Ok(true));
		assert_eq!(parse_bool(OwnedFragment::testing("0")), Ok(false));
		assert_eq!(parse_bool(OwnedFragment::testing("1.0")), Ok(true));
		assert_eq!(
			parse_bool(OwnedFragment::testing("0.0")),
			Ok(false)
		);
	}

	#[test]
	fn test_invalid_numeric_boolean() {
		assert!(parse_bool(OwnedFragment::testing("2")).is_err());
		assert!(parse_bool(OwnedFragment::testing("1.5")).is_err());
		assert!(parse_bool(OwnedFragment::testing("0.5")).is_err());
		assert!(parse_bool(OwnedFragment::testing("-1")).is_err());
		assert!(parse_bool(OwnedFragment::testing("100")).is_err());
	}

	#[test]
	fn test_empty_boolean_value() {
		assert!(parse_bool(OwnedFragment::testing("")).is_err());
		assert!(parse_bool(OwnedFragment::testing("   ")).is_err());
	}

	#[test]
	fn test_ambiguous_boolean_value() {
		assert!(parse_bool(OwnedFragment::testing("yes")).is_err());
		assert!(parse_bool(OwnedFragment::testing("no")).is_err());
		assert!(parse_bool(OwnedFragment::testing("y")).is_err());
		assert!(parse_bool(OwnedFragment::testing("n")).is_err());
		assert!(parse_bool(OwnedFragment::testing("on")).is_err());
		assert!(parse_bool(OwnedFragment::testing("off")).is_err());
		assert!(parse_bool(OwnedFragment::testing("t")).is_err());
		assert!(parse_bool(OwnedFragment::testing("f")).is_err());
	}

	#[test]
	fn test_invalid_boolean_format() {
		assert!(parse_bool(OwnedFragment::testing("invalid")).is_err());
		assert!(parse_bool(OwnedFragment::testing("123")).is_err());
		assert!(parse_bool(OwnedFragment::testing("abc")).is_err());
		assert!(parse_bool(OwnedFragment::testing("maybe")).is_err());
	}

	#[test]
	fn test_case_insensitive_ambiguous() {
		assert!(parse_bool(OwnedFragment::testing("Yes")).is_err());
		assert!(parse_bool(OwnedFragment::testing("NO")).is_err());
		assert!(parse_bool(OwnedFragment::testing("On")).is_err());
		assert!(parse_bool(OwnedFragment::testing("OFF")).is_err());
	}
}
