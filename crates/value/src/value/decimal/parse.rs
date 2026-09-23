// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use crate::{
	error::{Error, TypeError},
	fragment::Fragment,
	value::{
		decimal::{Decimal, parse_error},
		value_type::ValueType,
	},
};

pub fn parse_decimal(fragment: Fragment) -> Result<Decimal, Error> {
	let fragment_owned = fragment.clone();
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
			target: ValueType::DECIMAL,
			fragment: fragment_owned,
		}
		.into());
	}

	Decimal::parse(&value).map_err(|error| parse_error(error, fragment_owned))
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_parse_decimal_integer() {
		let decimal = parse_decimal(Fragment::testing("123")).unwrap();
		assert_eq!(decimal.to_string(), "123");
	}

	#[test]
	fn test_parse_decimal_with_fractional() {
		let decimal = parse_decimal(Fragment::testing("123.45")).unwrap();
		assert_eq!(decimal.to_string(), "123.45");
	}

	#[test]
	fn test_parse_decimal_with_underscores() {
		let decimal = parse_decimal(Fragment::testing("1_234.56")).unwrap();
		assert_eq!(decimal.to_string(), "1234.56");
	}

	#[test]
	fn test_parse_decimal_negative() {
		let decimal = parse_decimal(Fragment::testing("-123.45")).unwrap();
		assert_eq!(decimal.to_string(), "-123.45");
	}

	#[test]
	fn test_parse_decimal_empty() {
		assert!(parse_decimal(Fragment::testing("")).is_err());
	}

	#[test]
	fn test_parse_decimal_invalid() {
		assert!(parse_decimal(Fragment::testing("not_a_number")).is_err());
	}

	#[test]
	fn test_parse_decimal_scientific_notation() {
		let decimal = parse_decimal(Fragment::testing("1.23e2")).unwrap();
		assert_eq!(decimal.to_string(), "123");
	}

	#[test]
	fn parse_keeps_the_literal_scale_and_rejects_seventy_seven_digits() {
		// The literal scale is what the column type derives from, and 77 digits cannot be stored.
		let decimal = parse_decimal(Fragment::testing(" 1_000.50 ")).unwrap();
		assert_eq!((decimal.to_string(), decimal.scale()), ("1000.50".to_string(), 2));
		let error = parse_decimal(Fragment::testing("9".repeat(77))).unwrap_err();
		assert_eq!(error.code, "NUMBER_002");
	}
}
