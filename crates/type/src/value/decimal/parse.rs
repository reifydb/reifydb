// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{borrow::Cow, str::FromStr};

use bigdecimal::BigDecimal as BigDecimalInner;

use crate::{
	Error, Fragment, Type, error::diagnostic::number::invalid_number_format, return_error, value::decimal::Decimal,
};

pub fn parse_decimal(fragment: Fragment) -> Result<Decimal, Error> {
	// Fragment is already owned, no conversion needed
	let fragment_owned = fragment.clone();
	let raw_value = fragment.text();

	// Fast path: check if we need any string processing
	let needs_trimming = raw_value.as_bytes().first().map_or(false, |&b| b.is_ascii_whitespace())
		|| raw_value.as_bytes().last().map_or(false, |&b| b.is_ascii_whitespace());
	let has_underscores = raw_value.as_bytes().contains(&b'_');

	let value = match (needs_trimming, has_underscores) {
		(false, false) => Cow::Borrowed(raw_value),
		(true, false) => Cow::Borrowed(raw_value.trim()),
		(false, true) => Cow::Owned(raw_value.replace('_', "")),
		(true, true) => Cow::Owned(raw_value.trim().replace('_', "")),
	};

	if value.is_empty() {
		return_error!(invalid_number_format(fragment_owned.clone(), Type::Decimal));
	}

	let big_decimal = BigDecimalInner::from_str(&value)
		.map_err(|_| crate::error!(invalid_number_format(fragment_owned, Type::Decimal)))?;

	Ok(Decimal::new(big_decimal))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Fragment;

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
}
