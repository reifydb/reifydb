// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{borrow::Cow, str::FromStr};

use bigdecimal::BigDecimal as BigDecimalInner;

use crate::{
	Error, IntoFragment, Type,
	error::diagnostic::number::invalid_number_format,
	return_error,
	value::decimal::{Decimal, Precision},
};

pub fn parse_decimal<'a>(
	fragment: impl IntoFragment<'a>,
) -> Result<Decimal, Error> {
	let fragment = fragment.into_fragment();
	let fragment_owned = fragment.clone().into_owned();
	let raw_value = fragment.text();

	// Fast path: check if we need any string processing
	let needs_trimming = raw_value
		.as_bytes()
		.first()
		.map_or(false, |&b| b.is_ascii_whitespace())
		|| raw_value
			.as_bytes()
			.last()
			.map_or(false, |&b| b.is_ascii_whitespace());
	let has_underscores = raw_value.as_bytes().contains(&b'_');

	let value = match (needs_trimming, has_underscores) {
		(false, false) => Cow::Borrowed(raw_value),
		(true, false) => Cow::Borrowed(raw_value.trim()),
		(false, true) => Cow::Owned(raw_value.replace('_', "")),
		(true, true) => Cow::Owned(raw_value.trim().replace('_', "")),
	};

	if value.is_empty() {
		return_error!(invalid_number_format(
			fragment_owned.clone(),
			Type::Decimal
		));
	}

	let big_decimal = BigDecimalInner::from_str(&value).map_err(|_| {
		crate::error!(invalid_number_format(
			fragment_owned.clone(),
			Type::Decimal
		))
	})?;

	let scale_u8 = determine_scale_from_string(&value);
	let precision_u8 = calculate_min_precision(&big_decimal, scale_u8);

	let precision = Precision::new(precision_u8);
	let scale = crate::value::decimal::Scale::new(scale_u8);

	Decimal::new(big_decimal, precision, scale).map_err(|_| {
		crate::error!(invalid_number_format(
			fragment_owned,
			Type::Decimal
		))
	})
}

fn determine_scale_from_string(s: &str) -> u8 {
	if let Some(dot_pos) = s.find('.') {
		let after_dot = &s[dot_pos + 1..];
		// Remove any scientific notation part
		let scale_part = if let Some(e_pos) = after_dot.find(['e', 'E'])
		{
			&after_dot[..e_pos]
		} else {
			after_dot
		};
		scale_part.len().min(38) as u8
	} else {
		0
	}
}

fn calculate_min_precision(value: &BigDecimalInner, min_scale: u8) -> u8 {
	let str_repr = value.to_string();
	let digits: Vec<char> =
		str_repr.chars().filter(|c| c.is_ascii_digit()).collect();
	let digit_count = digits.len() as u8;

	std::cmp::max(digit_count, min_scale).min(38)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::OwnedFragment;

	#[test]
	fn test_parse_decimal_integer() {
		let decimal =
			parse_decimal(OwnedFragment::testing("123")).unwrap();
		assert_eq!(decimal.to_string(), "123");
		assert_eq!(decimal.scale().value(), 0);
	}

	#[test]
	fn test_parse_decimal_with_fractional() {
		let decimal = parse_decimal(OwnedFragment::testing("123.45"))
			.unwrap();
		assert_eq!(decimal.to_string(), "123.45");
		assert_eq!(decimal.scale().value(), 2);
	}

	#[test]
	fn test_parse_decimal_with_underscores() {
		let decimal = parse_decimal(OwnedFragment::testing("1_234.56"))
			.unwrap();
		assert_eq!(decimal.to_string(), "1234.56");
		assert_eq!(decimal.scale().value(), 2);
	}

	#[test]
	fn test_parse_decimal_negative() {
		let decimal = parse_decimal(OwnedFragment::testing("-123.45"))
			.unwrap();
		assert_eq!(decimal.to_string(), "-123.45");
		assert_eq!(decimal.scale().value(), 2);
	}

	#[test]
	fn test_parse_decimal_empty() {
		assert!(parse_decimal(OwnedFragment::testing("")).is_err());
	}

	#[test]
	fn test_parse_decimal_invalid() {
		assert!(parse_decimal(OwnedFragment::testing("not_a_number"))
			.is_err());
	}

	#[test]
	fn test_parse_decimal_scientific_notation() {
		let decimal = parse_decimal(OwnedFragment::testing("1.23e2"))
			.unwrap();
		assert_eq!(decimal.to_string(), "123");
	}
}
