// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::borrow::Cow;

use num_bigint::BigInt;

use crate::{
	Error, IntoFragment, Type, err,
	error::diagnostic::number::{
		invalid_number_format, number_out_of_range,
	},
	return_error,
	value::varint::VarInt,
};

pub fn parse_varint<'a>(
	fragment: impl IntoFragment<'a>,
) -> Result<VarInt, Error> {
	let fragment = fragment.into_fragment();
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
		(false, false) => Cow::Borrowed(raw_value), /* Fast path -
		                                              * no processing
		                                              * needed */
		(true, false) => Cow::Borrowed(raw_value.trim()),
		(false, true) => Cow::Owned(raw_value.replace('_', "")),
		(true, true) => Cow::Owned(raw_value.trim().replace('_', "")),
	};

	if value.is_empty() {
		return_error!(invalid_number_format(fragment, Type::VarInt));
	}

	// Try parsing as BigInt first
	match value.parse::<BigInt>() {
		Ok(v) => Ok(VarInt::from(v)),
		Err(_) => {
			// If BigInt parsing fails, try parsing as f64 for
			// scientific notation and truncation
			if let Ok(f) = value.parse::<f64>() {
				if f.is_infinite() {
					err!(number_out_of_range(
						fragment,
						Type::VarInt,
						None
					))
				} else {
					let truncated = f.trunc();
					// Convert the truncated float to BigInt
					if let Ok(bigint) =
						format!("{:.0}", truncated)
							.parse::<BigInt>()
					{
						Ok(VarInt::from(bigint))
					} else {
						err!(invalid_number_format(
							fragment,
							Type::VarInt
						))
					}
				}
			} else {
				err!(invalid_number_format(
					fragment,
					Type::VarInt
				))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::OwnedFragment;

	#[test]
	fn test_parse_varint_valid_zero() {
		assert_eq!(
			parse_varint(OwnedFragment::testing("0")).unwrap(),
			VarInt::zero()
		);
	}

	#[test]
	fn test_parse_varint_valid_positive() {
		let result =
			parse_varint(OwnedFragment::testing("12345")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_varint_valid_negative() {
		let result =
			parse_varint(OwnedFragment::testing("-12345")).unwrap();
		assert_eq!(format!("{}", result), "-12345");
	}

	#[test]
	fn test_parse_varint_large_positive() {
		let large_num = "123456789012345678901234567890";
		let result = parse_varint(OwnedFragment::testing(large_num))
			.unwrap();
		assert_eq!(format!("{}", result), large_num);
	}

	#[test]
	fn test_parse_varint_large_negative() {
		let large_num = "-123456789012345678901234567890";
		let result = parse_varint(OwnedFragment::testing(large_num))
			.unwrap();
		assert_eq!(format!("{}", result), large_num);
	}

	#[test]
	fn test_parse_varint_scientific_notation() {
		let result =
			parse_varint(OwnedFragment::testing("1e5")).unwrap();
		assert_eq!(format!("{}", result), "100000");
	}

	#[test]
	fn test_parse_varint_scientific_negative() {
		let result =
			parse_varint(OwnedFragment::testing("-1.5e3")).unwrap();
		assert_eq!(format!("{}", result), "-1500");
	}

	#[test]
	fn test_parse_varint_float_truncation() {
		let result = parse_varint(OwnedFragment::testing("123.789"))
			.unwrap();
		assert_eq!(format!("{}", result), "123");
	}

	#[test]
	fn test_parse_varint_float_truncation_negative() {
		let result = parse_varint(OwnedFragment::testing("-123.789"))
			.unwrap();
		assert_eq!(format!("{}", result), "-123");
	}

	#[test]
	fn test_parse_varint_with_underscores() {
		let result = parse_varint(OwnedFragment::testing("1_234_567"))
			.unwrap();
		assert_eq!(format!("{}", result), "1234567");
	}

	#[test]
	fn test_parse_varint_with_leading_space() {
		let result =
			parse_varint(OwnedFragment::testing(" 12345")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_varint_with_trailing_space() {
		let result =
			parse_varint(OwnedFragment::testing("12345 ")).unwrap();
		assert_eq!(format!("{}", result), "12345");
	}

	#[test]
	fn test_parse_varint_with_both_spaces() {
		let result = parse_varint(OwnedFragment::testing(" -12345 "))
			.unwrap();
		assert_eq!(format!("{}", result), "-12345");
	}

	#[test]
	fn test_parse_varint_invalid_empty() {
		assert!(parse_varint(OwnedFragment::testing("")).is_err());
	}

	#[test]
	fn test_parse_varint_invalid_whitespace() {
		assert!(parse_varint(OwnedFragment::testing("   ")).is_err());
	}

	#[test]
	fn test_parse_varint_invalid_text() {
		assert!(parse_varint(OwnedFragment::testing("abc")).is_err());
	}

	#[test]
	fn test_parse_varint_invalid_multiple_dots() {
		assert!(parse_varint(OwnedFragment::testing("1.2.3")).is_err());
	}

	#[test]
	fn test_parse_varint_infinity() {
		assert!(parse_varint(OwnedFragment::testing("inf")).is_err());
	}

	#[test]
	fn test_parse_varint_negative_infinity() {
		assert!(parse_varint(OwnedFragment::testing("-inf")).is_err());
	}
}
