// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use crate::value::value_type::ValueType;

pub fn value_max(value: ValueType) -> Cow<'static, str> {
	match value {
		ValueType::Int {
			precision,
		}
		| ValueType::Uint {
			precision,
		} => Cow::Owned(nines(precision.value(), 0)),
		ValueType::Decimal {
			precision,
			scale,
		} => Cow::Owned(nines(precision.value(), scale.value())),
		other => Cow::Borrowed(fixed_max(other)),
	}
}

pub fn value_range(value: ValueType) -> Cow<'static, str> {
	match value {
		ValueType::Int {
			precision,
		} => {
			let max = nines(precision.value(), 0);
			Cow::Owned(format!("-{max} to {max}"))
		}
		ValueType::Uint {
			precision,
		} => Cow::Owned(format!("0 to {}", nines(precision.value(), 0))),
		ValueType::Decimal {
			precision,
			scale,
		} => {
			let max = nines(precision.value(), scale.value());
			Cow::Owned(format!("-{max} to {max}"))
		}
		other => Cow::Borrowed(fixed_range(other)),
	}
}

fn nines(precision: u8, scale: u8) -> String {
	let integer_digits = precision.saturating_sub(scale) as usize;
	let integer = if integer_digits == 0 {
		"0".to_string()
	} else {
		let digits = "9".repeat(integer_digits);
		let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
		for (index, digit) in digits.chars().enumerate() {
			if index > 0 && (digits.len() - index).is_multiple_of(3) {
				grouped.push('_');
			}
			grouped.push(digit);
		}
		grouped
	};
	if scale == 0 {
		integer
	} else {
		format!("{integer}.{}", "9".repeat(scale as usize))
	}
}

fn fixed_max(value: ValueType) -> &'static str {
	match value {
		ValueType::Boolean => unreachable!(),
		ValueType::Float4 => "+3.4e38",
		ValueType::Float8 => "+1.8e308",
		ValueType::Int1 => "127",
		ValueType::Int2 => "32_767",
		ValueType::Int4 => "2_147_483_647",
		ValueType::Int8 => "9_223_372_036_854_775_807",
		ValueType::Int16 => "170_141_183_460_469_231_731_687_303_715_884_105_727",
		ValueType::Utf8 => unreachable!(),
		ValueType::Uint1 => "255",
		ValueType::Uint2 => "65_535",
		ValueType::Uint4 => "4_294_967_295",
		ValueType::Uint8 => "18_446_744_073_709_551_615",
		ValueType::Uint16 => "340_282_366_920_938_463_463_374_607_431_768_211_455",
		ValueType::Date => unreachable!(),
		ValueType::DateTime => unreachable!(),
		ValueType::Time => unreachable!(),
		ValueType::Duration => unreachable!(),
		ValueType::IdentityId => unreachable!(),
		ValueType::Uuid4 => unreachable!(),
		ValueType::Uuid7 => unreachable!(),
		ValueType::Blob => unreachable!(),
		ValueType::Int {
			..
		}
		| ValueType::Uint {
			..
		}
		| ValueType::Decimal {
			..
		} => unreachable!(),
		ValueType::Option(_) => unreachable!(),
		ValueType::Any => unreachable!(),
		ValueType::DictionaryId => unreachable!(),
		ValueType::List(_) => unreachable!(),
		ValueType::Record(_) => unreachable!(),
		ValueType::Tuple(_) => unreachable!(),
		ValueType::Digest {
			..
		} => unreachable!(),
	}
}

fn fixed_range(value: ValueType) -> &'static str {
	match value {
		ValueType::Boolean => unreachable!(),
		ValueType::Float4 => "-3.4e38 to +3.4e38",
		ValueType::Float8 => "-1.8e308 to +1.8e308",
		ValueType::Int1 => "-128 to 127",
		ValueType::Int2 => "-32_768 to 32_767",
		ValueType::Int4 => "-2_147_483_648 to 2_147_483_647",
		ValueType::Int8 => "-9_223_372_036_854_775_808 to 9_223_372_036_854_775_807",
		ValueType::Int16 => {
			"-170_141_183_460_469_231_731_687_303_715_884_105_728 to 170_141_183_460_469_231_731_687_303_715_884_105_727"
		}
		ValueType::Utf8 => unreachable!(),
		ValueType::Uint1 => "0 to 255",
		ValueType::Uint2 => "0 to 65_535",
		ValueType::Uint4 => "0 to 4_294_967_295",
		ValueType::Uint8 => "0 to 18_446_744_073_709_551_615",
		ValueType::Uint16 => "0 to 340_282_366_920_938_463_463_374_607_431_768_211_455",
		ValueType::Date => unreachable!(),
		ValueType::DateTime => unreachable!(),
		ValueType::Time => unreachable!(),
		ValueType::Duration => unreachable!(),
		ValueType::IdentityId => unreachable!(),
		ValueType::Uuid4 => unreachable!(),
		ValueType::Uuid7 => unreachable!(),
		ValueType::Blob => unreachable!(),
		ValueType::Int {
			..
		}
		| ValueType::Uint {
			..
		}
		| ValueType::Decimal {
			..
		} => unreachable!(),
		ValueType::Option(_) => unreachable!(),
		ValueType::Any => unreachable!(),
		ValueType::DictionaryId => unreachable!(),
		ValueType::List(_) => unreachable!(),
		ValueType::Record(_) => unreachable!(),
		ValueType::Tuple(_) => unreachable!(),
		ValueType::Digest {
			..
		} => unreachable!(),
	}
}

#[cfg(test)]
pub mod tests {

	mod value_max {
		use crate::{error::util::value_max, value::value_type::ValueType};

		#[test]
		fn test_signed_ints() {
			assert_eq!(value_max(ValueType::Int1), "127");
			assert_eq!(value_max(ValueType::Int2), "32_767");
			assert_eq!(value_max(ValueType::Int4), "2_147_483_647");
			assert_eq!(value_max(ValueType::Int8), "9_223_372_036_854_775_807");
			assert_eq!(value_max(ValueType::Int16), "170_141_183_460_469_231_731_687_303_715_884_105_727");
		}

		#[test]
		fn test_unsigned_ints() {
			assert_eq!(value_max(ValueType::Uint1), "255");
			assert_eq!(value_max(ValueType::Uint2), "65_535");
			assert_eq!(value_max(ValueType::Uint4), "4_294_967_295");
			assert_eq!(value_max(ValueType::Uint8), "18_446_744_073_709_551_615");
			assert_eq!(value_max(ValueType::Uint16), "340_282_366_920_938_463_463_374_607_431_768_211_455");
		}

		#[test]
		fn test_floats() {
			assert_eq!(value_max(ValueType::Float4), "+3.4e38");
			assert_eq!(value_max(ValueType::Float8), "+1.8e308");
		}
	}

	mod value_range {
		use crate::{error::util::value_range, value::value_type::ValueType};

		#[test]
		fn test_signed_ints() {
			assert_eq!(value_range(ValueType::Int1), "-128 to 127");
			assert_eq!(value_range(ValueType::Int2), "-32_768 to 32_767");
			assert_eq!(value_range(ValueType::Int4), "-2_147_483_648 to 2_147_483_647");
			assert_eq!(
				value_range(ValueType::Int8),
				"-9_223_372_036_854_775_808 to 9_223_372_036_854_775_807"
			);
			assert_eq!(
				value_range(ValueType::Int16),
				"-170_141_183_460_469_231_731_687_303_715_884_105_728 to 170_141_183_460_469_231_731_687_303_715_884_105_727"
			);
		}

		#[test]
		fn test_unsigned_ints() {
			assert_eq!(value_range(ValueType::Uint1), "0 to 255");
			assert_eq!(value_range(ValueType::Uint2), "0 to 65_535");
			assert_eq!(value_range(ValueType::Uint4), "0 to 4_294_967_295");
			assert_eq!(value_range(ValueType::Uint8), "0 to 18_446_744_073_709_551_615");
			assert_eq!(
				value_range(ValueType::Uint16),
				"0 to 340_282_366_920_938_463_463_374_607_431_768_211_455"
			);
		}

		#[test]
		fn test_floats() {
			assert_eq!(value_range(ValueType::Float4), "-3.4e38 to +3.4e38");
			assert_eq!(value_range(ValueType::Float8), "-1.8e308 to +1.8e308");
		}
	}

	mod parameterized {
		use crate::{
			error::util::{value_max, value_range},
			value::{
				constraint::{precision::Precision, scale::Scale},
				value_type::ValueType,
			},
		};

		#[test]
		fn range_follows_the_declared_precision_and_scale() {
			// The error text must name the bound the column enforces, not the widest one.
			let decimal = ValueType::decimal(Precision::new(10), Scale::new(2));
			assert_eq!(value_max(decimal.clone()), "99_999_999.99");
			assert_eq!(value_range(decimal), "-99_999_999.99 to 99_999_999.99");
			assert_eq!(value_range(ValueType::int(Precision::new(4))), "-9_999 to 9_999");
			assert_eq!(value_range(ValueType::uint(Precision::new(3))), "0 to 999");
			assert_eq!(value_max(ValueType::decimal(Precision::new(2), Scale::new(2))), "0.99");
			assert_eq!(value_max(ValueType::INT).matches('9').count(), 76);
		}
	}
}
