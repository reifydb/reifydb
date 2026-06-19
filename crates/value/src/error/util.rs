// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::value::value_type::ValueType;

pub fn value_max<'a>(value: ValueType) -> &'a str {
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
		ValueType::Int => "unlimited",
		ValueType::Uint => "unlimited",
		ValueType::Decimal => "unlimited",
		ValueType::Option(_) => unreachable!(),
		ValueType::Any => unreachable!(),
		ValueType::DictionaryId => unreachable!(),
		ValueType::List(_) => unreachable!(),
		ValueType::Record(_) => unreachable!(),
		ValueType::Tuple(_) => unreachable!(),
	}
}

pub fn value_range<'a>(value: ValueType) -> &'a str {
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
		ValueType::Int => "unlimited",
		ValueType::Uint => "unlimited",
		ValueType::Decimal => "unlimited",
		ValueType::Option(_) => unreachable!(),
		ValueType::Any => unreachable!(),
		ValueType::DictionaryId => unreachable!(),
		ValueType::List(_) => unreachable!(),
		ValueType::Record(_) => unreachable!(),
		ValueType::Tuple(_) => unreachable!(),
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
}
