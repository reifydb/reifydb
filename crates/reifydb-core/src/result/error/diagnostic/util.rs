// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type;

pub(crate) fn value_max<'a>(value: Type) -> &'a str {
	match value {
		Type::Bool => unreachable!(),
		Type::Float4 => "+3.4e38",
		Type::Float8 => "+1.8e308",
		Type::Int1 => "127",
		Type::Int2 => "32_767",
		Type::Int4 => "2_147_483_647",
		Type::Int8 => "9_223_372_036_854_775_807",
		Type::Int16 => {
			"170_141_183_460_469_231_731_687_303_715_884_105_727"
		}
		Type::Utf8 => unreachable!(),
		Type::Uint1 => "255",
		Type::Uint2 => "65_535",
		Type::Uint4 => "4_294_967_295",
		Type::Uint8 => "18_446_744_073_709_551_615",
		Type::Uint16 => {
			"340_282_366_920_938_463_463_374_607_431_768_211_455"
		}
		Type::Date => unreachable!(),
		Type::DateTime => unreachable!(),
		Type::Time => unreachable!(),
		Type::Interval => unreachable!(),
		Type::RowId => "18_446_744_073_709_551_615",
		Type::Uuid4 => unreachable!(),
		Type::Uuid7 => unreachable!(),
		Type::Blob => unreachable!(),
		Type::Undefined => unreachable!(),
	}
}

pub(crate) fn value_range<'a>(value: Type) -> &'a str {
	match value {
		Type::Bool => unreachable!(),
		Type::Float4 => "-3.4e38 to +3.4e38",
		Type::Float8 => "-1.8e308 to +1.8e308",
		Type::Int1 => "-128 to 127",
		Type::Int2 => "-32_768 to 32_767",
		Type::Int4 => "-2_147_483_648 to 2_147_483_647",
		Type::Int8 => {
			"-9_223_372_036_854_775_808 to 9_223_372_036_854_775_807"
		}
		Type::Int16 => {
			"-170_141_183_460_469_231_731_687_303_715_884_105_728 to 170_141_183_460_469_231_731_687_303_715_884_105_727"
		}
		Type::Utf8 => unreachable!(),
		Type::Uint1 => "0 to 255",
		Type::Uint2 => "0 to 65_535",
		Type::Uint4 => "0 to 4_294_967_295",
		Type::Uint8 => "0 to 18_446_744_073_709_551_615",
		Type::Uint16 => {
			"0 to 340_282_366_920_938_463_463_374_607_431_768_211_455"
		}
		Type::Date => unreachable!(),
		Type::DateTime => unreachable!(),
		Type::Time => unreachable!(),
		Type::Interval => unreachable!(),
		Type::RowId => "0 to 18_446_744_073_709_551_615",
		Type::Uuid4 => unreachable!(),
		Type::Uuid7 => unreachable!(),
		Type::Blob => unreachable!(),
		Type::Undefined => unreachable!(),
	}
}

#[cfg(test)]
mod tests {

	mod value_max {
		use crate::{Type, result::error::diagnostic::util::value_max};

		#[test]
		fn test_signed_ints() {
			assert_eq!(value_max(Type::Int1), "127");
			assert_eq!(value_max(Type::Int2), "32_767");
			assert_eq!(value_max(Type::Int4), "2_147_483_647");
			assert_eq!(
				value_max(Type::Int8),
				"9_223_372_036_854_775_807"
			);
			assert_eq!(
				value_max(Type::Int16),
				"170_141_183_460_469_231_731_687_303_715_884_105_727"
			);
		}

		#[test]
		fn test_unsigned_ints() {
			assert_eq!(value_max(Type::Uint1), "255");
			assert_eq!(value_max(Type::Uint2), "65_535");
			assert_eq!(value_max(Type::Uint4), "4_294_967_295");
			assert_eq!(
				value_max(Type::Uint8),
				"18_446_744_073_709_551_615"
			);
			assert_eq!(
				value_max(Type::Uint16),
				"340_282_366_920_938_463_463_374_607_431_768_211_455"
			);
		}

		#[test]
		fn test_floats() {
			assert_eq!(value_max(Type::Float4), "+3.4e38");
			assert_eq!(value_max(Type::Float8), "+1.8e308");
		}
	}

	mod value_range {
		use crate::{
			Type, result::error::diagnostic::util::value_range,
		};

		#[test]
		fn test_signed_ints() {
			assert_eq!(value_range(Type::Int1), "-128 to 127");
			assert_eq!(
				value_range(Type::Int2),
				"-32_768 to 32_767"
			);
			assert_eq!(
				value_range(Type::Int4),
				"-2_147_483_648 to 2_147_483_647"
			);
			assert_eq!(
				value_range(Type::Int8),
				"-9_223_372_036_854_775_808 to 9_223_372_036_854_775_807"
			);
			assert_eq!(
				value_range(Type::Int16),
				"-170_141_183_460_469_231_731_687_303_715_884_105_728 to 170_141_183_460_469_231_731_687_303_715_884_105_727"
			);
		}

		#[test]
		fn test_unsigned_ints() {
			assert_eq!(value_range(Type::Uint1), "0 to 255");
			assert_eq!(value_range(Type::Uint2), "0 to 65_535");
			assert_eq!(
				value_range(Type::Uint4),
				"0 to 4_294_967_295"
			);
			assert_eq!(
				value_range(Type::Uint8),
				"0 to 18_446_744_073_709_551_615"
			);
			assert_eq!(
				value_range(Type::Uint16),
				"0 to 340_282_366_920_938_463_463_374_607_431_768_211_455"
			);
		}

		#[test]
		fn test_floats() {
			assert_eq!(
				value_range(Type::Float4),
				"-3.4e38 to +3.4e38"
			);
			assert_eq!(
				value_range(Type::Float8),
				"-1.8e308 to +1.8e308"
			);
		}
	}
}
