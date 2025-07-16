// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::DataType;

pub(crate) fn value_max<'a>(value: DataType) -> &'a str {
    match value {
        DataType::Bool => unreachable!(),
        DataType::Float4 => "+3.4e38",
        DataType::Float8 => "+1.8e308",
        DataType::Int1 => "127",
        DataType::Int2 => "32_767",
        DataType::Int4 => "2_147_483_647",
        DataType::Int8 => "9_223_372_036_854_775_807",
        DataType::Int16 => "170_141_183_460_469_231_731_687_303_715_884_105_727",
        DataType::Utf8 => unreachable!(),
        DataType::Uint1 => "255",
        DataType::Uint2 => "65_535",
        DataType::Uint4 => "4_294_967_295",
        DataType::Uint8 => "18_446_744_073_709_551_615",
        DataType::Uint16 => "340_282_366_920_938_463_463_374_607_431_768_211_455",
        DataType::Date => unreachable!(),
        DataType::DateTime => unreachable!(),
        DataType::Time => unreachable!(),
        DataType::Interval => unreachable!(),
        DataType::Undefined => unreachable!(),
    }
}

pub(crate) fn value_range<'a>(value: DataType) -> &'a str {
    match value {
        DataType::Bool => unreachable!(),
        DataType::Float4 => "-3.4e38 to +3.4e38",
        DataType::Float8 => "-1.8e308 to +1.8e308",
        DataType::Int1 => "-128 to 127",
        DataType::Int2 => "-32_768 to 32_767",
        DataType::Int4 => "-2_147_483_648 to 2_147_483_647",
        DataType::Int8 => "-9_223_372_036_854_775_808 to 9_223_372_036_854_775_807",
        DataType::Int16 => {
            "-170_141_183_460_469_231_731_687_303_715_884_105_728 to 170_141_183_460_469_231_731_687_303_715_884_105_727"
        }
        DataType::Utf8 => unreachable!(),
        DataType::Uint1 => "0 to 255",
        DataType::Uint2 => "0 to 65_535",
        DataType::Uint4 => "0 to 4_294_967_295",
        DataType::Uint8 => "0 to 18_446_744_073_709_551_615",
        DataType::Uint16 => "0 to 340_282_366_920_938_463_463_374_607_431_768_211_455",
        DataType::Date => unreachable!(),
        DataType::DateTime => unreachable!(),
        DataType::Time => unreachable!(),
        DataType::Interval => unreachable!(),
        DataType::Undefined => unreachable!(),
    }
}

#[cfg(test)]
mod tests {

    mod value_max {
        use crate::diagnostic::util::value_max;
        use crate::DataType;

        #[test]
        fn test_signed_ints() {
            assert_eq!(value_max(DataType::Int1), "127");
            assert_eq!(value_max(DataType::Int2), "32_767");
            assert_eq!(value_max(DataType::Int4), "2_147_483_647");
            assert_eq!(value_max(DataType::Int8), "9_223_372_036_854_775_807");
            assert_eq!(
				value_max(DataType::Int16),
				"170_141_183_460_469_231_731_687_303_715_884_105_727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(value_max(DataType::Uint1), "255");
            assert_eq!(value_max(DataType::Uint2), "65_535");
            assert_eq!(value_max(DataType::Uint4), "4_294_967_295");
            assert_eq!(value_max(DataType::Uint8), "18_446_744_073_709_551_615");
            assert_eq!(
				value_max(DataType::Uint16),
				"340_282_366_920_938_463_463_374_607_431_768_211_455"
            );
        }

        #[test]
        fn test_floats() {
            assert_eq!(value_max(DataType::Float4), "+3.4e38");
            assert_eq!(value_max(DataType::Float8), "+1.8e308");
        }

    }

    mod value_range {
        use crate::diagnostic::util::value_range;
        use crate::DataType;

        #[test]
        fn test_signed_ints() {
            assert_eq!(value_range(DataType::Int1), "-128 to 127");
            assert_eq!(value_range(DataType::Int2), "-32_768 to 32_767");
            assert_eq!(value_range(DataType::Int4), "-2_147_483_648 to 2_147_483_647");
            assert_eq!(
				value_range(DataType::Int8),
				"-9_223_372_036_854_775_808 to 9_223_372_036_854_775_807"
            );
            assert_eq!(
				value_range(DataType::Int16),
				"-170_141_183_460_469_231_731_687_303_715_884_105_728 to 170_141_183_460_469_231_731_687_303_715_884_105_727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(value_range(DataType::Uint1), "0 to 255");
            assert_eq!(value_range(DataType::Uint2), "0 to 65_535");
            assert_eq!(value_range(DataType::Uint4), "0 to 4_294_967_295");
            assert_eq!(value_range(DataType::Uint8), "0 to 18_446_744_073_709_551_615");
            assert_eq!(
				value_range(DataType::Uint16),
				"0 to 340_282_366_920_938_463_463_374_607_431_768_211_455"
            );
        }

        #[test]
        fn test_floats() {
            assert_eq!(value_range(DataType::Float4), "-3.4e38 to +3.4e38");
            assert_eq!(value_range(DataType::Float8), "-1.8e308 to +1.8e308");
        }

    }
}