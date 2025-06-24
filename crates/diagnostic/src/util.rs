// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::Kind;

pub(crate) fn value_max<'a>(value: Kind) -> &'a str {
    match value {
        Kind::Bool => unreachable!(),
        Kind::Float4 => "+3.4e38",
        Kind::Float8 => "+1.8e308",
        Kind::Int1 => "127",
        Kind::Int2 => "32_767",
        Kind::Int4 => "2_147_483_647",
        Kind::Int8 => "9_223_372_036_854_775_807",
        Kind::Int16 => "170_141_183_460_469_231_731_687_303_715_884_105_727",
        Kind::Text => unreachable!(),
        Kind::Uint1 => "255",
        Kind::Uint2 => "65_535",
        Kind::Uint4 => "4_294_967_295",
        Kind::Uint8 => "18_446_744_073_709_551_615",
        Kind::Uint16 => "340_282_366_920_938_463_463_374_607_431_768_211_455",
        Kind::Undefined => unreachable!(),
    }
}

pub(crate) fn value_range<'a>(value: Kind) -> &'a str {
    match value {
        Kind::Bool => unreachable!(),
        Kind::Float4 => "-3.4e38 to +3.4e38",
        Kind::Float8 => "-1.8e308 to +1.8e308",
        Kind::Int1 => "-128 to 127",
        Kind::Int2 => "-32_768 to 32_767",
        Kind::Int4 => "-2_147_483_648 to 2_147_483_647",
        Kind::Int8 => "-9_223_372_036_854_775_808 to 9_223_372_036_854_775_807",
        Kind::Int16 => {
            "-170_141_183_460_469_231_731_687_303_715_884_105_728 to 170_141_183_460_469_231_731_687_303_715_884_105_727"
        }
        Kind::Text => unreachable!(),
        Kind::Uint1 => "0 to 255",
        Kind::Uint2 => "0 to 65_535",
        Kind::Uint4 => "0 to 4_294_967_295",
        Kind::Uint8 => "0 to 18_446_744_073_709_551_615",
        Kind::Uint16 => "0 to 340_282_366_920_938_463_463_374_607_431_768_211_455",
        Kind::Undefined => unreachable!(),
    }
}

#[cfg(test)]
mod tests {

    mod value_max {
        use crate::util::value_max;
        use reifydb_core::Kind;

        #[test]
        fn test_signed_ints() {
            assert_eq!(value_max(Kind::Int1), "127");
            assert_eq!(value_max(Kind::Int2), "32_767");
            assert_eq!(value_max(Kind::Int4), "2_147_483_647");
            assert_eq!(value_max(Kind::Int8), "9_223_372_036_854_775_807");
            assert_eq!(
                value_max(Kind::Int16),
                "170_141_183_460_469_231_731_687_303_715_884_105_727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(value_max(Kind::Uint1), "255");
            assert_eq!(value_max(Kind::Uint2), "65_535");
            assert_eq!(value_max(Kind::Uint4), "4_294_967_295");
            assert_eq!(value_max(Kind::Uint8), "18_446_744_073_709_551_615");
            assert_eq!(
                value_max(Kind::Uint16),
                "340_282_366_920_938_463_463_374_607_431_768_211_455"
            );
        }

        #[test]
        fn test_floats() {
            assert_eq!(value_max(Kind::Float4), "+3.4e38");
            assert_eq!(value_max(Kind::Float8), "+1.8e308");
        }
    }

    mod value_range {
        use crate::util::value_range;
        use reifydb_core::Kind;

        #[test]
        fn test_signed_ints() {
            assert_eq!(value_range(Kind::Int1), "-128 to 127");
            assert_eq!(value_range(Kind::Int2), "-32_768 to 32_767");
            assert_eq!(value_range(Kind::Int4), "-2_147_483_648 to 2_147_483_647");
            assert_eq!(
                value_range(Kind::Int8),
                "-9_223_372_036_854_775_808 to 9_223_372_036_854_775_807"
            );
            assert_eq!(
                value_range(Kind::Int16),
                "-170_141_183_460_469_231_731_687_303_715_884_105_728 to 170_141_183_460_469_231_731_687_303_715_884_105_727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(value_range(Kind::Uint1), "0 to 255");
            assert_eq!(value_range(Kind::Uint2), "0 to 65_535");
            assert_eq!(value_range(Kind::Uint4), "0 to 4_294_967_295");
            assert_eq!(value_range(Kind::Uint8), "0 to 18_446_744_073_709_551_615");
            assert_eq!(
                value_range(Kind::Uint16),
                "0 to 340_282_366_920_938_463_463_374_607_431_768_211_455"
            );
        }

        #[test]
        fn test_floats() {
            assert_eq!(value_range(Kind::Float4), "-3.4e38 to +3.4e38");
            assert_eq!(value_range(Kind::Float8), "-1.8e308 to +1.8e308");
        }
    }
}
