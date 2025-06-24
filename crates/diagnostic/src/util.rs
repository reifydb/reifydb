// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::Kind;

pub(crate) fn value_max<'a>(value: Kind) -> &'a str {
    match value {
        Kind::Bool => unreachable!(),
        Kind::Float4 => "+3.4e38",
        Kind::Float8 => "+1.8e308",
        Kind::Int1 => "127",
        Kind::Int2 => "32,767",
        Kind::Int4 => "2,147,483,647",
        Kind::Int8 => "9,223,372,036,854,775,807",
        Kind::Int16 => "170,141,183,460,469,231,731,687,303,715,884,105,727",
        Kind::String => unreachable!(),
        Kind::Uint1 => "255",
        Kind::Uint2 => "65,535",
        Kind::Uint4 => "4,294,967,295",
        Kind::Uint8 => "18,446,744,073,709,551,615",
        Kind::Uint16 => "340,282,366,920,938,463,463,374,607,431,768,211,455",
        Kind::Undefined => unreachable!(),
    }
}

pub(crate) fn value_range<'a>(value: Kind) -> &'a str {
    match value {
        Kind::Bool => unreachable!(),
        Kind::Float4 => "-3.4e38 to +3.4e38",
        Kind::Float8 => "-1.8e308 to +1.8e308",
        Kind::Int1 => "-128 to 127",
        Kind::Int2 => "-32,768 to 32,767",
        Kind::Int4 => "-2,147,483,648 to 2,147,483,647",
        Kind::Int8 => "-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807",
        Kind::Int16 => {
            "-170,141,183,460,469,231,731,687,303,715,884,105,728 to 170,141,183,460,469,231,731,687,303,715,884,105,727"
        }
        Kind::String => unreachable!(),
        Kind::Uint1 => "0 to 255",
        Kind::Uint2 => "0 to 65,535",
        Kind::Uint4 => "0 to 4,294,967,295",
        Kind::Uint8 => "0 to 18,446,744,073,709,551,615",
        Kind::Uint16 => "0 to 340,282,366,920,938,463,463,374,607,431,768,211,455",
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
            assert_eq!(value_max(Kind::Int2), "32,767");
            assert_eq!(value_max(Kind::Int4), "2,147,483,647");
            assert_eq!(value_max(Kind::Int8), "9,223,372,036,854,775,807");
            assert_eq!(
                value_max(Kind::Int16),
                "170,141,183,460,469,231,731,687,303,715,884,105,727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(value_max(Kind::Uint1), "255");
            assert_eq!(value_max(Kind::Uint2), "65,535");
            assert_eq!(value_max(Kind::Uint4), "4,294,967,295");
            assert_eq!(value_max(Kind::Uint8), "18,446,744,073,709,551,615");
            assert_eq!(
                value_max(Kind::Uint16),
                "340,282,366,920,938,463,463,374,607,431,768,211,455"
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
            assert_eq!(value_range(Kind::Int2), "-32,768 to 32,767");
            assert_eq!(value_range(Kind::Int4), "-2,147,483,648 to 2,147,483,647");
            assert_eq!(
                value_range(Kind::Int8),
                "-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807"
            );
            assert_eq!(
                value_range(Kind::Int16),
                "-170,141,183,460,469,231,731,687,303,715,884,105,728 to 170,141,183,460,469,231,731,687,303,715,884,105,727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(value_range(Kind::Uint1), "0 to 255");
            assert_eq!(value_range(Kind::Uint2), "0 to 65,535");
            assert_eq!(value_range(Kind::Uint4), "0 to 4,294,967,295");
            assert_eq!(value_range(Kind::Uint8), "0 to 18,446,744,073,709,551,615");
            assert_eq!(
                value_range(Kind::Uint16),
                "0 to 340,282,366,920,938,463,463,374,607,431,768,211,455"
            );
        }

        #[test]
        fn test_floats() {
            assert_eq!(value_range(Kind::Float4), "-3.4e38 to +3.4e38");
            assert_eq!(value_range(Kind::Float8), "-1.8e308 to +1.8e308");
        }
    }
}
