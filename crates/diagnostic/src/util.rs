// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::ValueKind;

pub(crate) fn value_max<'a>(value: ValueKind) -> &'a str {
    match value {
        ValueKind::Bool => unreachable!(),
        ValueKind::Float4 => "+3.4e38",
        ValueKind::Float8 => "+1.8e308",
        ValueKind::Int1 => "127",
        ValueKind::Int2 => "32,767",
        ValueKind::Int4 => "2,147,483,647",
        ValueKind::Int8 => "9,223,372,036,854,775,807",
        ValueKind::Int16 => "170,141,183,460,469,231,731,687,303,715,884,105,727",
        ValueKind::String => unreachable!(),
        ValueKind::Uint1 => "255",
        ValueKind::Uint2 => "65,535",
        ValueKind::Uint4 => "4,294,967,295",
        ValueKind::Uint8 => "18,446,744,073,709,551,615",
        ValueKind::Uint16 => "340,282,366,920,938,463,463,374,607,431,768,211,455",
        ValueKind::Undefined => unreachable!(),
    }
}

pub(crate) fn value_range<'a>(value: ValueKind) -> &'a str {
    match value {
        ValueKind::Bool => unreachable!(),
        ValueKind::Float4 => "-3.4e38 to +3.4e38",
        ValueKind::Float8 => "-1.8e308 to +1.8e308",
        ValueKind::Int1 => "-128 to 127",
        ValueKind::Int2 => "-32,768 to 32,767",
        ValueKind::Int4 => "-2,147,483,648 to 2,147,483,647",
        ValueKind::Int8 => "-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807",
        ValueKind::Int16 => {
            "-170,141,183,460,469,231,731,687,303,715,884,105,728 to 170,141,183,460,469,231,731,687,303,715,884,105,727"
        }
        ValueKind::String => unreachable!(),
        ValueKind::Uint1 => "0 to 255",
        ValueKind::Uint2 => "0 to 65,535",
        ValueKind::Uint4 => "0 to 4,294,967,295",
        ValueKind::Uint8 => "0 to 18,446,744,073,709,551,615",
        ValueKind::Uint16 => "0 to 340,282,366,920,938,463,463,374,607,431,768,211,455",
        ValueKind::Undefined => unreachable!(),
    }
}

#[cfg(test)]
mod tests {

    mod value_max {
        use crate::util::value_max;
        use reifydb_core::ValueKind;

        #[test]
        fn test_signed_ints() {
            assert_eq!(value_max(ValueKind::Int1), "127");
            assert_eq!(value_max(ValueKind::Int2), "32,767");
            assert_eq!(value_max(ValueKind::Int4), "2,147,483,647");
            assert_eq!(value_max(ValueKind::Int8), "9,223,372,036,854,775,807");
            assert_eq!(
                value_max(ValueKind::Int16),
                "170,141,183,460,469,231,731,687,303,715,884,105,727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(value_max(ValueKind::Uint1), "255");
            assert_eq!(value_max(ValueKind::Uint2), "65,535");
            assert_eq!(value_max(ValueKind::Uint4), "4,294,967,295");
            assert_eq!(value_max(ValueKind::Uint8), "18,446,744,073,709,551,615");
            assert_eq!(
                value_max(ValueKind::Uint16),
                "340,282,366,920,938,463,463,374,607,431,768,211,455"
            );
        }

        #[test]
        fn test_floats() {
            assert_eq!(value_max(ValueKind::Float4), "+3.4e38");
            assert_eq!(value_max(ValueKind::Float8), "+1.8e308");
        }
    }

    mod value_range {
        use crate::util::value_range;
        use reifydb_core::ValueKind;

        #[test]
        fn test_signed_ints() {
            assert_eq!(value_range(ValueKind::Int1), "-128 to 127");
            assert_eq!(value_range(ValueKind::Int2), "-32,768 to 32,767");
            assert_eq!(value_range(ValueKind::Int4), "-2,147,483,648 to 2,147,483,647");
            assert_eq!(
                value_range(ValueKind::Int8),
                "-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807"
            );
            assert_eq!(
                value_range(ValueKind::Int16),
                "-170,141,183,460,469,231,731,687,303,715,884,105,728 to 170,141,183,460,469,231,731,687,303,715,884,105,727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(value_range(ValueKind::Uint1), "0 to 255");
            assert_eq!(value_range(ValueKind::Uint2), "0 to 65,535");
            assert_eq!(value_range(ValueKind::Uint4), "0 to 4,294,967,295");
            assert_eq!(value_range(ValueKind::Uint8), "0 to 18,446,744,073,709,551,615");
            assert_eq!(
                value_range(ValueKind::Uint16),
                "0 to 340,282,366,920,938,463,463,374,607,431,768,211,455"
            );
        }

        #[test]
        fn test_floats() {
            assert_eq!(value_range(ValueKind::Float4), "-3.4e38 to +3.4e38");
            assert_eq!(value_range(ValueKind::Float8), "-1.8e308 to +1.8e308");
        }
    }
}
