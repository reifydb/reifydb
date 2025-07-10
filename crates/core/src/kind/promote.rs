// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Kind;
use std::cmp::min;

impl Kind {
    /// Promote two ValueKinds to a common supertype, similar to PostgreSQL expression evaluation.
    pub fn promote(left: Kind, right: Kind) -> Kind {
        use Kind::*;

        if left == Undefined || right == Undefined {
            return Undefined;
        }

        if left == Utf8 || right == Utf8 {
            return Utf8;
        }

        if left == Bool || right == Bool {
            return Bool;
        }

        if left == Float8 || right == Float8 {
            return Float8;
        }

        if left == Float4 || right == Float4 {
            return Float8;
        }

        let signed_order = [Int1, Int2, Int4, Int8, Int16];
        let unsigned_order = [Uint1, Uint2, Uint4, Uint8, Uint16];

        let is_signed = |k: Kind| signed_order.contains(&k);
        let is_unsigned = |k: Kind| unsigned_order.contains(&k);

        let rank = |k: Kind| match k {
            Int1 | Uint1 => 0,
            Int2 | Uint2 => 1,
            Int4 | Uint4 => 2,
            Int8 | Uint8 => 3,
            Int16 | Uint16 => 4,
            _ => usize::MAX,
        };

        if is_signed(left) && is_signed(right) {
            return signed_order[min(rank(left).max(rank(right)), 3) + 1];
        }

        if is_unsigned(left) && is_unsigned(right) {
            return unsigned_order[min(rank(left).max(rank(right)), 3) + 1];
        }

        if (is_signed(left) && is_unsigned(right)) || (is_unsigned(left) && is_signed(right)) {
            return match rank(left).max(rank(right)) + 1 {
                0 => Int1,
                1 => Int2,
                2 => Int4,
                3 => Int8,
                4 => Int16,
                _ => Int16,
            };
        }

        Undefined
    }
}

#[cfg(test)]
mod tests {
    use crate::Kind;

    #[test]
    fn test_promote_bool() {
        use Kind::*;
        let cases = [
            (Bool, Bool, Bool),
            (Bool, Float4, Bool),
            (Bool, Float8, Bool),
            (Bool, Int1, Bool),
            (Bool, Int2, Bool),
            (Bool, Int4, Bool),
            (Bool, Int8, Bool),
            (Bool, Int16, Bool),
            (Bool, Utf8, Utf8),
            (Bool, Uint1, Bool),
            (Bool, Uint2, Bool),
            (Bool, Uint4, Bool),
            (Bool, Uint8, Bool),
            (Bool, Uint16, Bool),
            (Bool, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_float4() {
        use Kind::*;
        let cases = [
            (Float4, Bool, Bool),
            (Float4, Float4, Float8),
            (Float4, Float8, Float8),
            (Float4, Int1, Float8),
            (Float4, Int2, Float8),
            (Float4, Int4, Float8),
            (Float4, Int8, Float8),
            (Float4, Int16, Float8),
            (Float4, Utf8, Utf8),
            (Float4, Uint1, Float8),
            (Float4, Uint2, Float8),
            (Float4, Uint4, Float8),
            (Float4, Uint8, Float8),
            (Float4, Uint16, Float8),
            (Float4, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_float8() {
        use Kind::*;
        let cases = [
            (Float8, Bool, Bool),
            (Float8, Float4, Float8),
            (Float8, Float8, Float8),
            (Float8, Int1, Float8),
            (Float8, Int2, Float8),
            (Float8, Int4, Float8),
            (Float8, Int8, Float8),
            (Float8, Int16, Float8),
            (Float8, Utf8, Utf8),
            (Float8, Uint1, Float8),
            (Float8, Uint2, Float8),
            (Float8, Uint4, Float8),
            (Float8, Uint8, Float8),
            (Float8, Uint16, Float8),
            (Float8, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_int1() {
        use Kind::*;
        let cases = [
            (Int1, Bool, Bool),
            (Int1, Float4, Float8),
            (Int1, Float8, Float8),
            (Int1, Int1, Int2),
            (Int1, Int2, Int4),
            (Int1, Int4, Int8),
            (Int1, Int8, Int16),
            (Int1, Int16, Int16),
            (Int1, Utf8, Utf8),
            (Int1, Uint1, Int2),
            (Int1, Uint2, Int4),
            (Int1, Uint4, Int8),
            (Int1, Uint8, Int16),
            (Int1, Uint16, Int16),
            (Int1, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_int2() {
        use Kind::*;
        let cases = [
            (Int2, Bool, Bool),
            (Int2, Float4, Float8),
            (Int2, Float8, Float8),
            (Int2, Int1, Int4),
            (Int2, Int2, Int4),
            (Int2, Int4, Int8),
            (Int2, Int8, Int16),
            (Int2, Int16, Int16),
            (Int2, Utf8, Utf8),
            (Int2, Uint1, Int4),
            (Int2, Uint2, Int4),
            (Int2, Uint4, Int8),
            (Int2, Uint8, Int16),
            (Int2, Uint16, Int16),
            (Int2, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_int4() {
        use Kind::*;
        let cases = [
            (Int4, Bool, Bool),
            (Int4, Float4, Float8),
            (Int4, Float8, Float8),
            (Int4, Int1, Int8),
            (Int4, Int2, Int8),
            (Int4, Int4, Int8),
            (Int4, Int8, Int16),
            (Int4, Int16, Int16),
            (Int4, Utf8, Utf8),
            (Int4, Uint1, Int8),
            (Int4, Uint2, Int8),
            (Int4, Uint4, Int8),
            (Int4, Uint8, Int16),
            (Int4, Uint16, Int16),
            (Int4, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_int8() {
        use Kind::*;
        let cases = [
            (Int8, Bool, Bool),
            (Int8, Float4, Float8),
            (Int8, Float8, Float8),
            (Int8, Int1, Int16),
            (Int8, Int2, Int16),
            (Int8, Int4, Int16),
            (Int8, Int8, Int16),
            (Int8, Int16, Int16),
            (Int8, Utf8, Utf8),
            (Int8, Uint1, Int16),
            (Int8, Uint2, Int16),
            (Int8, Uint4, Int16),
            (Int8, Uint8, Int16),
            (Int8, Uint16, Int16),
            (Int8, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_int16() {
        use Kind::*;
        let cases = [
            (Int16, Bool, Bool),
            (Int16, Float4, Float8),
            (Int16, Float8, Float8),
            (Int16, Int1, Int16),
            (Int16, Int2, Int16),
            (Int16, Int4, Int16),
            (Int16, Int8, Int16),
            (Int16, Int16, Int16),
            (Int16, Utf8, Utf8),
            (Int16, Uint1, Int16),
            (Int16, Uint2, Int16),
            (Int16, Uint4, Int16),
            (Int16, Uint8, Int16),
            (Int16, Uint16, Int16),
            (Int16, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_string() {
        use Kind::*;
        let kinds = [
            Bool, Float4, Float8, Int1, Int2, Int4, Int8, Int16, Utf8, Uint1, Uint2, Uint4, Uint8,
            Uint16,
        ];
        for kind in kinds {
            assert_eq!(Kind::promote(Utf8, kind), Utf8);
        }

        assert_eq!(Kind::promote(Utf8, Undefined), Undefined);
    }

    #[test]
    fn test_promote_uint1() {
        use Kind::*;
        let cases = [
            (Uint1, Bool, Bool),
            (Uint1, Float4, Float8),
            (Uint1, Float8, Float8),
            (Uint1, Int1, Int2),
            (Uint1, Int2, Int4),
            (Uint1, Int4, Int8),
            (Uint1, Int8, Int16),
            (Uint1, Int16, Int16),
            (Uint1, Utf8, Utf8),
            (Uint1, Uint1, Uint2),
            (Uint1, Uint2, Uint4),
            (Uint1, Uint4, Uint8),
            (Uint1, Uint8, Uint16),
            (Uint1, Uint16, Uint16),
            (Uint1, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_uint2() {
        use Kind::*;
        let cases = [
            (Uint2, Bool, Bool),
            (Uint2, Float4, Float8),
            (Uint2, Float8, Float8),
            (Uint2, Int1, Int4),
            (Uint2, Int2, Int4),
            (Uint2, Int4, Int8),
            (Uint2, Int8, Int16),
            (Uint2, Int16, Int16),
            (Uint2, Utf8, Utf8),
            (Uint2, Uint1, Uint4),
            (Uint2, Uint2, Uint4),
            (Uint2, Uint4, Uint8),
            (Uint2, Uint8, Uint16),
            (Uint2, Uint16, Uint16),
            (Uint2, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_uint4() {
        use Kind::*;
        let cases = [
            (Uint4, Bool, Bool),
            (Uint4, Float4, Float8),
            (Uint4, Float8, Float8),
            (Uint4, Int1, Int8),
            (Uint4, Int2, Int8),
            (Uint4, Int4, Int8),
            (Uint4, Int8, Int16),
            (Uint4, Int16, Int16),
            (Uint4, Utf8, Utf8),
            (Uint4, Uint1, Uint8),
            (Uint4, Uint2, Uint8),
            (Uint4, Uint4, Uint8),
            (Uint4, Uint8, Uint16),
            (Uint4, Uint16, Uint16),
            (Uint4, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_uint8() {
        use Kind::*;
        let cases = [
            (Uint8, Bool, Bool),
            (Uint8, Float4, Float8),
            (Uint8, Float8, Float8),
            (Uint8, Int1, Int16),
            (Uint8, Int2, Int16),
            (Uint8, Int4, Int16),
            (Uint8, Int8, Int16),
            (Uint8, Int16, Int16),
            (Uint8, Utf8, Utf8),
            (Uint8, Uint1, Uint16),
            (Uint8, Uint2, Uint16),
            (Uint8, Uint4, Uint16),
            (Uint8, Uint8, Uint16),
            (Uint8, Uint16, Uint16),
            (Uint8, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_uint16() {
        use Kind::*;
        let cases = [
            (Uint16, Bool, Bool),
            (Uint16, Float4, Float8),
            (Uint16, Float8, Float8),
            (Uint16, Int1, Int16),
            (Uint16, Int2, Int16),
            (Uint16, Int4, Int16),
            (Uint16, Int8, Int16),
            (Uint16, Int16, Int16),
            (Uint16, Utf8, Utf8),
            (Uint16, Uint1, Uint16),
            (Uint16, Uint2, Uint16),
            (Uint16, Uint4, Uint16),
            (Uint16, Uint8, Uint16),
            (Uint16, Uint16, Uint16),
            (Uint16, Undefined, Undefined),
        ];
        for (left, right, expected) in cases {
            assert_eq!(Kind::promote(left, right), expected);
        }
    }

    #[test]
    fn test_promote_undefined() {
        use Kind::*;
        let kinds = [
            Bool, Float4, Float8, Int1, Int2, Int4, Int8, Int16, Utf8, Uint1, Uint2, Uint4, Uint8,
            Uint16, Undefined,
        ];
        for kind in kinds {
            assert_eq!(Kind::promote(Undefined, kind), Undefined);
        }
    }
}
