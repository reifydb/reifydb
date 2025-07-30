// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnValues;

impl ColumnValues {
    pub fn push_undefined(&mut self) {
        match self {
            ColumnValues::Bool(container) => container.push_undefined(),
            ColumnValues::Float4(container) => container.push_undefined(),
            ColumnValues::Float8(container) => container.push_undefined(),
            ColumnValues::Int1(container) => container.push_undefined(),
            ColumnValues::Int2(container) => container.push_undefined(),
            ColumnValues::Int4(container) => container.push_undefined(),
            ColumnValues::Int8(container) => container.push_undefined(),
            ColumnValues::Int16(container) => container.push_undefined(),
            ColumnValues::Utf8(container) => container.push_undefined(),
            ColumnValues::Uint1(container) => container.push_undefined(),
            ColumnValues::Uint2(container) => container.push_undefined(),
            ColumnValues::Uint4(container) => container.push_undefined(),
            ColumnValues::Uint8(container) => container.push_undefined(),
            ColumnValues::Uint16(container) => container.push_undefined(),
            ColumnValues::Date(container) => container.push_undefined(),
            ColumnValues::DateTime(container) => container.push_undefined(),
            ColumnValues::Time(container) => container.push_undefined(),
            ColumnValues::Interval(container) => container.push_undefined(),
            ColumnValues::Undefined(container) => container.push_undefined(),
            ColumnValues::RowId(container) => container.push_undefined(),
            ColumnValues::Uuid4(container) => container.push_undefined(),
            ColumnValues::Uuid7(container) => container.push_undefined(),
            ColumnValues::Blob(container) => container.push_undefined(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::column::ColumnValues;

    #[test]
    fn test_bool() {
        let mut col = ColumnValues::bool(vec![true]);
        col.push_undefined();
        let ColumnValues::Bool(container) = col else {
            panic!("Expected Bool");
        };

        assert_eq!(container.values().to_vec(), vec![true, false]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_float4() {
        let mut col = ColumnValues::float4(vec![1.0]);
        col.push_undefined();
        let ColumnValues::Float4(container) = col else {
            panic!("Expected Float4");
        };

        assert_eq!(container.values().as_slice(), &[1.0, 0.0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_float8() {
        let mut col = ColumnValues::float8(vec![1.0]);
        col.push_undefined();
        let ColumnValues::Float8(container) = col else {
            panic!("Expected Float8");
        };

        assert_eq!(container.values().as_slice(), &[1.0, 0.0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int1() {
        let mut col = ColumnValues::int1(vec![1]);
        col.push_undefined();
        let ColumnValues::Int1(container) = col else {
            panic!("Expected Int1");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int2() {
        let mut col = ColumnValues::int2(vec![1]);
        col.push_undefined();
        let ColumnValues::Int2(container) = col else {
            panic!("Expected Int2");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int4() {
        let mut col = ColumnValues::int4(vec![1]);
        col.push_undefined();
        let ColumnValues::Int4(container) = col else {
            panic!("Expected Int4");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int8() {
        let mut col = ColumnValues::int8(vec![1]);
        col.push_undefined();
        let ColumnValues::Int8(container) = col else {
            panic!("Expected Int8");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int16() {
        let mut col = ColumnValues::int16(vec![1]);
        col.push_undefined();
        let ColumnValues::Int16(container) = col else {
            panic!("Expected Int16");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_string() {
        let mut col = ColumnValues::utf8(vec!["a"]);
        col.push_undefined();
        let ColumnValues::Utf8(container) = col else {
            panic!("Expected Utf8");
        };

        assert_eq!(container.values().as_slice(), &["a".to_string(), "".to_string()]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint1() {
        let mut col = ColumnValues::uint1(vec![1]);
        col.push_undefined();
        let ColumnValues::Uint1(container) = col else {
            panic!("Expected Uint1");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint2() {
        let mut col = ColumnValues::uint2(vec![1]);
        col.push_undefined();
        let ColumnValues::Uint2(container) = col else {
            panic!("Expected Uint2");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint4() {
        let mut col = ColumnValues::uint4(vec![1]);
        col.push_undefined();
        let ColumnValues::Uint4(container) = col else {
            panic!("Expected Uint4");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint8() {
        let mut col = ColumnValues::uint8(vec![1]);
        col.push_undefined();
        let ColumnValues::Uint8(container) = col else {
            panic!("Expected Uint8");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint16() {
        let mut col = ColumnValues::uint16(vec![1]);
        col.push_undefined();
        let ColumnValues::Uint16(container) = col else {
            panic!("Expected Uint16");
        };

        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_undefined() {
        let mut col = ColumnValues::undefined(5);
        col.push_undefined();
        let ColumnValues::Undefined(container) = col else {
            panic!("Expected Undefined");
        };

        assert_eq!(container.len(), 6);
    }
}
