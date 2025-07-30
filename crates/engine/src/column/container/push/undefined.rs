// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;

impl EngineColumnData {
    pub fn push_undefined(&mut self) {
        match self {
            EngineColumnData::Bool(container) => container.push_undefined(),
            EngineColumnData::Float4(container) => container.push_undefined(),
            EngineColumnData::Float8(container) => container.push_undefined(),
            EngineColumnData::Int1(container) => container.push_undefined(),
            EngineColumnData::Int2(container) => container.push_undefined(),
            EngineColumnData::Int4(container) => container.push_undefined(),
            EngineColumnData::Int8(container) => container.push_undefined(),
            EngineColumnData::Int16(container) => container.push_undefined(),
            EngineColumnData::Utf8(container) => container.push_undefined(),
            EngineColumnData::Uint1(container) => container.push_undefined(),
            EngineColumnData::Uint2(container) => container.push_undefined(),
            EngineColumnData::Uint4(container) => container.push_undefined(),
            EngineColumnData::Uint8(container) => container.push_undefined(),
            EngineColumnData::Uint16(container) => container.push_undefined(),
            EngineColumnData::Date(container) => container.push_undefined(),
            EngineColumnData::DateTime(container) => container.push_undefined(),
            EngineColumnData::Time(container) => container.push_undefined(),
            EngineColumnData::Interval(container) => container.push_undefined(),
            EngineColumnData::Undefined(container) => container.push_undefined(),
            EngineColumnData::RowId(container) => container.push_undefined(),
            EngineColumnData::Uuid4(container) => container.push_undefined(),
            EngineColumnData::Uuid7(container) => container.push_undefined(),
            EngineColumnData::Blob(container) => container.push_undefined(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::column::EngineColumnData;

    #[test]
    fn test_bool() {
        let mut col = EngineColumnData::bool(vec![true]);
        col.push_undefined();
        let EngineColumnData::Bool(container) = col else {
            panic!("Expected Bool");
        };

        assert_eq!(container.data().to_vec(), vec![true, false]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_float4() {
        let mut col = EngineColumnData::float4(vec![1.0]);
        col.push_undefined();
        let EngineColumnData::Float4(container) = col else {
            panic!("Expected Float4");
        };

        assert_eq!(container.data().as_slice(), &[1.0, 0.0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_float8() {
        let mut col = EngineColumnData::float8(vec![1.0]);
        col.push_undefined();
        let EngineColumnData::Float8(container) = col else {
            panic!("Expected Float8");
        };

        assert_eq!(container.data().as_slice(), &[1.0, 0.0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int1() {
        let mut col = EngineColumnData::int1(vec![1]);
        col.push_undefined();
        let EngineColumnData::Int1(container) = col else {
            panic!("Expected Int1");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int2() {
        let mut col = EngineColumnData::int2(vec![1]);
        col.push_undefined();
        let EngineColumnData::Int2(container) = col else {
            panic!("Expected Int2");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int4() {
        let mut col = EngineColumnData::int4(vec![1]);
        col.push_undefined();
        let EngineColumnData::Int4(container) = col else {
            panic!("Expected Int4");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int8() {
        let mut col = EngineColumnData::int8(vec![1]);
        col.push_undefined();
        let EngineColumnData::Int8(container) = col else {
            panic!("Expected Int8");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_int16() {
        let mut col = EngineColumnData::int16(vec![1]);
        col.push_undefined();
        let EngineColumnData::Int16(container) = col else {
            panic!("Expected Int16");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_string() {
        let mut col = EngineColumnData::utf8(vec!["a"]);
        col.push_undefined();
        let EngineColumnData::Utf8(container) = col else {
            panic!("Expected Utf8");
        };

        assert_eq!(container.data().as_slice(), &["a".to_string(), "".to_string()]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint1() {
        let mut col = EngineColumnData::uint1(vec![1]);
        col.push_undefined();
        let EngineColumnData::Uint1(container) = col else {
            panic!("Expected Uint1");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint2() {
        let mut col = EngineColumnData::uint2(vec![1]);
        col.push_undefined();
        let EngineColumnData::Uint2(container) = col else {
            panic!("Expected Uint2");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint4() {
        let mut col = EngineColumnData::uint4(vec![1]);
        col.push_undefined();
        let EngineColumnData::Uint4(container) = col else {
            panic!("Expected Uint4");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint8() {
        let mut col = EngineColumnData::uint8(vec![1]);
        col.push_undefined();
        let EngineColumnData::Uint8(container) = col else {
            panic!("Expected Uint8");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_uint16() {
        let mut col = EngineColumnData::uint16(vec![1]);
        col.push_undefined();
        let EngineColumnData::Uint16(container) = col else {
            panic!("Expected Uint16");
        };

        assert_eq!(container.data().as_slice(), &[1, 0]);
        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_undefined() {
        let mut col = EngineColumnData::undefined(5);
        col.push_undefined();
        let EngineColumnData::Undefined(container) = col else {
            panic!("Expected Undefined");
        };

        assert_eq!(container.len(), 6);
    }
}
