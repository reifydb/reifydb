// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use crate::value::uuid::{Uuid4, Uuid7};
use crate::value::Blob;
use crate::{Date, DateTime, Interval, RowId, Time};

impl ColumnValues {
    pub fn push_undefined(&mut self) {
        match self {
            ColumnValues::Bool(values, bitvecity) => {
                values.push(false);
                bitvecity.push(false);
            }
            ColumnValues::Float4(values, bitvecity) => {
                values.push(0.0);
                bitvecity.push(false);
            }
            ColumnValues::Float8(values, bitvecity) => {
                values.push(0.0);
                bitvecity.push(false);
            }
            ColumnValues::Int1(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Int2(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Int4(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Int8(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Int16(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Utf8(values, bitvecity) => {
                values.push(String::new());
                bitvecity.push(false);
            }
            ColumnValues::Uint1(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Uint2(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Uint4(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Uint8(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Uint16(values, bitvecity) => {
                values.push(0);
                bitvecity.push(false);
            }
            ColumnValues::Date(values, bitvecity) => {
                values.push(Date::default());
                bitvecity.push(false);
            }
            ColumnValues::DateTime(values, bitvecity) => {
                values.push(DateTime::default());
                bitvecity.push(false);
            }
            ColumnValues::Time(values, bitvecity) => {
                values.push(Time::default());
                bitvecity.push(false);
            }
            ColumnValues::Interval(values, bitvecity) => {
                values.push(Interval::default());
                bitvecity.push(false);
            }
            ColumnValues::Undefined(len) => {
                *len += 1;
            }
            ColumnValues::RowId(values, bitvec) => {
                values.push(RowId::default());
                bitvec.push(false);
            }
            ColumnValues::Uuid4(values, bitvec) => {
                values.push(Uuid4::default());
                bitvec.push(false);
            }
            ColumnValues::Uuid7(values, bitvec) => {
                values.push(Uuid7::default());
                bitvec.push(false);
            }
            ColumnValues::Blob(values, bitvec) => {
                values.push(Blob::new(vec![]));
                bitvec.push(false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::ColumnValues;

    #[test]
    fn test_bool() {
        let mut col = ColumnValues::bool(vec![true]);
        col.push_undefined();
        if let ColumnValues::Bool(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[true, false]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_float4() {
        let mut col = ColumnValues::float4(vec![1.0]);
        col.push_undefined();
        if let ColumnValues::Float4(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1.0, 0.0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_float8() {
        let mut col = ColumnValues::float8(vec![1.0]);
        col.push_undefined();
        if let ColumnValues::Float8(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1.0, 0.0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_int1() {
        let mut col = ColumnValues::int1(vec![1]);
        col.push_undefined();
        if let ColumnValues::Int1(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_int2() {
        let mut col = ColumnValues::int2(vec![1]);
        col.push_undefined();
        if let ColumnValues::Int2(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_int4() {
        let mut col = ColumnValues::int4(vec![1]);
        col.push_undefined();
        if let ColumnValues::Int4(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_int8() {
        let mut col = ColumnValues::int8(vec![1]);
        col.push_undefined();
        if let ColumnValues::Int8(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_int16() {
        let mut col = ColumnValues::int16(vec![1]);
        col.push_undefined();
        if let ColumnValues::Int16(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_string() {
        let mut col = ColumnValues::utf8(vec!["a".to_string()]);
        col.push_undefined();
        if let ColumnValues::Utf8(v, bitvec) = col {
            assert_eq!(v.as_slice(), &["a", ""]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_uint1() {
        let mut col = ColumnValues::uint1(vec![1]);
        col.push_undefined();
        if let ColumnValues::Uint1(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_uint2() {
        let mut col = ColumnValues::uint2(vec![1]);
        col.push_undefined();
        if let ColumnValues::Uint2(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_uint4() {
        let mut col = ColumnValues::uint4(vec![1]);
        col.push_undefined();
        if let ColumnValues::Uint4(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_uint8() {
        let mut col = ColumnValues::uint8(vec![1]);
        col.push_undefined();
        if let ColumnValues::Uint8(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_uint16() {
        let mut col = ColumnValues::uint16(vec![1]);
        col.push_undefined();
        if let ColumnValues::Uint16(v, bitvec) = col {
            assert_eq!(v.as_slice(), &[1, 0]);
            assert!(bitvec.get(0));
            assert!(!bitvec.get(1));
        }
    }

    #[test]
    fn test_undefined() {
        let mut col = ColumnValues::Undefined(5);
        col.push_undefined();
        if let ColumnValues::Undefined(len) = col {
            assert_eq!(len, 6);
        }
    }
}
