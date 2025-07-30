// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{ColumnValues, FrameColumn};
use reifydb_core::BitVec;

impl FrameColumn {
    pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
        self.values_mut().filter(mask)
    }
}

impl ColumnValues {
    pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
        match self {
            ColumnValues::Bool(container) => container.filter(mask),
            ColumnValues::Float4(container) => container.filter(mask),
            ColumnValues::Float8(container) => container.filter(mask),
            ColumnValues::Int1(container) => container.filter(mask),
            ColumnValues::Int2(container) => container.filter(mask),
            ColumnValues::Int4(container) => container.filter(mask),
            ColumnValues::Int8(container) => container.filter(mask),
            ColumnValues::Int16(container) => container.filter(mask),
            ColumnValues::Uint1(container) => container.filter(mask),
            ColumnValues::Uint2(container) => container.filter(mask),
            ColumnValues::Uint4(container) => container.filter(mask),
            ColumnValues::Uint8(container) => container.filter(mask),
            ColumnValues::Uint16(container) => container.filter(mask),
            ColumnValues::Utf8(container) => container.filter(mask),
            ColumnValues::Date(container) => container.filter(mask),
            ColumnValues::DateTime(container) => container.filter(mask),
            ColumnValues::Time(container) => container.filter(mask),
            ColumnValues::Interval(container) => container.filter(mask),
            ColumnValues::Undefined(container) => container.filter(mask),
            ColumnValues::RowId(container) => container.filter(mask),
            ColumnValues::Uuid4(container) => container.filter(mask),
            ColumnValues::Uuid7(container) => container.filter(mask),
            ColumnValues::Blob(container) => container.filter(mask),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::column::ColumnValues;
    use reifydb_core::{BitVec, Value};

    #[test]
    fn test_filter_bool() {
        let mut col = ColumnValues::bool([true, false, true, false]);
        let mask = BitVec::from_slice(&[true, false, true, false]);

        col.filter(&mask).unwrap();

        assert_eq!(col.len(), 2);
        assert_eq!(col.get_value(0), Value::Bool(true));
        assert_eq!(col.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_filter_int4() {
        let mut col = ColumnValues::int4([1, 2, 3, 4, 5]);
        let mask = BitVec::from_slice(&[true, false, true, false, true]);

        col.filter(&mask).unwrap();

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), Value::Int4(1));
        assert_eq!(col.get_value(1), Value::Int4(3));
        assert_eq!(col.get_value(2), Value::Int4(5));
    }

    #[test]
    fn test_filter_float4() {
        let mut col = ColumnValues::float4([1.0, 2.0, 3.0, 4.0]);
        let mask = BitVec::from_slice(&[false, true, false, true]);

        col.filter(&mask).unwrap();

        assert_eq!(col.len(), 2);
        match col.get_value(0) {
            Value::Float4(v) => assert_eq!(v.value(), 2.0),
            _ => panic!("Expected Float4"),
        }
        match col.get_value(1) {
            Value::Float4(v) => assert_eq!(v.value(), 4.0),
            _ => panic!("Expected Float4"),
        }
    }

    #[test]
    fn test_filter_string() {
        let mut col = ColumnValues::utf8(["a", "b", "c", "d"]);
        let mask = BitVec::from_slice(&[true, false, false, true]);

        col.filter(&mask).unwrap();

        assert_eq!(col.len(), 2);
        assert_eq!(col.get_value(0), Value::Utf8("a".to_string()));
        assert_eq!(col.get_value(1), Value::Utf8("d".to_string()));
    }

    #[test]
    fn test_filter_undefined() {
        let mut col = ColumnValues::undefined(5);
        let mask = BitVec::from_slice(&[true, false, true, false, false]);

        col.filter(&mask).unwrap();

        assert_eq!(col.len(), 2);
        assert_eq!(col.get_value(0), Value::Undefined);
        assert_eq!(col.get_value(1), Value::Undefined);
    }

    #[test]
    fn test_filter_empty_mask() {
        let mut col = ColumnValues::int4([1, 2, 3]);
        let mask = BitVec::from_slice(&[false, false, false]);

        col.filter(&mask).unwrap();

        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_filter_all_true_mask() {
        let mut col = ColumnValues::int4([1, 2, 3]);
        let mask = BitVec::from_slice(&[true, true, true]);

        col.filter(&mask).unwrap();

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), Value::Int4(1));
        assert_eq!(col.get_value(1), Value::Int4(2));
        assert_eq!(col.get_value(2), Value::Int4(3));
    }
}
