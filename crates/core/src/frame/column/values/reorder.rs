// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;

impl ColumnValues {
    pub fn reorder(&mut self, indices: &[usize]) {
        match self {
            ColumnValues::Bool(container) => container.reorder(indices),
            ColumnValues::Float4(container) => container.reorder(indices),
            ColumnValues::Float8(container) => container.reorder(indices),
            ColumnValues::Int1(container) => container.reorder(indices),
            ColumnValues::Int2(container) => container.reorder(indices),
            ColumnValues::Int4(container) => container.reorder(indices),
            ColumnValues::Int8(container) => container.reorder(indices),
            ColumnValues::Int16(container) => container.reorder(indices),
            ColumnValues::Utf8(container) => container.reorder(indices),
            ColumnValues::Uint1(container) => container.reorder(indices),
            ColumnValues::Uint2(container) => container.reorder(indices),
            ColumnValues::Uint4(container) => container.reorder(indices),
            ColumnValues::Uint8(container) => container.reorder(indices),
            ColumnValues::Uint16(container) => container.reorder(indices),
            ColumnValues::Date(container) => container.reorder(indices),
            ColumnValues::DateTime(container) => container.reorder(indices),
            ColumnValues::Time(container) => container.reorder(indices),
            ColumnValues::Interval(container) => container.reorder(indices),
            ColumnValues::Undefined(container) => container.reorder(indices),
            ColumnValues::RowId(container) => container.reorder(indices),
            ColumnValues::Uuid4(container) => container.reorder(indices),
            ColumnValues::Uuid7(container) => container.reorder(indices),
            ColumnValues::Blob(container) => container.reorder(indices),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::column::ColumnValues;

    #[test]
    fn test_reorder_bool() {
        let mut col = ColumnValues::bool([true, false, true]);
        col.reorder(&[2, 0, 1]);

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), crate::Value::Bool(true));
        assert_eq!(col.get_value(1), crate::Value::Bool(true));
        assert_eq!(col.get_value(2), crate::Value::Bool(false));
    }

    #[test]
    fn test_reorder_float4() {
        let mut col = ColumnValues::float4([1.0, 2.0, 3.0]);
        col.reorder(&[2, 0, 1]);

        assert_eq!(col.len(), 3);
        // Check values after reordering
        match col.get_value(0) {
            crate::Value::Float4(v) => assert_eq!(v.value(), 3.0),
            _ => panic!("Expected Float4"),
        }
        match col.get_value(1) {
            crate::Value::Float4(v) => assert_eq!(v.value(), 1.0),
            _ => panic!("Expected Float4"),
        }
        match col.get_value(2) {
            crate::Value::Float4(v) => assert_eq!(v.value(), 2.0),
            _ => panic!("Expected Float4"),
        }
    }

    #[test]
    fn test_reorder_int4() {
        let mut col = ColumnValues::int4([1, 2, 3]);
        col.reorder(&[2, 0, 1]);

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), crate::Value::Int4(3));
        assert_eq!(col.get_value(1), crate::Value::Int4(1));
        assert_eq!(col.get_value(2), crate::Value::Int4(2));
    }

    #[test]
    fn test_reorder_string() {
        let mut col = ColumnValues::utf8(["a".to_string(), "b".to_string(), "c".to_string()]);
        col.reorder(&[2, 0, 1]);

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), crate::Value::Utf8("c".to_string()));
        assert_eq!(col.get_value(1), crate::Value::Utf8("a".to_string()));
        assert_eq!(col.get_value(2), crate::Value::Utf8("b".to_string()));
    }

    #[test]
    fn test_reorder_undefined() {
        let mut col = ColumnValues::undefined(3);
        col.reorder(&[2, 0, 1]);
        assert_eq!(col.len(), 3);

        col.reorder(&[1, 0]);
        assert_eq!(col.len(), 2);
    }
}
