// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;

impl EngineColumnData {
    pub fn reorder(&mut self, indices: &[usize]) {
        match self {
            EngineColumnData::Bool(container) => container.reorder(indices),
            EngineColumnData::Float4(container) => container.reorder(indices),
            EngineColumnData::Float8(container) => container.reorder(indices),
            EngineColumnData::Int1(container) => container.reorder(indices),
            EngineColumnData::Int2(container) => container.reorder(indices),
            EngineColumnData::Int4(container) => container.reorder(indices),
            EngineColumnData::Int8(container) => container.reorder(indices),
            EngineColumnData::Int16(container) => container.reorder(indices),
            EngineColumnData::Utf8(container) => container.reorder(indices),
            EngineColumnData::Uint1(container) => container.reorder(indices),
            EngineColumnData::Uint2(container) => container.reorder(indices),
            EngineColumnData::Uint4(container) => container.reorder(indices),
            EngineColumnData::Uint8(container) => container.reorder(indices),
            EngineColumnData::Uint16(container) => container.reorder(indices),
            EngineColumnData::Date(container) => container.reorder(indices),
            EngineColumnData::DateTime(container) => container.reorder(indices),
            EngineColumnData::Time(container) => container.reorder(indices),
            EngineColumnData::Interval(container) => container.reorder(indices),
            EngineColumnData::Undefined(container) => container.reorder(indices),
            EngineColumnData::RowId(container) => container.reorder(indices),
            EngineColumnData::Uuid4(container) => container.reorder(indices),
            EngineColumnData::Uuid7(container) => container.reorder(indices),
            EngineColumnData::Blob(container) => container.reorder(indices),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::column::EngineColumnData;
    use reifydb_core::Value;

    #[test]
    fn test_reorder_bool() {
        let mut col = EngineColumnData::bool([true, false, true]);
        col.reorder(&[2, 0, 1]);

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), Value::Bool(true));
        assert_eq!(col.get_value(1), Value::Bool(true));
        assert_eq!(col.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_reorder_float4() {
        let mut col = EngineColumnData::float4([1.0, 2.0, 3.0]);
        col.reorder(&[2, 0, 1]);

        assert_eq!(col.len(), 3);
        // Check values after reordering
        match col.get_value(0) {
            Value::Float4(v) => assert_eq!(v.value(), 3.0),
            _ => panic!("Expected Float4"),
        }
        match col.get_value(1) {
            Value::Float4(v) => assert_eq!(v.value(), 1.0),
            _ => panic!("Expected Float4"),
        }
        match col.get_value(2) {
            Value::Float4(v) => assert_eq!(v.value(), 2.0),
            _ => panic!("Expected Float4"),
        }
    }

    #[test]
    fn test_reorder_int4() {
        let mut col = EngineColumnData::int4([1, 2, 3]);
        col.reorder(&[2, 0, 1]);

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), Value::Int4(3));
        assert_eq!(col.get_value(1), Value::Int4(1));
        assert_eq!(col.get_value(2), Value::Int4(2));
    }

    #[test]
    fn test_reorder_string() {
        let mut col = EngineColumnData::utf8(["a".to_string(), "b".to_string(), "c".to_string()]);
        col.reorder(&[2, 0, 1]);

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), Value::Utf8("c".to_string()));
        assert_eq!(col.get_value(1), Value::Utf8("a".to_string()));
        assert_eq!(col.get_value(2), Value::Utf8("b".to_string()));
    }

    #[test]
    fn test_reorder_undefined() {
        let mut col = EngineColumnData::undefined(3);
        col.reorder(&[2, 0, 1]);
        assert_eq!(col.len(), 3);

        col.reorder(&[1, 0]);
        assert_eq!(col.len(), 2);
    }
}
