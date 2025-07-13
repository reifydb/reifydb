// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::num::ordered_float::{OrderedF32, OrderedF64};
use crate::row::{EncodedRow, Layout};
use crate::{DataType, Value};

impl Layout {
    pub fn set_values(&self, row: &mut EncodedRow, values: &[Value]) {
        debug_assert!(values.len() == self.fields.len());
        for (idx, value) in values.iter().enumerate() {
            self.set_value(row, idx, value)
        }
    }

    pub fn set_value(&self, row: &mut EncodedRow, index: usize, val: &Value) {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());

        match (field.value, val) {
            (DataType::Bool, Value::Bool(v)) => self.set_bool(row, index, *v),
            (DataType::Bool, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Float4, Value::Float4(v)) => self.set_f32(row, index, v.value()),
            (DataType::Float4, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Float8, Value::Float8(v)) => self.set_f64(row, index, v.value()),
            (DataType::Float8, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Int1, Value::Int1(v)) => self.set_i8(row, index, *v),
            (DataType::Int1, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Int2, Value::Int2(v)) => self.set_i16(row, index, *v),
            (DataType::Int2, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Int4, Value::Int4(v)) => self.set_i32(row, index, *v),
            (DataType::Int4, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Int8, Value::Int8(v)) => self.set_i64(row, index, *v),
            (DataType::Int8, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Int16, Value::Int16(v)) => self.set_i128(row, index, *v),
            (DataType::Int16, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Utf8, Value::Utf8(v)) => self.set_utf8(row, index, v),
            (DataType::Utf8, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Uint1, Value::Uint1(v)) => self.set_u8(row, index, *v),
            (DataType::Uint1, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Uint2, Value::Uint2(v)) => self.set_u16(row, index, *v),
            (DataType::Uint2, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Uint4, Value::Uint4(v)) => self.set_u32(row, index, *v),
            (DataType::Uint4, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Uint8, Value::Uint8(v)) => self.set_u64(row, index, *v),
            (DataType::Uint8, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Uint16, Value::Uint16(v)) => self.set_u128(row, index, *v),
            (DataType::Uint16, Value::Undefined) => self.set_undefined(row, index),

            (DataType::Undefined, Value::Undefined) => {}
            (_, _) => unreachable!(),
        }
    }

    pub fn get_value(&self, row: &EncodedRow, index: usize) -> Value {
        let field = &self.fields[index];
        match field.value {
            DataType::Bool => Value::Bool(self.get_bool(row, index)),
            DataType::Float4 => OrderedF32::try_from(self.get_f32(row, index))
                .map(Value::Float4)
                .unwrap_or(Value::Undefined),
            DataType::Float8 => OrderedF64::try_from(self.get_f64(row, index))
                .map(Value::Float8)
                .unwrap_or(Value::Undefined),
            DataType::Int1 => Value::Int1(self.get_i8(row, index)),
            DataType::Int2 => Value::Int2(self.get_i16(row, index)),
            DataType::Int4 => Value::Int4(self.get_i32(row, index)),
            DataType::Int8 => Value::Int8(self.get_i64(row, index)),
            DataType::Int16 => Value::Int16(self.get_i128(row, index)),
            DataType::Utf8 => Value::Utf8(self.get_utf8(row, index).to_string()),
            DataType::Uint1 => Value::Uint1(self.get_u8(row, index)),
            DataType::Uint2 => Value::Uint2(self.get_u16(row, index)),
            DataType::Uint4 => Value::Uint4(self.get_u32(row, index)),
            DataType::Uint8 => Value::Uint8(self.get_u64(row, index)),
            DataType::Uint16 => Value::Uint16(self.get_u128(row, index)),
            DataType::Undefined => Value::Undefined,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::num::ordered_float::{OrderedF32, OrderedF64};
    use crate::row::Layout;
    use crate::{DataType, Value};

    #[test]
    fn test_set_value_utf8_with_dynamic_content() {
        let layout = Layout::new(&[DataType::Utf8, DataType::Int4, DataType::Utf8]);
        let mut row = layout.allocate_row();

        let value1 = Value::Utf8("hello".to_string());
        let value2 = Value::Int4(42);
        let value3 = Value::Utf8("world".to_string());

        layout.set_value(&mut row, 0, &value1);
        layout.set_value(&mut row, 1, &value2);
        layout.set_value(&mut row, 2, &value3);

        assert_eq!(layout.get_utf8(&row, 0), "hello");
        assert_eq!(layout.get_i32(&row, 1), 42);
        assert_eq!(layout.get_utf8(&row, 2), "world");
    }

    #[test]
    fn test_set_values_with_mixed_dynamic_content() {
        let layout = Layout::new(&[
            DataType::Bool,
            DataType::Utf8,
            DataType::Float4,
            DataType::Utf8,
            DataType::Int2,
        ]);
        let mut row = layout.allocate_row();

        let values = vec![
            Value::Bool(true),
            Value::Utf8("first_string".to_string()),
            Value::Float4(OrderedF32::try_from(3.14f32).unwrap()),
            Value::Utf8("second_string".to_string()),
            Value::Int2(-100),
        ];

        layout.set_values(&mut row, &values);

        assert_eq!(layout.get_bool(&row, 0), true);
        assert_eq!(layout.get_utf8(&row, 1), "first_string");
        assert_eq!(layout.get_f32(&row, 2), 3.14f32);
        assert_eq!(layout.get_utf8(&row, 3), "second_string");
        assert_eq!(layout.get_i16(&row, 4), -100);
    }

    #[test]
    fn test_set_value_with_empty_and_large_utf8() {
        let layout = Layout::new(&[DataType::Utf8, DataType::Utf8, DataType::Utf8]);
        let mut row = layout.allocate_row();

        let large_string = "X".repeat(2000);
        let values = vec![
            Value::Utf8("".to_string()),
            Value::Utf8(large_string.clone()),
            Value::Utf8("small".to_string()),
        ];

        layout.set_values(&mut row, &values);

        assert_eq!(layout.get_utf8(&row, 0), "");
        assert_eq!(layout.get_utf8(&row, 1), large_string);
        assert_eq!(layout.get_utf8(&row, 2), "small");
        assert_eq!(layout.dynamic_section_size(&row), 2005); // 0 + 2000 + 5
    }

    #[test]
    fn test_get_value_from_dynamic_content() {
        let layout = Layout::new(&[DataType::Utf8, DataType::Int8, DataType::Utf8]);
        let mut row = layout.allocate_row();

        layout.set_utf8(&mut row, 0, "test_string");
        layout.set_i64(&mut row, 1, 9876543210i64);
        layout.set_utf8(&mut row, 2, "another_string");

        let value0 = layout.get_value(&row, 0);
        let value1 = layout.get_value(&row, 1);
        let value2 = layout.get_value(&row, 2);

        match value0 {
            Value::Utf8(s) => assert_eq!(s, "test_string"),
            _ => panic!("Expected UTF8 value"),
        }

        match value1 {
            Value::Int8(i) => assert_eq!(i, 9876543210),
            _ => panic!("Expected Int8 value"),
        }

        match value2 {
            Value::Utf8(s) => assert_eq!(s, "another_string"),
            _ => panic!("Expected UTF8 value"),
        }
    }

    #[test]
    fn test_set_value_undefined_with_utf8_fields() {
        let layout = Layout::new(&[DataType::Utf8, DataType::Bool, DataType::Utf8]);
        let mut row = layout.allocate_row();

        // Set some values
        layout.set_value(&mut row, 0, &Value::Utf8("hello".to_string()));
        layout.set_value(&mut row, 1, &Value::Bool(true));
        layout.set_value(&mut row, 2, &Value::Utf8("world".to_string()));

        assert!(row.is_defined(0));
        assert!(row.is_defined(1));
        assert!(row.is_defined(2));

        // Set some as undefined
        layout.set_value(&mut row, 0, &Value::Undefined);
        layout.set_value(&mut row, 2, &Value::Undefined);

        assert!(!row.is_defined(0));
        assert!(row.is_defined(1));
        assert!(!row.is_defined(2));

        assert_eq!(layout.get_bool(&row, 1), true);
    }

    #[test]
    fn test_get_value_all_types_including_utf8() {
        let layout = Layout::new(&[
            DataType::Bool,
            DataType::Int1,
            DataType::Int2,
            DataType::Int4,
            DataType::Int8,
            DataType::Uint1,
            DataType::Uint2,
            DataType::Uint4,
            DataType::Uint8,
            DataType::Float4,
            DataType::Float8,
            DataType::Utf8,
        ]);
        let mut row = layout.allocate_row();

        layout.set_bool(&mut row, 0, true);
        layout.set_i8(&mut row, 1, -42);
        layout.set_i16(&mut row, 2, -1000i16);
        layout.set_i32(&mut row, 3, -50000i32);
        layout.set_i64(&mut row, 4, -3000000000i64);
        layout.set_u8(&mut row, 5, 200u8);
        layout.set_u16(&mut row, 6, 50000u16);
        layout.set_u32(&mut row, 7, 3000000000u32);
        layout.set_u64(&mut row, 8, 15000000000000000000u64);
        layout.set_f32(&mut row, 9, 2.5);
        layout.set_f64(&mut row, 10, 123.456789);
        layout.set_utf8(&mut row, 11, "dynamic_string");

        let values: Vec<Value> = (0..12).map(|i| layout.get_value(&row, i)).collect();

        assert_eq!(values[0], Value::Bool(true));
        assert_eq!(values[1], Value::Int1(-42));
        assert_eq!(values[2], Value::Int2(-1000));
        assert_eq!(values[3], Value::Int4(-50000));
        assert_eq!(values[4], Value::Int8(-3000000000));
        assert_eq!(values[5], Value::Uint1(200));
        assert_eq!(values[6], Value::Uint2(50000));
        assert_eq!(values[7], Value::Uint4(3000000000));
        assert_eq!(values[8], Value::Uint8(15000000000000000000));
        assert_eq!(values[9], Value::Float4(OrderedF32::try_from(2.5f32).unwrap()));
        assert_eq!(values[10], Value::Float8(OrderedF64::try_from(123.456789f64).unwrap()));
        assert_eq!(values[11], Value::Utf8("dynamic_string".to_string()));
    }

    #[test]
    fn test_set_values_sparse_with_utf8() {
        let layout = Layout::new(&[DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Utf8]);
        let mut row = layout.allocate_row();

        // Only set some values
        let values = vec![
            Value::Utf8("first".to_string()),
            Value::Undefined,
            Value::Utf8("third".to_string()),
            Value::Undefined,
        ];

        layout.set_values(&mut row, &values);

        assert!(row.is_defined(0));
        assert!(!row.is_defined(1));
        assert!(row.is_defined(2));
        assert!(!row.is_defined(3));

        assert_eq!(layout.get_utf8(&row, 0), "first");
        assert_eq!(layout.get_utf8(&row, 2), "third");
    }

    #[test]
    fn test_set_values_unicode_strings() {
        let layout = Layout::new(&[DataType::Utf8, DataType::Int4, DataType::Utf8]);
        let mut row = layout.allocate_row();

        let values = vec![
            Value::Utf8("🎉🚀✨".to_string()),
            Value::Int4(123),
            Value::Utf8("Hello 世界".to_string()),
        ];

        layout.set_values(&mut row, &values);

        assert_eq!(layout.get_utf8(&row, 0), "🎉🚀✨");
        assert_eq!(layout.get_i32(&row, 1), 123);
        assert_eq!(layout.get_utf8(&row, 2), "Hello 世界");
    }

    #[test]
    fn test_static_fields_only_no_dynamic_with_values() {
        let layout = Layout::new(&[DataType::Bool, DataType::Int4, DataType::Float8]);
        let mut row = layout.allocate_row();

        let values = vec![
            Value::Bool(false),
            Value::Int4(999),
            Value::Float8(OrderedF64::try_from(std::f64::consts::E).unwrap()),
        ];

        layout.set_values(&mut row, &values);

        // Verify no dynamic section
        assert_eq!(layout.dynamic_section_size(&row), 0);
        assert_eq!(row.len(), layout.total_static_size());

        assert_eq!(layout.get_bool(&row, 0), false);
        assert_eq!(layout.get_i32(&row, 1), 999);
        assert_eq!(layout.get_f64(&row, 2), std::f64::consts::E);
    }

    #[test]
    fn test_value_roundtrip_with_dynamic_content() {
        let layout =
            Layout::new(&[DataType::Utf8, DataType::Int2, DataType::Utf8, DataType::Float4]);
        let mut row = layout.allocate_row();

        let original_values = vec![
            Value::Utf8("roundtrip_test".to_string()),
            Value::Int2(32000),
            Value::Utf8("".to_string()),
            Value::Float4(OrderedF32::try_from(1.5f32).unwrap()),
        ];

        // Set values
        layout.set_values(&mut row, &original_values);

        // Get values back
        let retrieved_values: Vec<Value> = (0..4).map(|i| layout.get_value(&row, i)).collect();

        assert_eq!(retrieved_values, original_values);
    }
}
