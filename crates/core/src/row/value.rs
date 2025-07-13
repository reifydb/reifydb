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
        debug_assert_eq!(row.len(), self.data_size);

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

            (DataType::Utf8, Value::Utf8(v)) => self.set_str(row, index, v),
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
            DataType::Utf8 => Value::Utf8(self.get_str(row, index).to_string()),
            DataType::Uint1 => Value::Uint1(self.get_u8(row, index)),
            DataType::Uint2 => Value::Uint2(self.get_u16(row, index)),
            DataType::Uint4 => Value::Uint4(self.get_u32(row, index)),
            DataType::Uint8 => Value::Uint8(self.get_u64(row, index)),
            DataType::Uint16 => Value::Uint16(self.get_u128(row, index)),
            DataType::Undefined => Value::Undefined,
        }
    }
}
