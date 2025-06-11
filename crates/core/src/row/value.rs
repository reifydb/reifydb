// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ordered_float::{OrderedF32, OrderedF64};
use crate::row::{Layout, Row};
use crate::{Value, ValueKind};

impl Layout {
    pub fn set_values(&self, row: &mut Row, values: &[Value]) {
        debug_assert!(values.len() == self.fields.len());
        for (idx, value) in values.iter().enumerate() {
            self.set_value(row, idx, value)
        }
    }

    pub fn set_value(&self, row: &mut Row, index: usize, val: &Value) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);

        match (field.value, val) {
            (ValueKind::Bool, Value::Bool(v)) => self.set_bool(row, index, *v),
            (ValueKind::Bool, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Float4, Value::Float4(v)) => self.set_f32(row, index, v.value()),
            (ValueKind::Float4, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Float8, Value::Float8(v)) => self.set_f64(row, index, v.value()),
            (ValueKind::Float8, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Int1, Value::Int1(v)) => self.set_i8(row, index, *v),
            (ValueKind::Int1, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Int2, Value::Int2(v)) => self.set_i16(row, index, *v),
            (ValueKind::Int2, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Int4, Value::Int4(v)) => self.set_i32(row, index, *v),
            (ValueKind::Int4, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Int8, Value::Int8(v)) => self.set_i64(row, index, *v),
            (ValueKind::Int8, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Int16, Value::Int16(v)) => self.set_i128(row, index, *v),
            (ValueKind::Int16, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::String, Value::String(v)) => self.set_str(row, index, v),
            (ValueKind::String, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Uint1, Value::Uint1(v)) => self.set_u8(row, index, *v),
            (ValueKind::Uint1, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Uint2, Value::Uint2(v)) => self.set_u16(row, index, *v),
            (ValueKind::Uint2, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Uint4, Value::Uint4(v)) => self.set_u32(row, index, *v),
            (ValueKind::Uint4, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Uint8, Value::Uint8(v)) => self.set_u64(row, index, *v),
            (ValueKind::Uint8, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Uint16, Value::Uint16(v)) => self.set_u128(row, index, *v),
            (ValueKind::Uint16, Value::Undefined) => self.set_undefined(row, index),

            (ValueKind::Undefined, Value::Undefined) => {}
            (_, _) => unreachable!(),
        }
    }

    pub fn get_value(&self, row: &Row, index: usize) -> Value {
        let field = &self.fields[index];
        unsafe {
            let src = row.as_ptr().add(field.offset);
            match field.value {
                ValueKind::Bool => Value::Bool(self.get_bool(row, index)),
                ValueKind::Float4 => OrderedF32::try_from(self.get_f32(row, index))
                    .map(Value::Float4)
                    .unwrap_or(Value::Undefined),
                ValueKind::Float8 => OrderedF64::try_from(self.get_f64(row, index))
                    .map(Value::Float8)
                    .unwrap_or(Value::Undefined),
                ValueKind::Int1 => Value::Int1(self.get_i8(row, index)),
                ValueKind::Int2 => Value::Int2(self.get_i16(row, index)),
                ValueKind::Int4 => Value::Int4(self.get_i32(row, index)),
                ValueKind::Int8 => Value::Int8(self.get_i64(row, index)),
                ValueKind::Int16 => Value::Int16(self.get_i128(row, index)),
                ValueKind::String => Value::String(self.get_str(row, index).to_string()),
                ValueKind::Uint1 => Value::Uint1(self.get_u8(row, index)),
                ValueKind::Uint2 => Value::Uint2(self.get_u16(row, index)),
                ValueKind::Uint4 => Value::Uint4(self.get_u32(row, index)),
                ValueKind::Uint8 => Value::Uint8(self.get_u64(row, index)),
                ValueKind::Uint16 => Value::Uint16(self.get_u128(row, index)),
                ValueKind::Undefined => Value::Undefined,
                _ => unimplemented!(),
            }
        }
    }
}
