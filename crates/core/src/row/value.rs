// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::num::ordered_float::{OrderedF32, OrderedF64};
use crate::row::{EncodedRow, Layout};
use crate::{Kind, Value};

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
            (Kind::Bool, Value::Bool(v)) => self.set_bool(row, index, *v),
            (Kind::Bool, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Float4, Value::Float4(v)) => self.set_f32(row, index, v.value()),
            (Kind::Float4, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Float8, Value::Float8(v)) => self.set_f64(row, index, v.value()),
            (Kind::Float8, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Int1, Value::Int1(v)) => self.set_i8(row, index, *v),
            (Kind::Int1, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Int2, Value::Int2(v)) => self.set_i16(row, index, *v),
            (Kind::Int2, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Int4, Value::Int4(v)) => self.set_i32(row, index, *v),
            (Kind::Int4, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Int8, Value::Int8(v)) => self.set_i64(row, index, *v),
            (Kind::Int8, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Int16, Value::Int16(v)) => self.set_i128(row, index, *v),
            (Kind::Int16, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Text, Value::String(v)) => self.set_str(row, index, v),
            (Kind::Text, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Uint1, Value::Uint1(v)) => self.set_u8(row, index, *v),
            (Kind::Uint1, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Uint2, Value::Uint2(v)) => self.set_u16(row, index, *v),
            (Kind::Uint2, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Uint4, Value::Uint4(v)) => self.set_u32(row, index, *v),
            (Kind::Uint4, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Uint8, Value::Uint8(v)) => self.set_u64(row, index, *v),
            (Kind::Uint8, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Uint16, Value::Uint16(v)) => self.set_u128(row, index, *v),
            (Kind::Uint16, Value::Undefined) => self.set_undefined(row, index),

            (Kind::Undefined, Value::Undefined) => {}
            (_, _) => unreachable!(),
        }
    }

    pub fn get_value(&self, row: &EncodedRow, index: usize) -> Value {
        let field = &self.fields[index];
        match field.value {
            Kind::Bool => Value::Bool(self.get_bool(row, index)),
            Kind::Float4 => OrderedF32::try_from(self.get_f32(row, index))
                .map(Value::Float4)
                .unwrap_or(Value::Undefined),
            Kind::Float8 => OrderedF64::try_from(self.get_f64(row, index))
                .map(Value::Float8)
                .unwrap_or(Value::Undefined),
            Kind::Int1 => Value::Int1(self.get_i8(row, index)),
            Kind::Int2 => Value::Int2(self.get_i16(row, index)),
            Kind::Int4 => Value::Int4(self.get_i32(row, index)),
            Kind::Int8 => Value::Int8(self.get_i64(row, index)),
            Kind::Int16 => Value::Int16(self.get_i128(row, index)),
            Kind::Text => Value::String(self.get_str(row, index).to_string()),
            Kind::Uint1 => Value::Uint1(self.get_u8(row, index)),
            Kind::Uint2 => Value::Uint2(self.get_u16(row, index)),
            Kind::Uint4 => Value::Uint4(self.get_u32(row, index)),
            Kind::Uint8 => Value::Uint8(self.get_u64(row, index)),
            Kind::Uint16 => Value::Uint16(self.get_u128(row, index)),
            Kind::Undefined => Value::Undefined,
        }
    }
}
