// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use reifydb_core::Value;
use reifydb_core::num::ordered_float::{OrderedF32, OrderedF64};

impl ColumnValues {
    pub fn get(&self, index: usize) -> Value {
        match self {
            ColumnValues::Bool(v, b) => {
                if b[index] {
                    Value::Bool(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Float4(v, b) => {
                if b[index] {
                    OrderedF32::try_from(v[index]).map(Value::Float4).unwrap_or(Value::Undefined)
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Float8(v, b) => {
                if b[index] {
                    OrderedF64::try_from(v[index]).map(Value::Float8).unwrap_or(Value::Undefined)
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int1(v, b) => {
                if b[index] {
                    Value::Int1(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int2(v, b) => {
                if b[index] {
                    Value::Int2(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int4(v, b) => {
                if b[index] {
                    Value::Int4(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int8(v, b) => {
                if b[index] {
                    Value::Int8(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int16(v, b) => {
                if b[index] {
                    Value::Int16(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Utf8(v, b) => {
                if b[index] {
                    Value::Utf8(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint1(v, b) => {
                if b[index] {
                    Value::Uint1(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint2(v, b) => {
                if b[index] {
                    Value::Uint2(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint4(v, b) => {
                if b[index] {
                    Value::Uint4(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint8(v, b) => {
                if b[index] {
                    Value::Uint8(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint16(v, b) => {
                if b[index] {
                    Value::Uint16(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Date(v, b) => {
                if b[index] {
                    Value::Date(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::DateTime(v, b) => {
                if b[index] {
                    Value::DateTime(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Time(v, b) => {
                if b[index] {
                    Value::Time(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Interval(v, b) => {
                if b[index] {
                    Value::Interval(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Undefined(_) => Value::Undefined,
        }
    }
}
