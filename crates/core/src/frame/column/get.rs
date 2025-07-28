// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use crate::Value;
use crate::{OrderedF32, OrderedF64};

impl ColumnValues {
    pub fn get(&self, index: usize) -> Value {
        match self {
            ColumnValues::Bool(v, b) => {
                if b.get(index) {
                    Value::Bool(v.get(index))
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Float4(v, b) => {
                if b.get(index) {
                    OrderedF32::try_from(v[index]).map(Value::Float4).unwrap_or(Value::Undefined)
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Float8(v, b) => {
                if b.get(index) {
                    OrderedF64::try_from(v[index]).map(Value::Float8).unwrap_or(Value::Undefined)
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int1(v, b) => {
                if b.get(index) {
                    Value::Int1(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int2(v, b) => {
                if b.get(index) {
                    Value::Int2(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int4(v, b) => {
                if b.get(index) {
                    Value::Int4(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int8(v, b) => {
                if b.get(index) {
                    Value::Int8(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Int16(v, b) => {
                if b.get(index) {
                    Value::Int16(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Utf8(v, b) => {
                if b.get(index) {
                    Value::Utf8(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint1(v, b) => {
                if b.get(index) {
                    Value::Uint1(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint2(v, b) => {
                if b.get(index) {
                    Value::Uint2(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint4(v, b) => {
                if b.get(index) {
                    Value::Uint4(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint8(v, b) => {
                if b.get(index) {
                    Value::Uint8(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uint16(v, b) => {
                if b.get(index) {
                    Value::Uint16(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Date(v, b) => {
                if b.get(index) {
                    Value::Date(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::DateTime(v, b) => {
                if b.get(index) {
                    Value::DateTime(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Time(v, b) => {
                if b.get(index) {
                    Value::Time(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Interval(v, b) => {
                if b.get(index) {
                    Value::Interval(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Undefined(_) => Value::Undefined,
            ColumnValues::RowId(v, b) => {
                if b.get(index) {
                    Value::RowId(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uuid4(v, b) => {
                if b.get(index) {
                    Value::Uuid4(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Uuid7(v, b) => {
                if b.get(index) {
                    Value::Uuid7(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Blob(v, b) => {
                if b.get(index) {
                    Value::Blob(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
        }
    }
}
