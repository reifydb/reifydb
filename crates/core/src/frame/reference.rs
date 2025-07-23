// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{Value, Date, DateTime, Time, Interval, RowId};
use std::collections::HashMap;
use std::sync::Arc;

pub struct RowRef<'df> {
    pub values: Vec<ValueRef<'df>>,
    pub column_index: Arc<Vec<String>>,
    pub columns: &'df HashMap<String, usize>,
}

impl<'df> RowRef<'df> {
    pub fn get(&self, name: &str) -> Option<&ValueRef<'df>> {
        self.columns.get(name).and_then(|&i| self.values.get(i))
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum ValueRef<'a> {
    Bool(&'a bool),
    Float4(&'a f32),
    Float8(&'a f64),
    Int1(&'a i8),
    Int2(&'a i16),
    Int4(&'a i32),
    Int8(&'a i64),
    Int16(&'a i128),
    String(&'a str),
    Uint1(&'a u8),
    Uint2(&'a u16),
    Uint4(&'a u32),
    Uint8(&'a u64),
    Uint16(&'a u128),
    Date(&'a Date),
    DateTime(&'a DateTime),
    Time(&'a Time),
    Interval(&'a Interval),
    RowId(&'a RowId),
    Undefined,
}

impl<'a> ValueRef<'a> {
    pub fn as_value(&self) -> Value {
        match self {
            ValueRef::Bool(b) => Value::Bool(**b),
            ValueRef::Float4(v) => Value::float4(**v),
            ValueRef::Float8(v) => Value::float8(**v),
            ValueRef::Int1(v) => Value::Int1(**v),
            ValueRef::Int2(v) => Value::Int2(**v),
            ValueRef::Int4(v) => Value::Int4(**v),
            ValueRef::Int8(v) => Value::Int8(**v),
            ValueRef::Int16(v) => Value::Int16(**v),
            ValueRef::Uint1(v) => Value::Uint1(**v),
            ValueRef::Uint2(v) => Value::Uint2(**v),
            ValueRef::Uint4(v) => Value::Uint4(**v),
            ValueRef::Uint8(v) => Value::Uint8(**v),
            ValueRef::Uint16(v) => Value::Uint16(**v),
            ValueRef::String(s) => Value::Utf8(s.to_string()),
            ValueRef::Date(v) => Value::Date((*v).clone()),
            ValueRef::DateTime(v) => Value::DateTime((*v).clone()),
            ValueRef::Time(v) => Value::Time((*v).clone()),
            ValueRef::Interval(v) => Value::Interval((*v).clone()),
            ValueRef::RowId(v) => Value::RowId(**v),
            ValueRef::Undefined => Value::Undefined,
        }
    }
}

impl<'a> ToString for ValueRef<'a> {
    fn to_string(&self) -> String {
        match self {
            ValueRef::Bool(v) => v.to_string(),
            ValueRef::Float4(v) => v.to_string(),
            ValueRef::Float8(v) => v.to_string(),
            ValueRef::Int1(v) => v.to_string(),
            ValueRef::Int2(v) => v.to_string(),
            ValueRef::Int4(v) => v.to_string(),
            ValueRef::Int8(v) => v.to_string(),
            ValueRef::Int16(v) => v.to_string(),
            ValueRef::Uint1(v) => v.to_string(),
            ValueRef::Uint2(v) => v.to_string(),
            ValueRef::Uint4(v) => v.to_string(),
            ValueRef::Uint8(v) => v.to_string(),
            ValueRef::Uint16(v) => v.to_string(),
            ValueRef::String(v) => v.to_string(),
            ValueRef::Date(v) => v.to_string(),
            ValueRef::DateTime(v) => v.to_string(),
            ValueRef::Time(v) => v.to_string(),
            ValueRef::Interval(v) => v.to_string(),
            ValueRef::RowId(v) => v.to_string(),
            ValueRef::Undefined => "Undefined".to_string(),
        }
    }
}
