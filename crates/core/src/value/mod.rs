// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod promote;

use crate::ordered_float::{OrderedF32, OrderedF64};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

/// All possible RQL value types
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ValueKind {
    /// A boolean: true or false.
    Bool,
    /// A 4-byte floating point
    Float4,
    /// An 8-byte floating point
    Float8,
    /// A 1-byte signed integer
    Int1,
    /// A 2-byte signed integer
    Int2,
    /// A 4-byte signed integer
    Int4,
    /// An 8-byte signed integer
    Int8,
    /// A 16-byte signed integer
    Int16,
    /// A UTF-8 encoded text. Maximum 255 bytes
    String,
    /// A 1-byte unsigned integer
    Uint1,
    /// A 2-byte unsigned integer
    Uint2,
    /// A 4-byte unsigned integer
    Uint4,
    /// A 8-byte unsigned integer
    Uint8,
    /// A 16-byte unsigned integer
    Uint16,
    /// Value is not defined (think null in common programming languages)
    Undefined,
}

impl Display for ValueKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueKind::Bool => f.write_str("BOOL"),
            ValueKind::Float4 => f.write_str("FLOAT4"),
            ValueKind::Float8 => f.write_str("FLOAT8"),
            ValueKind::Int1 => f.write_str("INT1"),
            ValueKind::Int2 => f.write_str("INT2"),
            ValueKind::Int4 => f.write_str("INT4"),
            ValueKind::Int8 => f.write_str("INT8"),
            ValueKind::Int16 => f.write_str("INT16"),
            ValueKind::String => f.write_str("STRING"),
            ValueKind::Uint1 => f.write_str("UINT1"),
            ValueKind::Uint2 => f.write_str("UINT2"),
            ValueKind::Uint4 => f.write_str("UINT4"),
            ValueKind::Uint8 => f.write_str("UINT8"),
            ValueKind::Uint16 => f.write_str("UINT16"),
            ValueKind::Undefined => f.write_str("UNDEFINED"),
        }
    }
}

impl From<&Value> for ValueKind {
    fn from(value: &Value) -> Self {
        match value {
            Value::Undefined => ValueKind::Undefined,
            Value::Bool(_) => ValueKind::Bool,
            Value::Float4(_) => ValueKind::Float4,
            Value::Float8(_) => ValueKind::Float8,
            Value::Int1(_) => ValueKind::Int1,
            Value::Int2(_) => ValueKind::Int2,
            Value::Int4(_) => ValueKind::Int4,
            Value::Int8(_) => ValueKind::Int8,
            Value::Int16(_) => ValueKind::Int16,
            Value::String(_) => ValueKind::String,
            Value::Uint1(_) => ValueKind::Uint1,
            Value::Uint2(_) => ValueKind::Uint2,
            Value::Uint4(_) => ValueKind::Uint4,
            Value::Uint8(_) => ValueKind::Uint8,
            Value::Uint16(_) => ValueKind::Uint16,
        }
    }
}

/// A RQL value, represented as a native Rust type.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    /// Value is not defined (think null in common programming languages)
    Undefined,
    /// A boolean: true or false.
    Bool(bool),
    /// A 4-byte floating point
    Float4(OrderedF32),
    /// An 8-byte floating point
    Float8(OrderedF64),
    /// A 1-byte signed integer
    Int1(i8),
    /// A 2-byte signed integer
    Int2(i16),
    /// A 4-byte signed integer
    Int4(i32),
    /// An 8-byte signed integer
    Int8(i64),
    /// A 16-byte signed integer
    Int16(i128),
    /// A UTF-8 encoded text. Maximum 255 bytes
    String(String),
    /// A 1-byte unsigned integer
    Uint1(u8),
    /// A 2-byte unsigned integer
    Uint2(u16),
    /// A 4-byte unsigned integer
    Uint4(u32),
    /// A 8-byte unsigned integer
    Uint8(u64),
    /// A 16-byte unsigned integer
    Uint16(u128),
}

impl Value {
    pub fn float4(v: impl Into<f32>) -> Self {
        OrderedF32::try_from(v.into()).map(Value::Float4).unwrap_or(Value::Undefined)
    }
    pub fn float8(v: impl Into<f64>) -> Self {
        OrderedF64::try_from(v.into()).map(Value::Float8).unwrap_or(Value::Undefined)
    }
}

impl Value {
    pub fn negate(&self) -> Self {
        match self {
            Value::Float4(f) => {
                OrderedF32::try_from(-f.value()).map(Value::Float4).unwrap_or(Value::Undefined)
            }
            Value::Float8(f) => {
                OrderedF64::try_from(-f.value()).map(Value::Float8).unwrap_or(Value::Undefined)
            }
            Value::Int1(v) => Value::Int1(-v),
            Value::Int2(v) => Value::Int2(-v),
            Value::Int4(v) => Value::Int4(-v),
            Value::Int8(v) => Value::Int8(-v),
            Value::Int16(v) => Value::Int16(-v),
            Value::Undefined => Value::Undefined,
            Value::Bool(_) => Value::Undefined,
            Value::String(_) => Value::Undefined,
            Value::Uint1(_)
            | Value::Uint2(_)
            | Value::Uint4(_)
            | Value::Uint8(_)
            | Value::Uint16(_) => Value::Undefined,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Int2(a), Value::Int2(b)) => a.partial_cmp(b),
            _ => unimplemented!(),
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Int2(a), Value::Int2(b)) => a.cmp(b),
            _ => unimplemented!(),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Bool(true) => f.write_str("true"),
            Value::Bool(false) => f.write_str("false"),
            Value::Float4(value) => Display::fmt(value, f),
            Value::Float8(value) => Display::fmt(value, f),
            Value::Int1(value) => Display::fmt(value, f),
            Value::Int2(value) => Display::fmt(value, f),
            Value::Int4(value) => Display::fmt(value, f),
            Value::Int8(value) => Display::fmt(value, f),
            Value::Int16(value) => Display::fmt(value, f),
            Value::String(value) => Display::fmt(value, f),
            Value::Uint1(value) => Display::fmt(value, f),
            Value::Uint2(value) => Display::fmt(value, f),
            Value::Uint4(value) => Display::fmt(value, f),
            Value::Uint8(value) => Display::fmt(value, f),
            Value::Uint16(value) => Display::fmt(value, f),
            Value::Undefined => f.write_str("undefined"),
        }
    }
}

impl Value {
    pub fn kind(&self) -> ValueKind {
        match self {
            Value::Undefined => ValueKind::Undefined,
            Value::Bool(_) => ValueKind::Bool,
            Value::Float4(_) => ValueKind::Float4,
            Value::Float8(_) => ValueKind::Float8,
            Value::Int1(_) => ValueKind::Int1,
            Value::Int2(_) => ValueKind::Int2,
            Value::Int4(_) => ValueKind::Int4,
            Value::Int8(_) => ValueKind::Int8,
            Value::Int16(_) => ValueKind::Int16,
            Value::String(_) => ValueKind::String,
            Value::Uint1(_) => ValueKind::Uint1,
            Value::Uint2(_) => ValueKind::Uint2,
            Value::Uint4(_) => ValueKind::Uint4,
            Value::Uint8(_) => ValueKind::Uint8,
            Value::Uint16(_) => ValueKind::Uint16,
        }
    }
}
