// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod promote;

use crate::ordered_float::{OrderedF32, OrderedF64};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

/// All possible RQL values
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Kind {
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

impl Kind {
    pub fn to_u8(&self) -> u8 {
        match self {
            Kind::Bool => 0x0E,
            Kind::Float4 => 0x01,
            Kind::Float8 => 0x02,
            Kind::Int1 => 0x03,
            Kind::Int2 => 0x04,
            Kind::Int4 => 0x05,
            Kind::Int8 => 0x06,
            Kind::Int16 => 0x07,
            Kind::String => 0x08,
            Kind::Uint1 => 0x09,
            Kind::Uint2 => 0x0A,
            Kind::Uint4 => 0x0B,
            Kind::Uint8 => 0x0C,
            Kind::Uint16 => 0x0D,
            Kind::Undefined => 0x00,
        }
    }
}

impl Kind {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0x00 => Kind::Undefined,
            0x01 => Kind::Float4,
            0x02 => Kind::Float8,
            0x03 => Kind::Int1,
            0x04 => Kind::Int2,
            0x05 => Kind::Int4,
            0x06 => Kind::Int8,
            0x07 => Kind::Int16,
            0x08 => Kind::String,
            0x09 => Kind::Uint1,
            0x0A => Kind::Uint2,
            0x0B => Kind::Uint4,
            0x0C => Kind::Uint8,
            0x0D => Kind::Uint16,
            0x0E => Kind::Bool,
            _ => unreachable!(),
        }
    }
}

impl Kind {
    pub fn size(&self) -> usize {
        match self {
            Kind::Bool => 1,
            Kind::Float4 => 4,
            Kind::Float8 => 8,
            Kind::Int1 => 1,
            Kind::Int2 => 2,
            Kind::Int4 => 4,
            Kind::Int8 => 8,
            Kind::Int16 => 16,
            Kind::String => 255,
            Kind::Uint1 => 1,
            Kind::Uint2 => 2,
            Kind::Uint4 => 4,
            Kind::Uint8 => 8,
            Kind::Uint16 => 16,
            Kind::Undefined => 0,
        }
    }

    pub fn alignment(&self) -> usize {
        match self {
            Kind::Bool => 1,
            Kind::Float4 => 4,
            Kind::Float8 => 8,
            Kind::Int1 => 1,
            Kind::Int2 => 2,
            Kind::Int4 => 4,
            Kind::Int8 => 8,
            Kind::Int16 => 16,
            Kind::String => 1,
            Kind::Uint1 => 1,
            Kind::Uint2 => 2,
            Kind::Uint4 => 4,
            Kind::Uint8 => 8,
            Kind::Uint16 => 16,
            Kind::Undefined => 0,
        }
    }
}

impl Display for Kind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Kind::Bool => f.write_str("BOOL"),
            Kind::Float4 => f.write_str("FLOAT4"),
            Kind::Float8 => f.write_str("FLOAT8"),
            Kind::Int1 => f.write_str("INT1"),
            Kind::Int2 => f.write_str("INT2"),
            Kind::Int4 => f.write_str("INT4"),
            Kind::Int8 => f.write_str("INT8"),
            Kind::Int16 => f.write_str("INT16"),
            Kind::String => f.write_str("STRING"),
            Kind::Uint1 => f.write_str("UINT1"),
            Kind::Uint2 => f.write_str("UINT2"),
            Kind::Uint4 => f.write_str("UINT4"),
            Kind::Uint8 => f.write_str("UINT8"),
            Kind::Uint16 => f.write_str("UINT16"),
            Kind::Undefined => f.write_str("UNDEFINED"),
        }
    }
}

impl From<&Value> for Kind {
    fn from(value: &Value) -> Self {
        match value {
            Value::Undefined => Kind::Undefined,
            Value::Bool(_) => Kind::Bool,
            Value::Float4(_) => Kind::Float4,
            Value::Float8(_) => Kind::Float8,
            Value::Int1(_) => Kind::Int1,
            Value::Int2(_) => Kind::Int2,
            Value::Int4(_) => Kind::Int4,
            Value::Int8(_) => Kind::Int8,
            Value::Int16(_) => Kind::Int16,
            Value::String(_) => Kind::String,
            Value::Uint1(_) => Kind::Uint1,
            Value::Uint2(_) => Kind::Uint2,
            Value::Uint4(_) => Kind::Uint4,
            Value::Uint8(_) => Kind::Uint8,
            Value::Uint16(_) => Kind::Uint16,
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
    pub fn kind(&self) -> Kind {
        match self {
            Value::Undefined => Kind::Undefined,
            Value::Bool(_) => Kind::Bool,
            Value::Float4(_) => Kind::Float4,
            Value::Float8(_) => Kind::Float8,
            Value::Int1(_) => Kind::Int1,
            Value::Int2(_) => Kind::Int2,
            Value::Int4(_) => Kind::Int4,
            Value::Int8(_) => Kind::Int8,
            Value::Int16(_) => Kind::Int16,
            Value::String(_) => Kind::String,
            Value::Uint1(_) => Kind::Uint1,
            Value::Uint2(_) => Kind::Uint2,
            Value::Uint4(_) => Kind::Uint4,
            Value::Uint8(_) => Kind::Uint8,
            Value::Uint16(_) => Kind::Uint16,
        }
    }
}
