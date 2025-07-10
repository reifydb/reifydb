// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Value;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub use get::GetKind;

mod get;
mod promote;

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
    /// A UTF-8 encoded text.
    Utf8,
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
            Kind::Utf8 => 0x08,
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
            0x08 => Kind::Utf8,
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
            Kind::Utf8 => 255,
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
            Kind::Utf8 => 1,
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
            Kind::Utf8 => f.write_str("STRING"),
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
            Value::Utf8(_) => Kind::Utf8,
            Value::Uint1(_) => Kind::Uint1,
            Value::Uint2(_) => Kind::Uint2,
            Value::Uint4(_) => Kind::Uint4,
            Value::Uint8(_) => Kind::Uint8,
            Value::Uint16(_) => Kind::Uint16,
        }
    }
}
