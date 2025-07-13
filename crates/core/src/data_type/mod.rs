// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Value;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub use get::GetKind;

mod get;
mod promote;

/// All possible RQL data types
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DataType {
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

impl DataType {
    pub fn to_u8(&self) -> u8 {
        match self {
            DataType::Bool => 0x0E,
            DataType::Float4 => 0x01,
            DataType::Float8 => 0x02,
            DataType::Int1 => 0x03,
            DataType::Int2 => 0x04,
            DataType::Int4 => 0x05,
            DataType::Int8 => 0x06,
            DataType::Int16 => 0x07,
            DataType::Utf8 => 0x08,
            DataType::Uint1 => 0x09,
            DataType::Uint2 => 0x0A,
            DataType::Uint4 => 0x0B,
            DataType::Uint8 => 0x0C,
            DataType::Uint16 => 0x0D,
            DataType::Undefined => 0x00,
        }
    }
}

impl DataType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0x00 => DataType::Undefined,
            0x01 => DataType::Float4,
            0x02 => DataType::Float8,
            0x03 => DataType::Int1,
            0x04 => DataType::Int2,
            0x05 => DataType::Int4,
            0x06 => DataType::Int8,
            0x07 => DataType::Int16,
            0x08 => DataType::Utf8,
            0x09 => DataType::Uint1,
            0x0A => DataType::Uint2,
            0x0B => DataType::Uint4,
            0x0C => DataType::Uint8,
            0x0D => DataType::Uint16,
            0x0E => DataType::Bool,
            _ => unreachable!(),
        }
    }
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Bool => 1,
            DataType::Float4 => 4,
            DataType::Float8 => 8,
            DataType::Int1 => 1,
            DataType::Int2 => 2,
            DataType::Int4 => 4,
            DataType::Int8 => 8,
            DataType::Int16 => 16,
            DataType::Utf8 => 255,
            DataType::Uint1 => 1,
            DataType::Uint2 => 2,
            DataType::Uint4 => 4,
            DataType::Uint8 => 8,
            DataType::Uint16 => 16,
            DataType::Undefined => 0,
        }
    }

    pub fn alignment(&self) -> usize {
        match self {
            DataType::Bool => 1,
            DataType::Float4 => 4,
            DataType::Float8 => 8,
            DataType::Int1 => 1,
            DataType::Int2 => 2,
            DataType::Int4 => 4,
            DataType::Int8 => 8,
            DataType::Int16 => 16,
            DataType::Utf8 => 1,
            DataType::Uint1 => 1,
            DataType::Uint2 => 2,
            DataType::Uint4 => 4,
            DataType::Uint8 => 8,
            DataType::Uint16 => 16,
            DataType::Undefined => 0,
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Bool => f.write_str("BOOL"),
            DataType::Float4 => f.write_str("FLOAT4"),
            DataType::Float8 => f.write_str("FLOAT8"),
            DataType::Int1 => f.write_str("INT1"),
            DataType::Int2 => f.write_str("INT2"),
            DataType::Int4 => f.write_str("INT4"),
            DataType::Int8 => f.write_str("INT8"),
            DataType::Int16 => f.write_str("INT16"),
            DataType::Utf8 => f.write_str("STRING"),
            DataType::Uint1 => f.write_str("UINT1"),
            DataType::Uint2 => f.write_str("UINT2"),
            DataType::Uint4 => f.write_str("UINT4"),
            DataType::Uint8 => f.write_str("UINT8"),
            DataType::Uint16 => f.write_str("UINT16"),
            DataType::Undefined => f.write_str("UNDEFINED"),
        }
    }
}

impl From<&Value> for DataType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Undefined => DataType::Undefined,
            Value::Bool(_) => DataType::Bool,
            Value::Float4(_) => DataType::Float4,
            Value::Float8(_) => DataType::Float8,
            Value::Int1(_) => DataType::Int1,
            Value::Int2(_) => DataType::Int2,
            Value::Int4(_) => DataType::Int4,
            Value::Int8(_) => DataType::Int8,
            Value::Int16(_) => DataType::Int16,
            Value::Utf8(_) => DataType::Utf8,
            Value::Uint1(_) => DataType::Uint1,
            Value::Uint2(_) => DataType::Uint2,
            Value::Uint4(_) => DataType::Uint4,
            Value::Uint8(_) => DataType::Uint8,
            Value::Uint16(_) => DataType::Uint16,
        }
    }
}
