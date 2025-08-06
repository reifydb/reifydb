// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Value;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

mod get;
mod promote;

pub use get::GetType;

/// All possible RQL data types
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Type {
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
    /// A date value (year, month, day)
    Date,
    /// A date and time value with nanosecond precision in UTC
    DateTime,
    /// A time value (hour, minute, second, nanosecond)
    Time,
    /// An interval representing a duration
    Interval,
    /// A row identifier (8-byte unsigned integer)
    RowId,
    /// A UUID version 4 (random)
    Uuid4,
    /// A UUID version 7 (timestamp-based)
    Uuid7,
    /// A binary large object (BLOB)
    Blob,
    /// Value is not defined (think null in common programming languages)
    Undefined,
}

impl Type {
    pub fn is_number(&self) -> bool {
        matches!(
            self,
            Type::Float4
                | Type::Float8
                | Type::Int1
                | Type::Int2
                | Type::Int4
                | Type::Int8
                | Type::Int16
                | Type::Uint1
                | Type::Uint2
                | Type::Uint4
                | Type::Uint8
                | Type::Uint16
        )
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, Type::Bool)
    }

    pub fn is_signed_integer(&self) -> bool {
        matches!(self, Type::Int1 | Type::Int2 | Type::Int4 | Type::Int8 | Type::Int16)
    }

    pub fn is_unsigned_integer(&self) -> bool {
        matches!(self, Type::Uint1 | Type::Uint2 | Type::Uint4 | Type::Uint8 | Type::Uint16)
    }

    pub fn is_integer(&self) -> bool {
        self.is_signed_integer() || self.is_unsigned_integer()
    }

    pub fn is_floating_point(&self) -> bool {
        matches!(self, Type::Float4 | Type::Float8)
    }

    pub fn is_utf8(&self) -> bool {
        matches!(self, Type::Utf8)
    }

    pub fn is_temporal(&self) -> bool {
        matches!(self, Type::Date | Type::DateTime | Type::Time | Type::Interval)
    }

    pub fn is_uuid(&self) -> bool {
        matches!(self, Type::Uuid4 | Type::Uuid7)
    }

    pub fn is_blob(&self) -> bool {
        matches!(self, Type::Blob)
    }
}

impl Type {
    pub fn to_u8(&self) -> u8 {
        match self {
            Type::Bool => 0x0E,
            Type::Float4 => 0x01,
            Type::Float8 => 0x02,
            Type::Int1 => 0x03,
            Type::Int2 => 0x04,
            Type::Int4 => 0x05,
            Type::Int8 => 0x06,
            Type::Int16 => 0x07,
            Type::Utf8 => 0x08,
            Type::Uint1 => 0x09,
            Type::Uint2 => 0x0A,
            Type::Uint4 => 0x0B,
            Type::Uint8 => 0x0C,
            Type::Uint16 => 0x0D,
            Type::Date => 0x0F,
            Type::DateTime => 0x10,
            Type::Time => 0x11,
            Type::Interval => 0x12,
            Type::RowId => 0x13,
            Type::Uuid4 => 0x14,
            Type::Uuid7 => 0x15,
            Type::Blob => 0x16,
            Type::Undefined => 0x00,
        }
    }
}

impl Type {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0x00 => Type::Undefined,
            0x01 => Type::Float4,
            0x02 => Type::Float8,
            0x03 => Type::Int1,
            0x04 => Type::Int2,
            0x05 => Type::Int4,
            0x06 => Type::Int8,
            0x07 => Type::Int16,
            0x08 => Type::Utf8,
            0x09 => Type::Uint1,
            0x0A => Type::Uint2,
            0x0B => Type::Uint4,
            0x0C => Type::Uint8,
            0x0D => Type::Uint16,
            0x0E => Type::Bool,
            0x0F => Type::Date,
            0x10 => Type::DateTime,
            0x11 => Type::Time,
            0x12 => Type::Interval,
            0x13 => Type::RowId,
            0x14 => Type::Uuid4,
            0x15 => Type::Uuid7,
            0x16 => Type::Blob,
            _ => unreachable!(),
        }
    }
}

impl Type {
    pub fn size(&self) -> usize {
        match self {
            Type::Bool => 1,
            Type::Float4 => 4,
            Type::Float8 => 8,
            Type::Int1 => 1,
            Type::Int2 => 2,
            Type::Int4 => 4,
            Type::Int8 => 8,
            Type::Int16 => 16,
            Type::Utf8 => 8, // offset: u32 + length: u32
            Type::Uint1 => 1,
            Type::Uint2 => 2,
            Type::Uint4 => 4,
            Type::Uint8 => 8,
            Type::Uint16 => 16,
            Type::Date => 4,
            Type::DateTime => 12, // seconds: i64 + nanos: u32
            Type::Time => 8,
            Type::Interval => 16, // months: i32 + days: i32 + nanos: i64
            Type::RowId => 8,
            Type::Uuid4 => 16,
            Type::Uuid7 => 16,
            Type::Blob => 8, // offset: u32 + length: u32
            Type::Undefined => 0,
        }
    }

    pub fn alignment(&self) -> usize {
        match self {
            Type::Bool => 1,
            Type::Float4 => 4,
            Type::Float8 => 8,
            Type::Int1 => 1,
            Type::Int2 => 2,
            Type::Int4 => 4,
            Type::Int8 => 8,
            Type::Int16 => 16,
            Type::Utf8 => 4, // u32 alignment
            Type::Uint1 => 1,
            Type::Uint2 => 2,
            Type::Uint4 => 4,
            Type::Uint8 => 8,
            Type::Uint16 => 16,
            Type::Date => 4,
            Type::DateTime => 8,
            Type::Time => 8,
            Type::Interval => 8,
            Type::RowId => 8,
            Type::Uuid4 => 8,
            Type::Uuid7 => 8,
            Type::Blob => 4, // u32 alignment
            Type::Undefined => 0,
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Bool => f.write_str("Bool"),
            Type::Float4 => f.write_str("Float4"),
            Type::Float8 => f.write_str("Float8"),
            Type::Int1 => f.write_str("Int1"),
            Type::Int2 => f.write_str("Int2"),
            Type::Int4 => f.write_str("Int4"),
            Type::Int8 => f.write_str("Int8"),
            Type::Int16 => f.write_str("Int16"),
            Type::Utf8 => f.write_str("Utf8"),
            Type::Uint1 => f.write_str("Uint1"),
            Type::Uint2 => f.write_str("Uint2"),
            Type::Uint4 => f.write_str("Uint4"),
            Type::Uint8 => f.write_str("Uint8"),
            Type::Uint16 => f.write_str("Uint16"),
            Type::Date => f.write_str("Date"),
            Type::DateTime => f.write_str("DateTime"),
            Type::Time => f.write_str("Time"),
            Type::Interval => f.write_str("Interval"),
            Type::RowId => f.write_str("RowId"),
            Type::Uuid4 => f.write_str("Uuid4"),
            Type::Uuid7 => f.write_str("Uuid7"),
            Type::Blob => f.write_str("Blob"),
            Type::Undefined => f.write_str("Undefined"),
        }
    }
}

impl From<&Value> for Type {
    fn from(value: &Value) -> Self {
        match value {
            Value::Undefined => Type::Undefined,
            Value::Bool(_) => Type::Bool,
            Value::Float4(_) => Type::Float4,
            Value::Float8(_) => Type::Float8,
            Value::Int1(_) => Type::Int1,
            Value::Int2(_) => Type::Int2,
            Value::Int4(_) => Type::Int4,
            Value::Int8(_) => Type::Int8,
            Value::Int16(_) => Type::Int16,
            Value::Utf8(_) => Type::Utf8,
            Value::Uint1(_) => Type::Uint1,
            Value::Uint2(_) => Type::Uint2,
            Value::Uint4(_) => Type::Uint4,
            Value::Uint8(_) => Type::Uint8,
            Value::Uint16(_) => Type::Uint16,
            Value::Date(_) => Type::Date,
            Value::DateTime(_) => Type::DateTime,
            Value::Time(_) => Type::Time,
            Value::Interval(_) => Type::Interval,
            Value::RowId(_) => Type::RowId,
            Value::Uuid4(_) => Type::Uuid4,
            Value::Uuid7(_) => Type::Uuid7,
            Value::Blob(_) => Type::Blob,
        }
    }
}

impl FromStr for Type {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BOOL" => Ok(Type::Bool),
            "FLOAT4" => Ok(Type::Float4),
            "FLOAT8" => Ok(Type::Float8),
            "INT1" => Ok(Type::Int1),
            "INT2" => Ok(Type::Int2),
            "INT4" => Ok(Type::Int4),
            "INT8" => Ok(Type::Int8),
            "INT16" => Ok(Type::Int16),
            "UTF8" | "TEXT" => Ok(Type::Utf8),
            "UINT1" => Ok(Type::Uint1),
            "UINT2" => Ok(Type::Uint2),
            "UINT4" => Ok(Type::Uint4),
            "UINT8" => Ok(Type::Uint8),
            "UINT16" => Ok(Type::Uint16),
            "DATE" => Ok(Type::Date),
            "DATETIME" => Ok(Type::DateTime),
            "TIME" => Ok(Type::Time),
            "INTERVAL" => Ok(Type::Interval),
            "ROWID" => Ok(Type::RowId),
            "UUID4" => Ok(Type::Uuid4),
            "UUID7" => Ok(Type::Uuid7),
            "BLOB" => Ok(Type::Blob),
            "UNDEFINED" => Ok(Type::Undefined),
            _ => Err(()),
        }
    }
}
