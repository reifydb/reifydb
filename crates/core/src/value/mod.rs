// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

pub mod boolean;
mod date;
mod datetime;
mod interval;
mod is;
pub mod number;
mod ordered_f32;
mod ordered_f64;
pub mod row_id;
pub mod temporal;
mod time;
mod r#type;

pub use date::Date;
pub use datetime::DateTime;
pub use interval::Interval;
pub use is::*;
pub use ordered_f32::OrderedF32;
pub use ordered_f64::OrderedF64;
pub use row_id::RowId;
pub use time::Time;
pub use r#type::{GetType, Type};

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
    Utf8(String),
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
    /// A date value (year, month, day)
    Date(Date),
    /// A date and time value with nanosecond precision in UTC
    DateTime(DateTime),
    /// A time value (hour, minute, second, nanosecond)
    Time(Time),
    /// An interval representing a duration
    Interval(Interval),
    /// A row identifier (8-byte unsigned integer)
    RowId(RowId),
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
            Value::Interval(i) => Value::Interval(i.negate()),
            Value::Undefined => Value::Undefined,
            Value::Bool(_) => Value::Undefined,
            Value::Utf8(_) => Value::Undefined,
            Value::Date(_) => Value::Undefined,
            Value::DateTime(_) => Value::Undefined,
            Value::Time(_) => Value::Undefined,
            Value::Uint1(_)
            | Value::Uint2(_)
            | Value::Uint4(_)
            | Value::Uint8(_)
            | Value::Uint16(_)
            | Value::RowId(_) => Value::Undefined,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r)) => l.partial_cmp(r),
            (Value::Float4(l), Value::Float4(r)) => l.partial_cmp(r),
            (Value::Float8(l), Value::Float8(r)) => l.partial_cmp(r),
            (Value::Int1(l), Value::Int1(r)) => l.partial_cmp(r),
            (Value::Int2(l), Value::Int2(r)) => l.partial_cmp(r),
            (Value::Int4(l), Value::Int4(r)) => l.partial_cmp(r),
            (Value::Int8(l), Value::Int8(r)) => l.partial_cmp(r),
            (Value::Int16(l), Value::Int16(r)) => l.partial_cmp(r),
            (Value::Utf8(l), Value::Utf8(r)) => l.partial_cmp(r),
            (Value::Uint1(l), Value::Uint1(r)) => l.partial_cmp(r),
            (Value::Uint2(l), Value::Uint2(r)) => l.partial_cmp(r),
            (Value::Uint4(l), Value::Uint4(r)) => l.partial_cmp(r),
            (Value::Uint8(l), Value::Uint8(r)) => l.partial_cmp(r),
            (Value::Uint16(l), Value::Uint16(r)) => l.partial_cmp(r),
            (Value::Date(l), Value::Date(r)) => l.partial_cmp(r),
            (Value::DateTime(l), Value::DateTime(r)) => l.partial_cmp(r),
            (Value::Time(l), Value::Time(r)) => l.partial_cmp(r),
            (Value::Interval(l), Value::Interval(r)) => l.partial_cmp(r),
            (Value::RowId(l), Value::RowId(r)) => l.partial_cmp(r),
            _ => unimplemented!(),
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
            (Value::Float4(l), Value::Float4(r)) => l.cmp(r),
            (Value::Float8(l), Value::Float8(r)) => l.cmp(r),
            (Value::Int1(l), Value::Int1(r)) => l.cmp(r),
            (Value::Int2(l), Value::Int2(r)) => l.cmp(r),
            (Value::Int4(l), Value::Int4(r)) => l.cmp(r),
            (Value::Int8(l), Value::Int8(r)) => l.cmp(r),
            (Value::Int16(l), Value::Int16(r)) => l.cmp(r),
            (Value::Utf8(l), Value::Utf8(r)) => l.cmp(r),
            (Value::Uint1(l), Value::Uint1(r)) => l.cmp(r),
            (Value::Uint2(l), Value::Uint2(r)) => l.cmp(r),
            (Value::Uint4(l), Value::Uint4(r)) => l.cmp(r),
            (Value::Uint8(l), Value::Uint8(r)) => l.cmp(r),
            (Value::Uint16(l), Value::Uint16(r)) => l.cmp(r),
            (Value::Date(l), Value::Date(r)) => l.cmp(r),
            (Value::DateTime(l), Value::DateTime(r)) => l.cmp(r),
            (Value::Time(l), Value::Time(r)) => l.cmp(r),
            (Value::Interval(l), Value::Interval(r)) => l.cmp(r),
            (Value::RowId(l), Value::RowId(r)) => l.cmp(r),
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
            Value::Utf8(value) => Display::fmt(value, f),
            Value::Uint1(value) => Display::fmt(value, f),
            Value::Uint2(value) => Display::fmt(value, f),
            Value::Uint4(value) => Display::fmt(value, f),
            Value::Uint8(value) => Display::fmt(value, f),
            Value::Uint16(value) => Display::fmt(value, f),
            Value::Date(value) => Display::fmt(value, f),
            Value::DateTime(value) => Display::fmt(value, f),
            Value::Time(value) => Display::fmt(value, f),
            Value::Interval(value) => Display::fmt(value, f),
            Value::RowId(value) => Display::fmt(value, f),
            Value::Undefined => f.write_str("undefined"),
        }
    }
}

impl Value {
    pub fn ty(&self) -> Type {
        match self {
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
        }
    }
}
