// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use column::ColumnValues;

mod column;

use crate::ordered_float::{OrderedF32, OrderedF64};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

/// All possible RQL value types
#[derive(Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum ValueKind {
    /// A boolean: true or false.
    Bool,
    /// A 4-byte floating point
    Float4,
    /// An 8-byte floating point
    Float8,
    /// A 2-byte signed integer
    Int2,
    /// A UTF-8 encoded text.
    Text,
    /// A 2-byte unsigned integer
    Uint2,
    /// Value is not defined (think null in common programming languages)
    Undefined,
}

impl Display for ValueKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueKind::Bool => f.write_str("BOOL"),
            ValueKind::Float4 => f.write_str("FLOAT4"),
            ValueKind::Float8 => f.write_str("FLOAT8"),
            ValueKind::Int2 => f.write_str("INT2"),
            ValueKind::Uint2 => f.write_str("UINT2"),
            ValueKind::Text => f.write_str("TEXT"),
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
            Value::Int2(_) => ValueKind::Int2,
            Value::Text(_) => ValueKind::Text,
            Value::Uint2(_) => ValueKind::Uint2,
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
    /// A 2-byte signed integer
    Int2(i16),
    /// A UTF-8 encoded text.
    Text(String),
    /// A 2-byte unsigned integer
    Uint2(u16),
}

impl Value {
    pub fn float8(v: impl Into<f64>) -> Self {
        OrderedF64::try_from(v.into()).map(Value::Float8).unwrap_or(Value::Undefined)
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
            Value::Int2(value) => Display::fmt(value, f),
            Value::Text(value) => Display::fmt(value, f),
            Value::Uint2(value) => Display::fmt(value, f),
            Value::Undefined => f.write_str("undefined"),
        }
    }
}

impl Value {
    pub fn add(&self, other: Value) -> Value {
        use Value::*;

        match (self, other) {
            (Int2(left), Int2(right)) => Value::Int2(left + right),
            _ => unimplemented!(),
        }
    }
}
