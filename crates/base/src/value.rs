// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// All possible RQL value types
#[derive(Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum ValueKind {
    /// A boolean: true or false.
    Bool,
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
            Value::Int2(_) => ValueKind::Int2,
            Value::Text(_) => ValueKind::Text,
            Value::Uint2(_) => ValueKind::Uint2,
        }
    }
}

/// A RQL value, represented as a native Rust type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// Value is not defined (think null in common programming languages)
    Undefined,
    /// A boolean: true or false.
    Bool(bool),
    /// A 2-byte signed integer
    Int2(i16),
    /// A UTF-8 encoded text.
    Text(String),
    /// A 2-byte unsigned integer
    Uint2(u16),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Bool(true) => f.write_str("true"),
            Value::Bool(false) => f.write_str("false"),
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
