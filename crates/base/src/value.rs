// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// All possible RQL value types
#[derive(Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    /// A boolean: true or false.
    Boolean,
    /// A 2-byte signed integer
    Int2,
    /// A UTF-8 encoded text.
    Text,
    /// A 2-byte unsigned integer
    Uint2,
    /// Value is not defined (think null in common programming languages)
    Undefined,
}

impl Display for ValueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Boolean => f.write_str("BOOLEAN"),
            ValueType::Int2 => f.write_str("INT2"),
            ValueType::Uint2 => f.write_str("UINT2"),
            ValueType::Text => f.write_str("TEXT"),
            ValueType::Undefined => f.write_str("UNDEFINED"),
        }
    }
}

/// A RQL value, represented as a native Rust type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// Value is not defined (think null in common programming languages)
    Undefined,
    /// A boolean: true or false.
    Boolean(bool),
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
            Value::Boolean(true) => f.write_str("true"),
            Value::Boolean(false) => f.write_str("false"),
            Value::Int2(value) => Display::fmt(value, f),
            Value::Text(value) => Display::fmt(value, f),
            Value::Uint2(value) => Display::fmt(value, f),
            Value::Undefined => f.write_str("undefined"),
        }
    }
}
