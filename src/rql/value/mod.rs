// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use value::*;

mod value;

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// All possible RQL value types
#[derive(Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    /// A boolean: true or false.
    Boolean,
    /// A 32-bit signed integer
    Int2,
    /// A UTF-8 encoded text.
    Text,
    /// A 32-bit unsigned integer
    Uint2,
}

impl Display for ValueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Boolean => f.write_str("BOOLEAN"),
            ValueType::Int2 => f.write_str("I32"),
            ValueType::Uint2 => f.write_str("U32"),
            ValueType::Text => f.write_str("TEXT"),
        }
    }
}
