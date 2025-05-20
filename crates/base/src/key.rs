// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Value;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

pub enum KeyError {}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Key {
    Boolean(bool),
    Bytea(Vec<u8>),
    Int2(i16),
    Text(String),
    Uint2(u16),
    Undefined,
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Key::Bytea(lhs), Key::Bytea(rhs)) => lhs.cmp(rhs),
            (Key::Int2(lhs), Key::Int2(rhs)) => lhs.cmp(rhs),
            (Key::Text(lhs), Key::Text(rhs)) => lhs.cmp(rhs),
            (Key::Uint2(lhs), Key::Uint2(rhs)) => lhs.cmp(rhs),
            (Key::Undefined, _) => Ordering::Greater,
            (_, Key::Undefined) => Ordering::Less,
            (left, right) => unimplemented!("{left:?} {right:?}"),
        }
    }
}

impl TryFrom<Value> for Key {
    type Error = KeyError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(v) => Ok(Self::Boolean(v)),
            Value::Float4(_) => unimplemented!(),
            Value::Float8(_) => unimplemented!(),
            Value::Int2(v) => Ok(Self::Int2(v)),
            Value::Text(v) => Ok(Self::Text(v)),
            Value::Uint2(v) => Ok(Self::Uint2(v)),
            Value::Undefined => Ok(Self::Undefined),
        }
    }
}
