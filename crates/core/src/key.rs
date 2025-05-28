// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Value;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug)]
pub struct SortKey {
    pub column: String,
    pub direction: SortDirection,
}

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
            (Key::Bytea(l), Key::Bytea(lr)) => l.cmp(lr),
            (Key::Int2(l), Key::Int2(lr)) => l.cmp(lr),
            (Key::Text(l), Key::Text(lr)) => l.cmp(lr),
            (Key::Uint2(l), Key::Uint2(lr)) => l.cmp(lr),
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
