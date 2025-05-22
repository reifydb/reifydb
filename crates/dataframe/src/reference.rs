// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub struct RowRef<'df> {
    pub values: Vec<ValueRef<'df>>,
    pub column_index: Arc<Vec<String>>,
    pub columns: &'df HashMap<String, usize>,
}

impl<'df> RowRef<'df> {
    pub fn get(&self, name: &str) -> Option<&ValueRef<'df>> {
        self.columns.get(name).and_then(|&i| self.values.get(i))
    }
}

#[derive(Debug, Clone)]
pub enum ValueRef<'a> {
    Int2(&'a i16),
    // Float(&'a f64),
    Text(&'a str),
    Bool(&'a bool),
    Undefined,
}

impl<'a> ValueRef<'a> {
    pub fn as_value(&self) -> Value {
        match self {
            ValueRef::Int2(v) => Value::Int2(**v),
            // ValueRef::Float(v) => Value::Float(OrderedF64(**v)),
            ValueRef::Text(s) => Value::Text(s.to_string()),
            ValueRef::Bool(b) => Value::Bool(**b),
            ValueRef::Undefined => Value::Undefined,
        }
    }
}
