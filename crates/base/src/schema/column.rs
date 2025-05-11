// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueType;
use crate::expression::Expression;

#[derive(Debug, PartialEq)]
pub struct ColumnName(String);

impl ColumnName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl PartialEq<str> for ColumnName {
    fn eq(&self, other: &str) -> bool {
        self.0.as_str() == other
    }
}

/// A column
#[derive(Debug)]
pub struct Column {
    pub name: ColumnName,
    pub value_type: ValueType,
    pub default: Option<Expression>,
}
