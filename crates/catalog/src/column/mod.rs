// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use policy::{ColumnOverflowPolicy, ColumnPolicy, ColumnPolicyError, ColumnUnderflowPolicy, DEFAULT_COLUMN_OVERFLOW_POLICY};
use reifydb_core::ValueKind;

mod policy;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub value: ValueKind,
    // pub default: Option<Expression>,
    pub policies: Vec<ColumnPolicy>,
}

impl Column {
    pub fn new(name: String, value: ValueKind, policies: Vec<ColumnPolicy>) -> Self {
        Self { name, value, policies }
    }
}

impl Column {
    pub fn overflow_policy(&self) -> ColumnOverflowPolicy {
        ColumnOverflowPolicy::Error
    }

    pub fn underflow_policy(&self) -> ColumnUnderflowPolicy {
        ColumnUnderflowPolicy::Error
    }
}
