// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use policy::{OverflowPolicy, Policy, PolicyError, UnderflowPolicy};
use reifydb_core::ValueKind;

mod policy;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub value: ValueKind,
    // pub default: Option<Expression>,
    pub policies: Vec<Policy>,
}

impl Column {
    pub fn new(name: String, value: ValueKind) -> Self {
        Self { name, value, policies: Vec::new() }
    }
}

impl Column {
    pub fn overflow_policy(&self) -> OverflowPolicy {
        OverflowPolicy::Error
    }

    pub fn underflow_policy(&self) -> UnderflowPolicy {
        UnderflowPolicy::Error
    }
}
