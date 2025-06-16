// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use policy::{
    DepColumnPolicy, DEP_ColumnPolicyError, DepColumnSaturationPolicy, DEP_DEFAULT_COLUMN_SATURATION_POLICY,
};
use reifydb_core::ValueKind;

mod policy;

#[derive(Debug, Clone)]
pub struct DepColumn {
    pub name: String,
    pub value: ValueKind,
    // pub default: Option<Expression>,
    pub policies: Vec<DepColumnPolicy>,
}

impl DepColumn {
    pub fn new(name: String, value: ValueKind, policies: Vec<DepColumnPolicy>) -> Self {
        Self { name, value, policies }
    }
}

impl DepColumn {
    pub fn saturation_policy(&self) -> DepColumnSaturationPolicy {
        DepColumnSaturationPolicy::Error
    }
}
