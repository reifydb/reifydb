// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use create::ColumnToCreate;
use reifydb_diagnostic::Diagnostic;

mod create;
mod get;
mod layout;

#[derive(Debug, Clone)]
pub enum ColumnPolicy {
    Saturation(ColumnSaturationPolicy),
}

impl ColumnPolicy {
    pub fn default_saturation_policy() -> Self {
        Self::Saturation(ColumnSaturationPolicy::default())
    }
}

#[derive(Debug, Clone)]
pub enum ColumnSaturationPolicy {
    Error,
    // Saturate,
    // Wrap,
    // Zero,
    Undefined,
}

pub const DEFAULT_COLUMN_SATURATION_POLICY: ColumnSaturationPolicy = ColumnSaturationPolicy::Error;

impl Default for ColumnSaturationPolicy {
    fn default() -> Self {
        Self::Error
    }
}

#[derive(Debug, PartialEq)]
pub enum ColumnPolicyError {
    Saturation(Diagnostic),
}

impl ColumnPolicyError {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            ColumnPolicyError::Saturation(diagnostic) => diagnostic,
        }
    }
}
