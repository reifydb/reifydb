// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_diagnostic::Diagnostic;

#[derive(Debug, Clone)]
pub enum DepColumnPolicy {
    Saturation(DepColumnSaturationPolicy),
}

impl DepColumnPolicy {
    pub fn default_saturation_policy() -> Self {
        Self::Saturation(DepColumnSaturationPolicy::default())
    }
}

#[derive(Debug, Clone)]
pub enum DepColumnSaturationPolicy {
    Error,
    // Saturate,
    // Wrap,
    // Zero,
    Undefined,
}

pub const DEP_DEFAULT_COLUMN_SATURATION_POLICY: DepColumnSaturationPolicy = DepColumnSaturationPolicy::Error;

impl Default for DepColumnSaturationPolicy {
    fn default() -> Self {
        Self::Error
    }
}

#[derive(Debug, PartialEq)]
pub enum DEP_ColumnPolicyError {
    Saturation(Diagnostic),
}

impl DEP_ColumnPolicyError {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            DEP_ColumnPolicyError::Saturation(diagnostic) => diagnostic,
        }
    }
}
