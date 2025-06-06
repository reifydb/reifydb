// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_diagnostic::Diagnostic;

#[derive(Debug, Clone)]
pub enum ColumnPolicy {
    Overflow(ColumnOverflowPolicy),
    Underflow(ColumnUnderflowPolicy),
}

impl ColumnPolicy {
    pub fn default_overflow_policy() -> Self {
        Self::Overflow(ColumnOverflowPolicy::default())
    }
}

#[derive(Debug, Clone)]
pub enum ColumnOverflowPolicy {
    Error,
    // Saturate,
    // Wrap,
    // Zero,
    Undefined
}

pub const DEFAULT_COLUMN_OVERFLOW_POLICY: ColumnOverflowPolicy = ColumnOverflowPolicy::Error;

impl Default for ColumnOverflowPolicy {
    fn default() -> Self {
        Self::Error
    }
}

#[derive(Debug, Clone)]
pub enum ColumnUnderflowPolicy {
    Error,
    // Saturate,
    // Wrap,
    // Zero,
}

impl Default for ColumnUnderflowPolicy {
    fn default() -> Self {
        Self::Error
    }
}

#[derive(Debug, PartialEq)]
pub enum ColumnPolicyError {
    Overflow(Diagnostic),
    Underflow(Diagnostic),
}

impl ColumnPolicyError {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            ColumnPolicyError::Overflow(diagnostic) => diagnostic,
            ColumnPolicyError::Underflow(diagnostic) => diagnostic,
        }
    }
}
