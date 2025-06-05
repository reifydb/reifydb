// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::ValueKind;
use reifydb_diagnostic::Diagnostic;

#[derive(Debug, Clone)]
pub enum Policy {
    Default(),
    Overflow(OverflowPolicy),
    Underflow(UnderflowPolicy),
}

#[derive(Debug, Clone)]
pub enum OverflowPolicy {
    Error,
    // Saturate,
    // Wrap,
    // Zero,
}

#[derive(Debug, Clone)]
pub enum UnderflowPolicy {
    Error,
    // Saturate,
    // Wrap,
    // Zero,
}

#[derive(Debug, PartialEq)]
pub enum PolicyError {
    Overflow { column: String, value: ValueKind, input: String, diagnostic: Diagnostic },
    Underflow { column: String, value: ValueKind, input: String },
}

impl PolicyError {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            PolicyError::Overflow { diagnostic, .. } => diagnostic,
            PolicyError::Underflow { .. } => unimplemented!(),
        }
    }
}
