// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod ast;
pub mod auth;
pub mod boolean;
pub mod cast;
pub mod catalog;
pub mod conversion;
pub mod engine;
pub mod function;
pub mod network;
pub mod number;
pub mod query;
pub mod render;
pub mod sequence;
pub mod serialization;
pub mod temporal;
pub mod transaction;
pub mod uuid;
mod util;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Diagnostic {
    pub code: String,
    pub statement: Option<String>,
    pub message: String,
    pub column: Option<DiagnosticColumn>,

    pub span: Option<OwnedSpan>,
    pub label: Option<String>,
    pub help: Option<String>,
    pub notes: Vec<String>,
    pub cause: Option<Box<Diagnostic>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticColumn {
    pub name: String,
    pub ty: Type,
}

impl Display for Diagnostic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.code))
    }
}

impl Diagnostic {
    /// Set the statement for this diagnostic and all nested diagnostics recursively
    pub fn set_statement(&mut self, statement: String) {
        self.statement = Some(statement.clone());

        // Recursively set statement for all nested diagnostics
        if let Some(ref mut cause) = self.cause {
            let mut updated_cause = std::mem::replace(cause.as_mut(), Diagnostic::default());
            updated_cause.set_statement(statement);
            *cause = Box::new(updated_cause);
        }
    }

    /// Update the span for this diagnostic and all nested diagnostics recursively
    pub fn update_spans(&mut self, new_span: &OwnedSpan) {
        if self.span.is_some() {
            self.span = Some(new_span.clone());
        }
        if let Some(ref mut cause) = self.cause {
            cause.update_spans(new_span);
        }
    }
}

use crate::{OwnedSpan, Type};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
