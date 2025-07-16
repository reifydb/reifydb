// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{DataType, Span};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Diagnostic {
    pub code: String,
    pub statement: Option<String>,
    pub message: String,
    pub column: Option<DiagnosticColumn>,

    pub span: Option<Span>,
    pub label: Option<String>,
    pub help: Option<String>,
    pub notes: Vec<String>,
    pub caused_by: Option<Box<Diagnostic>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticColumn {
    pub name: String,
    pub data_type: DataType,
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
        if let Some(ref mut cause) = self.caused_by {
            let mut updated_cause = std::mem::replace(cause.as_mut(), Diagnostic::default());
            updated_cause.set_statement(statement);
            *cause = Box::new(updated_cause);
        }
    }
}
