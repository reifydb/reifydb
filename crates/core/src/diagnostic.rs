// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{DataType, Span};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub statement: Option<String>,
    pub message: String,
    pub column: Option<DiagnosticColumn>,

    pub span: Option<Span>,
    pub label: Option<String>,
    pub help: Option<String>,
    pub notes: Vec<String>,
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
