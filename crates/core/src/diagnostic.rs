// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Kind, Span};

#[derive(Debug, Clone, PartialEq)]
pub struct Diagnostic {
    pub code: String,
    pub message: String,
    pub column: Option<DiagnosticColumn>,

    pub span: Option<Span>,
    pub label: Option<String>,
    pub help: Option<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiagnosticColumn {
    pub name: String,
    pub value: Kind,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiagnosticTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<DiagnosticColumn>,
}
