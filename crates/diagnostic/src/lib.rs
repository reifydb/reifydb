// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb_core::ValueKind;
pub use span::{IntoSpan, Line, Offset, Span};

pub mod catalog;
pub mod plan;
pub mod policy;
pub mod sequence;
pub mod span;
mod util;

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
    pub value: ValueKind,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiagnosticTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<DiagnosticColumn>,
}

pub fn get_line(source: &str, line: u32) -> &str {
    source.lines().nth((line - 1) as usize).unwrap_or("")
}

pub trait DiagnosticRenderer {
    fn render(&self, diagnostic: &Diagnostic, source: &str) -> String;
}

pub struct DefaultRenderer;

use std::fmt::Write;

impl DiagnosticRenderer for DefaultRenderer {
    fn render(&self, d: &Diagnostic, source: &str) -> String {
        let mut output = String::new();

        let _ = writeln!(&mut output, "error[{}]: {}", d.code, d.message);

        if let Some(span) = &d.span {
            let line = span.line.0;
            let col = span.offset.0;
            let line_content = get_line(source, line);
            let line_number_width = line.to_string().len().max(2);

            let _ = writeln!(
                &mut output,
                " {0:>width$} │ {1}",
                line,
                line_content,
                width = line_number_width
            );
            let _ = writeln!(
                &mut output,
                " {0:>width$} │ {1}^",
                "",
                " ".repeat(col as usize),
                width = line_number_width
            );
            let _ = writeln!(
                &mut output,
                " {0:>width$} = {1}",
                "",
                d.label.as_deref().unwrap_or("value exceeds type bounds"),
                width = line_number_width
            );
        }

        if let Some(col) = &d.column {
            let _ =
                writeln!(&mut output, "\nnote: column `{}` is of type `{}`", col.name, col.value);
        }

        if let Some(help) = &d.help {
            let _ = writeln!(&mut output, "\nhelp: {}", help);
        }

        for note in &d.notes {
            let _ = writeln!(&mut output, "\nnote: {}", note);
        }

        output
    }
}

impl Diagnostic {
    pub fn to_string(&self, source: &str) -> String {
        match self.code {
            _ => DefaultRenderer.render(self, source),
        }
    }
}
