// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb_core::ValueKind;
pub use span::{Line, Offset, Span};

mod span;

#[derive(Debug, Clone, PartialEq)]
pub struct Diagnostic {
    pub code: &'static str,
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

// pub fn render_diagnostic(d: &Diagnostic, source: &str) {
//     println!("error[{}]: {}", d.code, d.message);
//
//     if let Some(span) = &d.span {
//         let line = span.line.0;
//         let col = span.offset.0;
//         println!(" --> line {}:", line);
//         println!("  {} | {}", line, get_line(source.trim(), line));
//         println!("    | {}^", " ".repeat(col));
//         if let Some(label) = &d.label {
//             println!("      = {}", label);
//         }
//     }
//
//     if let Some(help) = &d.help {
//         println!("help: {}", help);
//     }
//
//     for note in &d.notes {
//         println!("note: {}", note);
//     }
// }

pub fn get_line(source: &str, line: u32) -> &str {
    source.lines().nth((line - 1) as usize).unwrap_or("")
}

pub fn overflow_diagnostic(span: Span, value: &str, column: DiagnosticColumn) -> Diagnostic {
    let target_type = column.value;
    Diagnostic {
        code: "E0101",
        message: format!("value overflows column type `{}`", target_type),
        span: Some(span),
        label: Some(format!(
            "value `{}` does not fit into `{}` (range: {})",
            value, target_type, "-128 to 127"
        )),
        help: Some(format!(
            "reduce the value, change the column type to a wider type or change the overflow policy"
        )),
        notes: vec![],
        column: Some(column),
    }
}

pub trait DiagnosticRenderer {
    fn render(&self, diagnostic: &Diagnostic, source: &str);
}

pub struct OverflowRenderer;

impl DiagnosticRenderer for OverflowRenderer {
    fn render(&self, d: &Diagnostic, source: &str) {
        println!("error[{}]: {}", d.code, d.message);

        if let Some(span) = &d.span {
            let line = span.line.0;
            println!(" --> line {}:", line);
            println!("  {} | {}", line, get_line(source, line));
            let col = span.offset.0;
            println!("      | {}^", " ".repeat(col));
            println!("      = {}", d.label.as_deref().unwrap_or("value exceeds type bounds"));
        }

        if let Some(col) = &d.column {
            println!("note: column `{}` is of type `{}`", col.name, col.value);
        }

        if let Some(help) = &d.help {
            println!("help: {}", help);
        }

        for note in &d.notes {
            println!("note: {}", note);
        }
    }
}

impl Diagnostic {
    pub fn render(&self, source: &str) {
        match self.code {
            "E0101" => OverflowRenderer.render(self, source),
            _ => unimplemented!(),
        }
    }
}
