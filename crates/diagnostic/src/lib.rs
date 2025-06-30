// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb_core::{Diagnostic, Span};

pub mod catalog;
pub mod query;
pub mod sequence;
pub mod r#type;
mod util;

pub trait DiagnosticRenderer {
    fn render(&self, diagnostic: &Diagnostic, source: &str) -> String;
}

pub struct DefaultRenderer;

pub fn get_line(source: &str, line: u32) -> &str {
    source.lines().nth((line - 1) as usize).unwrap_or("")
}

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

impl DefaultRenderer {
    pub fn render_string(diagnostic: &Diagnostic, source: &str) -> String {
        DefaultRenderer.render(diagnostic, source)
    }
}
