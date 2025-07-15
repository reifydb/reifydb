// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb_core::{Diagnostic, Span};

pub mod catalog;
pub mod parse;
pub mod query;
pub mod sequence;
pub mod r#type;
mod util;
pub mod temporal;

pub trait DiagnosticRenderer {
    fn render(&self, diagnostic: &Diagnostic) -> String;
}

pub struct DefaultRenderer;

pub fn get_line(source: &str, line: u32) -> &str {
    source.lines().nth((line - 1) as usize).unwrap_or("")
}

use std::fmt::Write;

impl DiagnosticRenderer for DefaultRenderer {
    fn render(&self, diagnostic: &Diagnostic) -> String {
        let mut output = String::new();

        let _ = writeln!(&mut output, "ERROR {}", diagnostic.code);
        let _ = writeln!(&mut output, "  {}", diagnostic.message);
        let _ = writeln!(&mut output);

        if let Some(span) = &diagnostic.span {
            let line = span.line.0;
            let col = span.column.0;
            let statement = diagnostic.statement.as_ref().map(|x| x.as_str()).unwrap_or("");

            let _ = writeln!(&mut output, "LOCATION");
            let _ = writeln!(&mut output, "  line {}, column {}", line, col);
            let _ = writeln!(&mut output);

            let line_content = get_line(statement, line);

            let _ = writeln!(&mut output, "CODE");
            let _ = writeln!(&mut output, "  {} │ {}", line, line_content);
            let fragment_start = line_content.find(&span.fragment).unwrap_or(col as usize);
            let _ = writeln!(
                &mut output,
                "    │ {}{}",
                " ".repeat(fragment_start),
                "~".repeat(span.fragment.len())
            );
            let _ = writeln!(&mut output, "    │");
            
            let label_text = diagnostic.label.as_deref().unwrap_or("");
            let span_center = fragment_start + span.fragment.len() / 2;
            let label_center_offset = if label_text.len() / 2 > span_center {
                0
            } else {
                span_center - label_text.len() / 2
            };
            
            let _ = writeln!(
                &mut output,
                "    │ {}{}",
                " ".repeat(label_center_offset),
                label_text
            );
            let _ = writeln!(&mut output);
        }

        if let Some(help) = &diagnostic.help {
            let _ = writeln!(&mut output, "HELP");
            let _ = writeln!(&mut output, "  {}", help);
            let _ = writeln!(&mut output);
        }

        if let Some(col) = &diagnostic.column {
            let _ = writeln!(&mut output, "COLUMN");
            let _ = writeln!(&mut output, "  column `{}` is of type `{}`", col.name, col.data_type);
            let _ = writeln!(&mut output);
        }

        if !diagnostic.notes.is_empty() {
            let _ = writeln!(&mut output, "NOTES");
            for note in &diagnostic.notes {
                let _ = writeln!(&mut output, "  • {}", note);
            }
        }

        output
    }
}

impl DefaultRenderer {
    pub fn render_string(diagnostic: &Diagnostic) -> String {
        DefaultRenderer.render(diagnostic)
    }
}
