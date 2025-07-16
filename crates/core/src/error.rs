// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Diagnostic;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub struct Error(pub Diagnostic);

pub fn get_line(source: &str, line: u32) -> &str {
    source.lines().nth((line - 1) as usize).unwrap_or("")
}

use std::fmt::Write;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let diagnostic = &self.0;
        let mut output = String::new();

        if !diagnostic.caused_by.is_some() {
            self.render_flat(&mut output, diagnostic);
        } else {
            self.render_nested(&mut output, diagnostic, 0);
        }

        f.write_str(output.as_str())
    }
}

impl Error {
    fn render_flat(&self, output: &mut String, diagnostic: &Diagnostic) {
        let _ = writeln!(output, "ERROR {}", diagnostic.code);
        let _ = writeln!(output, "  {}", diagnostic.message);
        let _ = writeln!(output);

        if let Some(span) = &diagnostic.span {
            let line = span.line.0;
            let col = span.column.0;
            let statement = diagnostic.statement.as_ref().map(|x| x.as_str()).unwrap_or("");

            let _ = writeln!(output, "LOCATION");
            let _ = writeln!(output, "  line {}, column {}", line, col);
            let _ = writeln!(output);

            let line_content = get_line(statement, line);

            let _ = writeln!(output, "CODE");
            let _ = writeln!(output, "  {} │ {}", line, line_content);
            let fragment_start = line_content.find(&span.fragment).unwrap_or(col as usize);
            let _ = writeln!(
                output,
                "    │ {}{}",
                " ".repeat(fragment_start),
                "~".repeat(span.fragment.len())
            );
            let _ = writeln!(output, "    │");

            let label_text = diagnostic.label.as_deref().unwrap_or("");
            let span_center = fragment_start + span.fragment.len() / 2;
            let label_center_offset = if label_text.len() / 2 > span_center {
                0
            } else {
                span_center - label_text.len() / 2
            };

            let _ = writeln!(output, "    │ {}{}", " ".repeat(label_center_offset), label_text);
            let _ = writeln!(output);
        }

        if let Some(help) = &diagnostic.help {
            let _ = writeln!(output, "HELP");
            let _ = writeln!(output, "  {}", help);
            let _ = writeln!(output);
        }

        if let Some(col) = &diagnostic.column {
            let _ = writeln!(output, "COLUMN");
            let _ = writeln!(output, "  column `{}` is of type `{}`", col.name, col.data_type);
            let _ = writeln!(output);
        }

        if !diagnostic.notes.is_empty() {
            let _ = writeln!(output, "NOTES");
            for note in &diagnostic.notes {
                let _ = writeln!(output, "  • {}", note);
            }
        }
    }

    fn render_nested(&self, output: &mut String, diagnostic: &Diagnostic, depth: usize) {
        let indent = if depth == 0 { "" } else { "  " };
        let prefix = if depth == 0 { "" } else { "↳ " };

        // Main error line
        let _ = writeln!(output, "{}{}{}: {}", indent, prefix, diagnostic.code, diagnostic.message);

        // Location info
        if let Some(span) = &diagnostic.span {
            let line = span.line.0;
            let col = span.column.0;
            let statement = diagnostic.statement.as_ref().map(|x| x.as_str()).unwrap_or("");

            let _ = writeln!(
                output,
                "{}  at {} (line {}, column {})",
                indent,
                if statement.is_empty() {
                    "unknown".to_string()
                } else {
                    format!("\"{}\"", span.fragment)
                },
                line,
                col
            );
            let _ = writeln!(output);

            // Code visualization
            let line_content = get_line(statement, line);

            let _ = writeln!(output, "{}  {} │ {}", indent, line, line_content);
            let fragment_start = line_content.find(&span.fragment).unwrap_or(col as usize);
            let _ = writeln!(
                output,
                "{}    │ {}{}",
                indent,
                " ".repeat(fragment_start),
                "~".repeat(span.fragment.len())
            );

            let label_text = diagnostic.label.as_deref().unwrap_or("");
            if !label_text.is_empty() {
                let span_center = fragment_start + span.fragment.len() / 2;
                let label_center_offset = if label_text.len() / 2 > span_center {
                    0
                } else {
                    span_center - label_text.len() / 2
                };

                let _ = writeln!(
                    output,
                    "{}    │ {}{}",
                    indent,
                    " ".repeat(label_center_offset),
                    label_text
                );
            }
            let _ = writeln!(output);
        }

        // Handle nested cause first (if exists)
        if let Some(cause) = &diagnostic.caused_by {
            self.render_nested(output, cause, depth + 1);
        }

        // Help section
        if let Some(help) = &diagnostic.help {
            let _ = writeln!(output, "{}  help: {}", indent, help);
        }

        // Column info
        if let Some(col) = &diagnostic.column {
            let _ = writeln!(
                output,
                "{}  column `{}` is of type `{}`",
                indent, col.name, col.data_type
            );
        }

        // Notes
        if !diagnostic.notes.is_empty() {
            for note in &diagnostic.notes {
                let _ = writeln!(output, "{}  note: {}", indent, note);
            }
        }

        // Add spacing between diagnostic levels
        if depth > 0 {
            let _ = writeln!(output);
        }
    }
}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        self.0
    }
}

impl std::error::Error for Error {}
