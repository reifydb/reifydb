// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

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

        let _ = writeln!(&mut output, "error[{}]: {}", diagnostic.code, diagnostic.message);

        if let Some(span) = &diagnostic.span {
            let line = span.line.0;
            let col = span.offset.0;
            let statement = diagnostic.statement.as_ref().map(|x| x.as_str()).unwrap_or("");

            let line_content = get_line(statement, line);
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
                diagnostic.label.as_deref().unwrap_or("value exceeds type bounds"),
                width = line_number_width
            );
        }

        if let Some(col) = &diagnostic.column {
            let _ =
                writeln!(&mut output, "\nnote: column `{}` is of type `{}`", col.name, col.value);
        }

        if let Some(help) = &diagnostic.help {
            let _ = writeln!(&mut output, "\nhelp: {}", help);
        }

        for note in &diagnostic.notes {
            let _ = writeln!(&mut output, "\nnote: {}", note);
        }

        f.write_str(output.as_str())
    }
}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        self.0
    }
}

impl std::error::Error for Error {}
