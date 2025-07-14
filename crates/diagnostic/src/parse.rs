// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Diagnostic, Span};

pub fn multiple_expressions_without_braces(span: Span) -> Diagnostic {
    let keyword = span.fragment.clone();
    Diagnostic {
        code: "PA_005".to_string(),
        statement: None,
        message: format!("multiple expressions in `{}` require curly braces", &keyword),
        span: Some(span),
        label: Some("missing `{ … }` around expressions".to_string()),
        help: Some(format!(
            "wrap the expressions in curly braces:\n    {} {{ expr1, expr2, … }}",
            keyword
        )),
        column: None,
        notes: vec![],
    }
}
