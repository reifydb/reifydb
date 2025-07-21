// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::IntoOwnedSpan;
use crate::error::diagnostic::Diagnostic;

pub fn multiple_expressions_without_braces(span: impl IntoOwnedSpan) -> Diagnostic {
    let owned_span = span.into_span();
    let keyword = owned_span.fragment.clone();
    Diagnostic {
        code: "PA_005".to_string(),
        statement: None,
        message: format!("multiple expressions in `{}` require curly braces", &keyword),
        span: Some(owned_span),
        label: Some("missing `{ … }` around expressions".to_string()),
        help: Some(format!(
            "wrap the expressions in curly braces:\n    {} {{ expr1, expr2, … }}",
            keyword
        )),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn unrecognized_type(span: impl IntoOwnedSpan) -> Diagnostic {
    let owned_span = span.into_span();
    let type_name = owned_span.fragment.clone();
    Diagnostic {
        code: "PA_006".to_string(),
        statement: None,
        message: format!("cannot find type `{}`", &type_name),
        span: Some(owned_span),
        label: Some("type not found".to_string()),
        help: None,
        column: None,
        notes: vec![],
        cause: None,
    }
}
