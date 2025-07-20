// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::IntoOwnedSpan;
use crate::diagnostic::Diagnostic;

pub fn column_not_found(span: impl IntoOwnedSpan) -> Diagnostic {
    let owned_span = span.into_span();
    Diagnostic {
        code: "QUERY_001".to_string(),
        statement: None,
        message: "column not found".to_string(),
        span: Some(owned_span),
        label: Some("this column does not exist in the current context".to_string()),
        help: Some("check for typos or ensure the column is defined in the input".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}
