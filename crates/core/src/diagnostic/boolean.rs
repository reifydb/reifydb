// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Span;
use crate::diagnostic::Diagnostic;

pub fn invalid_boolean_format(span: Span) -> Diagnostic {
    let label = Some(format!("expected 'true' or 'false', found '{}'", span.fragment));
    Diagnostic {
        code: "BOOLEAN_001".to_string(),
        statement: None,
        message: "invalid boolean format".to_string(),
        span: Some(span),
        label,
        help: Some("use 'true' or 'false'".to_string()),
        notes: vec!["valid: true, TRUE".to_string(), "valid: false, FALSE".to_string()],
        column: None,
        cause: None,
    }
}

pub fn empty_boolean_value(span: Span) -> Diagnostic {
    let label = Some("boolean value cannot be empty".to_string());
    Diagnostic {
        code: "BOOLEAN_002".to_string(),
        statement: None,
        message: "empty boolean value".to_string(),
        span: Some(span),
        label,
        help: Some("provide either 'true' or 'false'".to_string()),
        notes: vec!["valid: true".to_string(), "valid: false".to_string()],
        column: None,
        cause: None,
    }
}

pub fn invalid_numeric_boolean(span: Span) -> Diagnostic {
    let label = Some(format!("number '{}' cannot be cast to boolean, only 1 or 0 are allowed", span.fragment));
    Diagnostic {
        code: "BOOLEAN_003".to_string(),
        statement: None,
        message: "invalid numeric boolean".to_string(),
        span: Some(span),
        label,
        help: Some("use 1 for true or 0 for false".to_string()),
        notes: vec![
            "valid: 1 → true".to_string(),
            "valid: 0 → false".to_string(),
            "invalid: any other number".to_string(),
        ],
        column: None,
        cause: None,
    }
}
