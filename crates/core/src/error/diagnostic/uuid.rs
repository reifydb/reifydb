// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::error::diagnostic::Diagnostic;
use crate::IntoOwnedSpan;

pub fn invalid_uuid4_format(span: impl IntoOwnedSpan) -> Diagnostic {
    let owned_span = span.into_span();
    let label = Some(format!("'{}' is not a valid UUID v4", owned_span.fragment));

    let help = "use UUID v4 format (e.g., 550e8400-e29b-41d4-a716-446655440000)".to_string();
    let notes = vec![
        "valid: 550e8400-e29b-41d4-a716-446655440000".to_string(),
        "valid: 6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_string(),
        "UUID v4 uses random or pseudo-random numbers".to_string(),
    ];

    Diagnostic {
        code: "UUID_001".to_string(),
        statement: None,
        message: "invalid UUID v4 format".to_string(),
        span: Some(owned_span),
        label,
        help: Some(help),
        notes,
        column: None,
        cause: None,
    }
}

pub fn invalid_uuid7_format(span: impl IntoOwnedSpan) -> Diagnostic {
    let owned_span = span.into_span();
    let label = Some(format!("'{}' is not a valid UUID v7", owned_span.fragment));

    let help = "use UUID v7 format (e.g., 017f22e2-79b0-7cc3-98c4-dc0c0c07398f)".to_string();
    let notes = vec![
        "valid: 017f22e2-79b0-7cc3-98c4-dc0c0c07398f".to_string(),
        "valid: 01854d6e-bd60-7b28-a3c7-6b4ad2c4e2e8".to_string(),
        "UUID v7 uses timestamp-based generation".to_string(),
    ];

    Diagnostic {
        code: "UUID_002".to_string(),
        statement: None,
        message: "invalid UUID v7 format".to_string(),
        span: Some(owned_span),
        label,
        help: Some(help),
        notes,
        column: None,
        cause: None,
    }
}