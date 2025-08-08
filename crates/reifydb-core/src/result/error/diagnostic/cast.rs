// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::error::diagnostic::Diagnostic;
use crate::{IntoOwnedSpan, Type};

pub fn unsupported_cast(span: impl IntoOwnedSpan, from_type: Type, to_type: Type) -> Diagnostic {
    let owned_span = span.into_span();
    let label =
        Some(format!("cannot cast {} of type {} to {}", owned_span.fragment, from_type, to_type));
    Diagnostic {
        code: "CAST_001".to_string(),
        statement: None,
        message: format!("unsupported cast from {} to {}", from_type, to_type),
        span: Some(owned_span),
        label,
        help: Some("ensure the source and target types are compatible for casting".to_string()),
        notes: vec![
            "supported casts include: numeric to numeric, string to temporal, boolean to numeric"
                .to_string(),
        ],
        column: None,
        cause: None,
    }
}

pub fn invalid_number(span: impl IntoOwnedSpan, target: Type, cause: Diagnostic) -> Diagnostic {
    let owned_span = span.into_span();
    let label = Some(format!("failed to cast to {}", target));
    Diagnostic {
        code: "CAST_002".to_string(),
        statement: None,
        message: format!("failed to cast to {}", target),
        span: Some(owned_span),
        label,
        help: None,
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}

pub fn invalid_temporal(span: impl IntoOwnedSpan, target: Type, cause: Diagnostic) -> Diagnostic {
    let owned_span = span.into_span();
    let label = Some(format!("failed to cast to {}", target));
    Diagnostic {
        code: "CAST_003".to_string(),
        statement: None,
        message: format!("failed to cast to {}", target),
        span: Some(owned_span),
        label,
        help: None,
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}

pub fn invalid_boolean(span: impl IntoOwnedSpan, cause: Diagnostic) -> Diagnostic {
    let owned_span = span.into_span();
    let label = Some("failed to cast to bool".to_string());
    Diagnostic {
        code: "CAST_004".to_string(),
        statement: None,
        message: "failed to cast to bool".to_string(),
        span: Some(owned_span),
        label,
        help: None,
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}

pub fn invalid_uuid(span: impl IntoOwnedSpan, target: Type, cause: Diagnostic) -> Diagnostic {
    let owned_span = span.into_span();
    let label = Some(format!("failed to cast to {}", target));
    Diagnostic {
        code: "CAST_005".to_string(),
        statement: None,
        message: format!("failed to cast to {}", target),
        span: Some(owned_span),
        label,
        help: None,
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}

pub fn invalid_blob_to_utf8(span: impl IntoOwnedSpan, cause: Diagnostic) -> Diagnostic {
    let owned_span = span.into_span();
    let label = Some("failed to cast BLOB to UTF8".to_string());
    Diagnostic {
        code: "CAST_006".to_string(),
        statement: None,
        message: "failed to cast BLOB to UTF8".to_string(),
        span: Some(owned_span),
        label,
        help: Some("BLOB contains invalid UTF-8 bytes. Consider using to_utf8_lossy() function instead".to_string()),
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}
