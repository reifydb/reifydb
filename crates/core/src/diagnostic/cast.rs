// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::diagnostic::Diagnostic;
use crate::{Type, Span};

pub fn unsupported_cast(span: Span, from_type: Type, to_type: Type) -> Diagnostic {
    let label = Some(format!("cannot cast {} of type {} to {}", span.fragment, from_type, to_type));
    Diagnostic {
        code: "CAST_001".to_string(),
        statement: None,
        message: format!("unsupported cast from {} to {}", from_type, to_type),
        span: Some(span),
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

pub fn invalid_number(span: Span, target: Type, cause: Diagnostic) -> Diagnostic {
    let label = Some(format!("failed to cast to {}", target));
    Diagnostic {
        code: "CAST_002".to_string(),
        statement: None,
        message: format!("failed to cast to {}", target),
        span: Some(span),
        label,
        help: None,
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}

pub fn invalid_temporal(span: Span, target: Type, cause: Diagnostic) -> Diagnostic {
    let label = Some(format!("failed to cast to {}", target));
    Diagnostic {
        code: "CAST_003".to_string(),
        statement: None,
        message: format!("failed to cast to {}", target),
        span: Some(span),
        label,
        help: None,
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}

pub fn invalid_boolean(span: Span, cause: Diagnostic) -> Diagnostic {
    let label = Some("failed to cast to bool".to_string());
    Diagnostic {
        code: "CAST_004".to_string(),
        statement: None,
        message: "failed to cast to bool".to_string(),
        span: Some(span),
        label,
        help: None,
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}
