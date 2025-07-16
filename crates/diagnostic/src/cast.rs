// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{Diagnostic, Span};
use reifydb_core::DataType;

pub fn unsupported_cast(span: Span, from_type: DataType, to_type: DataType) -> Diagnostic {
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
        caused_by: None,
    }
}

pub fn invalid_number(span: Span, target: DataType, cause: Diagnostic) -> Diagnostic {
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
        caused_by: Some(Box::from(cause)),
    }
}

pub fn invalid_temporal(span: Span, target: DataType, cause: Diagnostic) -> Diagnostic {
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
        caused_by: Some(Box::from(cause)),
    }
}
