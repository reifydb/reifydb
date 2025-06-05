// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Diagnostic, Span};

pub fn invalid_type(span: impl Into<Span>) -> Diagnostic {
    let span = span.into();
    Diagnostic {
        code: "PL0001".to_string(),
        message: format!("invalid type name: `{}`", span.fragment),
        span: Some(span.clone()),
        label: Some("not a recognized type".to_string()),
        help: None,
        column: None,
        notes: vec![],
    }
}
