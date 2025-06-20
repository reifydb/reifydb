// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Diagnostic, Span};
use reifydb_core::ValueKind;

impl Diagnostic {
    pub fn constant_type_mismatch(span: Span, expected: ValueKind) -> Self {
        Diagnostic {
            code: "EX_001".to_string(),
            message: format!("cannot interpret `{}` as `{}`", span.fragment.as_str(), expected),
            span: Some(span),
            label: Some("this constant does not match the expected type".to_string()),
            help: None,
            column: None,
            notes: vec![],
        }
    }
}
