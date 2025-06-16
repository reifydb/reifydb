// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Diagnostic, Span};

impl Diagnostic {
    pub fn schema_already_exists(span: Span, name: &str) -> Diagnostic {
        Diagnostic {
            code: "CA_001".to_string(),
            message: format!("schema `{}` already exists", name),
            span: Some(span),
            label: Some("duplicate schema definition".to_string()),
            help: Some("choose a different name or drop the existing schema first".to_string()),
            column: None,
            notes: vec![],
        }
    }
    
    pub fn schema_not_found(span: Span, name: &str) -> Diagnostic {
        Diagnostic {
            code: "CA_002".to_string(),
            message: format!("schema `{}` not found", name),
            span: Some(span),
            label: Some("undefined schema reference".to_string()),
            help: Some("make sure the schema exists before using it or create it first".to_string()),
            column: None,
            notes: vec![],
        }
    }
}
