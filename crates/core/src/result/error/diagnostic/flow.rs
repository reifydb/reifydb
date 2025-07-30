// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::error::diagnostic::Diagnostic;

/// View flow processing error
pub fn flow_error(message: String) -> Diagnostic {
    Diagnostic {
        code: "FLOW_001".to_string(),
        statement: None,
        message: format!("Flow processing error: {}", message),
        column: None,
        span: None,
        label: None,
        help: Some("Check view flow configuration".to_string()),
        notes: vec![],
        cause: None,
    }
}
