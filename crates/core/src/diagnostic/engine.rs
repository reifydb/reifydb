// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::diagnostic::Diagnostic;

/// General frame processing error
pub fn frame_error(message: String) -> Diagnostic {
    Diagnostic {
        code: "ENG_001".to_string(),
        statement: None,
        message: format!("Frame processing error: {}", message),
        column: None,
        span: None,
        label: None,
        help: Some("Check frame data and operations".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// View flow processing error
pub fn flow_error(message: String) -> Diagnostic {
    Diagnostic {
        code: "ENG_002".to_string(),
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

/// Column policy saturation error - wraps an existing diagnostic
pub fn saturation_error(diagnostic: Diagnostic) -> Diagnostic {
    let statement = diagnostic.statement.clone();
    let message = diagnostic.message.clone();
    let column = diagnostic.column.clone();
    let span = diagnostic.span.clone();
    let label = diagnostic.label.clone();
    let notes = diagnostic.notes.clone();
    
    Diagnostic {
        code: "ENG_003".to_string(),
        statement,
        message: format!("Column policy saturation: {}", message),
        column,
        span,
        label,
        help: Some("Adjust column policy constraints".to_string()),
        notes,
        cause: Some(Box::new(diagnostic)),
    }
}