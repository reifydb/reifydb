// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Re-export core::Error as the unified error type for this module
pub use reifydb_core::Error;

// Helper functions to create specific flow errors
use reifydb_core::diagnostic::Diagnostic;

pub fn flow_error(message: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "FLOW_001".to_string(),
        statement: None,
        message: format!("View flow processing failed: {}", message),
        column: None,
        span: None,
        label: None,
        help: None,
        notes: vec![],
        cause: None,
    })
}