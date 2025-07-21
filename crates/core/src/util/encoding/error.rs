// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Re-export core::Error as the unified error type for this module
pub use crate::Error;

// Helper function to create encoding errors
use crate::diagnostic::Diagnostic;

pub fn encoding_error(message: String) -> crate::Error {
    crate::Error(Diagnostic {
        code: "ENC_001".to_string(),
        statement: None,
        message: format!("Encoding error: {}", message),
        column: None,
        span: None,
        label: None,
        help: Some("Check data format and encoding".to_string()),
        notes: vec![],
        cause: None,
    })
}

// Note: bincode::Error conversion is now handled in core/error.rs
// Add any encoding-specific error conversions here if needed