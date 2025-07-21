// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Re-export core::Error as the unified error type for this crate
pub use reifydb_core::Error;

// Helper functions to create specific auth errors
use reifydb_core::diagnostic::Diagnostic;

pub fn authentication_failed(reason: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "AUTH_001".to_string(),
        statement: None,
        message: format!("Authentication failed: {}", reason),
        column: None,
        span: None,
        label: None,
        help: Some("Check credentials and try again".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn authorization_denied(resource: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "AUTH_002".to_string(),
        statement: None,
        message: format!("Access denied to resource: {}", resource),
        column: None,
        span: None,
        label: None,
        help: Some("Ensure you have the required permissions".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn token_expired() -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "AUTH_003".to_string(),
        statement: None,
        message: "Authentication token has expired".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Refresh your authentication token".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn invalid_token() -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "AUTH_004".to_string(),
        statement: None,
        message: "Invalid authentication token".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Provide a valid authentication token".to_string()),
        notes: vec![],
        cause: None,
    })
}