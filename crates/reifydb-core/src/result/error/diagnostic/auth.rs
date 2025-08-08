// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::error::diagnostic::Diagnostic;

/// Authentication failed due to invalid credentials or other reasons
pub fn authentication_failed(reason: String) -> Diagnostic {
    Diagnostic {
        code: "AUTH_001".to_string(),
        statement: None,
        message: format!("Authentication failed: {}", reason),
        column: None,
        span: None,
        label: None,
        help: Some("Check your credentials and try again".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Authorization denied for accessing a resource
pub fn authorization_denied(resource: String) -> Diagnostic {
    Diagnostic {
        code: "AUTH_002".to_string(),
        statement: None,
        message: format!("Authorization denied for resource: {}", resource),
        column: None,
        span: None,
        label: None,
        help: Some("Check your permissions for this resource".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Token has expired and needs to be refreshed
pub fn token_expired() -> Diagnostic {
    Diagnostic {
        code: "AUTH_003".to_string(),
        statement: None,
        message: "Authentication token has expired".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Refresh your authentication token".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Token is invalid or malformed
pub fn invalid_token() -> Diagnostic {
    Diagnostic {
        code: "AUTH_004".to_string(),
        statement: None,
        message: "Invalid or malformed authentication token".to_string(),
        column: None,
        span: None,
        label: None,
        help: Some("Provide a valid authentication token".to_string()),
        notes: vec![],
        cause: None,
    }
}
