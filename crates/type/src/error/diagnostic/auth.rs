// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{error::diagnostic::Diagnostic, fragment::Fragment};

/// Authentication failed due to invalid credentials or other reasons
pub fn authentication_failed(reason: String) -> Diagnostic {
	Diagnostic {
		code: "ASVTH_001".to_string(),
		statement: None,
		message: format!("Authentication failed: {}", reason),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check your credentials and try again".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Authorization denied for accessing a resource
pub fn authorization_denied(resource: String) -> Diagnostic {
	Diagnostic {
		code: "ASVTH_002".to_string(),
		statement: None,
		message: format!("Authorization denied for resource: {}", resource),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check your permissions for this resource".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Token has expired and needs to be refreshed
pub fn token_expired() -> Diagnostic {
	Diagnostic {
		code: "ASVTH_003".to_string(),
		statement: None,
		message: "Authentication token has expired".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Refresh your authentication token".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Token is invalid or malformed
pub fn invalid_token() -> Diagnostic {
	Diagnostic {
		code: "ASVTH_004".to_string(),
		statement: None,
		message: "Invalid or malformed authentication token".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Provide a valid authentication token".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
