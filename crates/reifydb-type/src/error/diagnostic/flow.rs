// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::error::diagnostic::Diagnostic;
use crate::fragment::OwnedFragment;

/// View flow processing error
pub fn flow_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "FLOW_001".to_string(),
		statement: None,
		message: format!("Flow processing error: {}", message),
		column: None,
		fragment: OwnedFragment::None,
		label: None,
		help: Some("Check view flow configuration".to_string()),
		notes: vec![],
		cause: None,
	}
}
