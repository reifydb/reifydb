// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{error::diagnostic::Diagnostic, fragment::Fragment};

/// Serde deserialization error
pub fn serde_deserialize_error(msg: String) -> Diagnostic {
	Diagnostic {
		code: "SERDE_001".to_string(),
		statement: None,
		message: format!("Serde deserialization error: {}", msg),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check data format and structure".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Serde serialization error
pub fn serde_serialize_error(msg: String) -> Diagnostic {
	Diagnostic {
		code: "SERDE_002".to_string(),
		statement: None,
		message: format!("Serde serialization error: {}", msg),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check data format and structure".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Keycode-specific serialization error
pub fn serde_keycode_error(msg: String) -> Diagnostic {
	Diagnostic {
		code: "SERDE_003".to_string(),
		statement: None,
		message: format!("Keycode serialization error: {}", msg),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check keycode data and format".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
