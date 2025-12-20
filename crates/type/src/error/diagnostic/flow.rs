// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{error::diagnostic::Diagnostic, fragment::OwnedFragment};

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

/// FlowTransaction keyspace overlap detected
pub fn flow_transaction_keyspace_overlap(key_debug: String) -> Diagnostic {
	Diagnostic {
		code: "FLOW_002".to_string(),
		statement: None,
		message: format!(
			"FlowTransaction keyspace overlap: key {} was already written by another FlowTransaction",
			key_debug
		),
		column: None,
		fragment: OwnedFragment::None,
		label: None,
		help: Some("FlowTransactions must operate on non-overlapping keyspaces. \
			This is typically enforced at the flow scheduler level."
			.to_string()),
		notes: vec![],
		cause: None,
	}
}
