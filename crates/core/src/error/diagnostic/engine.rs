// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{error::Diagnostic, fragment::Fragment};

pub fn frame_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "ENG_001".to_string(),
		rql: None,
		message: format!("Frame processing error: {}", message),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check frame data and operations".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn missing_row_number_column() -> Diagnostic {
	Diagnostic {
		code: "ENG_003".to_string(),
		rql: None,
		message: "Frame must have a __ROW__ID__ column for UPDATE operations".to_string(),
		column: None,
		fragment: Fragment::None,
		label: Some("missing required column".to_string()),
		help: Some("Ensure the query includes the encoded ID in the result set".to_string()),
		notes: vec!["UPDATE operations require encoded identifiers to locate existing rows".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn read_only_rejection(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "ENG_007".to_string(),
		rql: None,
		message: "Cannot execute write operations on a read-only engine".to_string(),
		column: None,
		fragment,
		label: Some("write rejected".to_string()),
		help: Some("Disable read-only mode before issuing write operations (admin, command, subscription)"
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
