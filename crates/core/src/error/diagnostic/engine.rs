// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::Diagnostic, fragment::Fragment};

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

pub fn saturation_error(diagnostic: Diagnostic) -> Diagnostic {
	let rql = diagnostic.rql.clone();
	let message = diagnostic.message.clone();
	let column = diagnostic.column.clone();
	let fragment = diagnostic.fragment.clone();
	let label = diagnostic.label.clone();
	let notes = diagnostic.notes.clone();

	Diagnostic {
		code: "ENG_002".to_string(),
		rql,
		message: format!("Column policy saturation: {}", message),
		column,
		fragment,
		label,
		help: Some("Adjust column policy constraints".to_string()),
		notes,
		cause: Some(Box::new(diagnostic)),
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

pub fn invalid_row_number_values() -> Diagnostic {
	Diagnostic {
		code: "ENG_004".to_string(),
		rql: None,
		message: "All RowNumber values must be defined for UPDATE operations".to_string(),
		column: None,
		fragment: Fragment::None,
		label: Some("invalid encoded identifiers".to_string()),
		help: Some("Check that the input data contains valid encoded IDs".to_string()),
		notes: vec!["RowNumber column must contain valid identifiers, not none values".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn invalid_parameter_reference(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	Diagnostic {
		code: "ENG_005".to_string(),
		rql: None,
		message: format!("Invalid parameter reference: {}", value),
		column: None,
		fragment,
		label: Some("invalid parameter syntax".to_string()),
		help: Some("Use $1, $2 for positional parameters or $name for named parameters".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn parameter_not_found(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	Diagnostic {
		code: "ENG_006".to_string(),
		rql: None,
		message: format!("Parameter not found: {}", value),
		column: None,
		fragment,
		label: Some("parameter not provided".to_string()),
		help: Some("Ensure all referenced parameters are provided in the query call".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn read_only_rejection(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "ENG_007".to_string(),
		rql: None,
		message: "Cannot execute write operations on a read-only replica".to_string(),
		column: None,
		fragment,
		label: Some("write rejected".to_string()),
		help: Some("Send write operations (admin, command, subscription) to the primary node".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
