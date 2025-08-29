// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	OwnedFragment, interface::fragment::IntoFragment,
	result::error::diagnostic::Diagnostic,
};

/// General frame processing error
pub fn frame_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "ENG_001".to_string(),
		statement: None,
		message: format!("Frame processing error: {}", message),
		column: None,
		fragment: OwnedFragment::None,
		label: None,
		help: Some("Check frame data and operations".to_string()),
		notes: vec![],
		cause: None,
	}
}

/// Column policy saturation error - wraps an existing diagnostic
pub fn saturation_error(diagnostic: Diagnostic) -> Diagnostic {
	let statement = diagnostic.statement.clone();
	let message = diagnostic.message.clone();
	let column = diagnostic.column.clone();
	let fragment = diagnostic.fragment.clone();
	let label = diagnostic.label.clone();
	let notes = diagnostic.notes.clone();

	Diagnostic {
		code: "ENG_002".to_string(),
		statement,
		message: format!("Column policy saturation: {}", message),
		column,
		fragment,
		label,
		help: Some("Adjust column policy constraints".to_string()),
		notes,
		cause: Some(Box::new(diagnostic)),
	}
}

/// Frame missing required ROW_NUMBER column error
pub fn missing_row_number_column() -> Diagnostic {
	Diagnostic {
        code: "ENG_003".to_string(),
        statement: None,
        message: "Frame must have a __ROW__ID__ column for UPDATE operations".to_string(),
        column: None,
        fragment: OwnedFragment::None,
        label: Some("missing required column".to_string()),
        help: Some("Ensure the query includes the row ID in the result set".to_string()),
        notes: vec![
            "UPDATE operations require row identifiers to locate existing rows".to_string(),
        ],
        cause: None,
    }
}

/// Invalid or undefined RowNumber values error
pub fn invalid_row_number_values() -> Diagnostic {
	Diagnostic {
        code: "ENG_004".to_string(),
        statement: None,
        message: "All RowNumber values must be defined for UPDATE operations".to_string(),
        column: None,
        fragment: OwnedFragment::None,
        label: Some("invalid row identifiers".to_string()),
        help: Some("Check that the input data contains valid row IDs".to_string()),
        notes: vec![
            "RowNumber column must contain valid identifiers, not undefined values".to_string(),
        ],
        cause: None,
    }
}

/// Invalid parameter reference error
pub fn invalid_parameter_reference<'a>(
	fragment: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	let value = fragment.text();
	Diagnostic {
        code: "ENG_005".to_string(),
        statement: None,
        message: format!("Invalid parameter reference: {}", value),
        column: None,
        fragment,
        label: Some("invalid parameter syntax".to_string()),
        help: Some("Use $1, $2 for positional parameters or $name for named parameters".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Parameter not found error
pub fn parameter_not_found<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	let value = fragment.text();
	Diagnostic {
        code: "ENG_006".to_string(),
        statement: None,
        message: format!("Parameter not found: {}", value),
        column: None,
        fragment,
        label: Some("parameter not provided".to_string()),
        help: Some("Ensure all referenced parameters are provided in the query call".to_string()),
        notes: vec![],
        cause: None,
    }
}
