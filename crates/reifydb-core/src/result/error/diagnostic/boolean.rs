// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{IntoDiagnosticOrigin, result::error::diagnostic::Diagnostic};

pub fn invalid_boolean_format(origin: impl IntoDiagnosticOrigin) -> Diagnostic {
	let origin = origin.into_origin();
	let fragment = origin.fragment().unwrap_or("");
	let label = Some(format!(
		"expected 'true' or 'false', found '{}'",
		fragment
	));
	Diagnostic {
		code: "BOOLEAN_001".to_string(),
		statement: None,
		message: "invalid boolean format".to_string(),
		origin: origin,
		label,
		help: Some("use 'true' or 'false'".to_string()),
		notes: vec![
			"valid: true, TRUE".to_string(),
			"valid: false, FALSE".to_string(),
		],
		column: None,
		cause: None,
	}
}

pub fn empty_boolean_value(origin: impl IntoDiagnosticOrigin) -> Diagnostic {
	let origin = origin.into_origin();
	let label = Some("boolean value cannot be empty".to_string());
	Diagnostic {
		code: "BOOLEAN_002".to_string(),
		statement: None,
		message: "empty boolean value".to_string(),
		origin: origin,
		label,
		help: Some("provide either 'true' or 'false'".to_string()),
		notes: vec![
			"valid: true".to_string(),
			"valid: false".to_string(),
		],
		column: None,
		cause: None,
	}
}

pub fn invalid_number_boolean(origin: impl IntoDiagnosticOrigin) -> Diagnostic {
	let origin = origin.into_origin();
	let fragment = origin.fragment().unwrap_or("");
	let label = Some(format!(
		"number '{}' cannot be cast to boolean, only 1 or 0 are allowed",
		fragment
	));
	Diagnostic {
		code: "BOOLEAN_003".to_string(),
		statement: None,
		message: "invalid boolean".to_string(),
		origin: origin,
		label,
		help: Some("use 1 for true or 0 for false".to_string()),
		notes: vec![
			"valid: 1 → true".to_string(),
			"valid: 0 → false".to_string(),
			"invalid: any other number".to_string(),
		],
		column: None,
		cause: None,
	}
}
