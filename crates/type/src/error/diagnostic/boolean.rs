// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{error::diagnostic::Diagnostic, fragment::Fragment};

pub fn invalid_boolean_format(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	let label = Some(format!("expected 'true' or 'false', found '{}'", value));
	Diagnostic {
		code: "BOOLEAN_001".to_string(),
		statement: None,
		message: "invalid boolean format".to_string(),
		fragment,
		label,
		help: Some("use 'true' or 'false'".to_string()),
		notes: vec!["valid: true, TRUE".to_string(), "valid: false, FALSE".to_string()],
		column: None,
		cause: None,
		operator_chain: None,
	}
}

pub fn empty_boolean_value(fragment: Fragment) -> Diagnostic {
	let label = Some("boolean value cannot be empty".to_string());
	Diagnostic {
		code: "BOOLEAN_002".to_string(),
		statement: None,
		message: "empty boolean value".to_string(),
		fragment,
		label,
		help: Some("provide either 'true' or 'false'".to_string()),
		notes: vec!["valid: true".to_string(), "valid: false".to_string()],
		column: None,
		cause: None,
		operator_chain: None,
	}
}

pub fn invalid_number_boolean(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	let label = Some(format!("number '{}' cannot be cast to boolean, only 1 or 0 are allowed", value));
	Diagnostic {
		code: "BOOLEAN_003".to_string(),
		statement: None,
		message: "invalid boolean".to_string(),
		fragment,
		label,
		help: Some("use 1 for true or 0 for false".to_string()),
		notes: vec![
			"valid: 1 → true".to_string(),
			"valid: 0 → false".to_string(),
			"invalid: any other number".to_string(),
		],
		column: None,
		cause: None,
		operator_chain: None,
	}
}
