// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{Fragment, error::diagnostic::Diagnostic};

pub fn invalid_uuid4_format(fragment: Fragment) -> Diagnostic {
	let label = Some(format!("'{}' is not a valid UUID v4", fragment.text()));

	let help = "use UUID v4 format (e.g., 550e8400-e29b-41d4-a716-446655440000)".to_string();
	let notes = vec![
		"valid: 550e8400-e29b-41d4-a716-446655440000".to_string(),
		"valid: f47ac10b-58cc-4372-a567-0e02b2c3d479".to_string(),
		"UUID v4 uses random or pseudo-random numbers".to_string(),
	];

	Diagnostic {
		code: "UUID_001".to_string(),
		statement: None,
		message: "invalid UUID v4 format".to_string(),
		fragment,
		label,
		help: Some(help),
		notes,
		column: None,
		cause: None,
		operator_chain: None,
	}
}

pub fn invalid_uuid7_format(fragment: Fragment) -> Diagnostic {
	let label = Some(format!("'{}' is not a valid UUID v7", fragment.text()));

	let help = "use UUID v7 format (e.g., 017f22e2-79b0-7cc3-98c4-dc0c0c07398f)".to_string();
	let notes = vec![
		"valid: 017f22e2-79b0-7cc3-98c4-dc0c0c07398f".to_string(),
		"valid: 01854d6e-bd60-7b28-a3c7-6b4ad2c4e2e8".to_string(),
		"UUID v7 uses timestamp-based generation".to_string(),
	];

	Diagnostic {
		code: "UUID_002".to_string(),
		statement: None,
		message: "invalid UUID v7 format".to_string(),
		fragment,
		label,
		help: Some(help),
		notes,
		column: None,
		cause: None,
		operator_chain: None,
	}
}
