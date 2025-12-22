// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{Fragment, error::diagnostic::Diagnostic};

/// Variable is not defined in the current scope
pub fn variable_not_found(name: &str) -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_001".to_string(),
		statement: None,
		message: format!("Variable '{}' is not defined", name),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some(format!("Define the variable using 'let {} = <value>' before using it", name)),
		notes: vec![],
		cause: None,
	}
}

/// Variable contains a dataframe and cannot be used directly in scalar expressions
pub fn variable_is_dataframe(name: &str) -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_002".to_string(),
		statement: None,
		message: format!(
			"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
			name
		),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some(format!(
			"Extract a scalar value from the dataframe using '${} | only()', '${} | first()', or '${} | first_or_undefined()'",
			name, name, name
		)),
		notes: vec![
			"Dataframes must be explicitly converted to scalar values before use in expressions"
				.to_string(),
			"Use only() for exactly 1 row × 1 column dataframes".to_string(),
			"Use first() to take the first value from the first column".to_string(),
		],
		cause: None,
	}
}

/// Variable is immutable and cannot be reassigned
pub fn variable_is_immutable(name: &str) -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_003".to_string(),
		statement: None,
		message: format!("Cannot reassign immutable variable '{}'", name),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Use 'let mut $name := value' to declare a mutable variable".to_string()),
		notes: vec!["Only mutable variables can be reassigned".to_string()],
		cause: None,
	}
}
