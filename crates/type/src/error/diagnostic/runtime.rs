// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{error::diagnostic::Diagnostic, fragment::OwnedFragment};

/// Variable is not defined in the current scope
pub fn variable_not_found(name: &str) -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_001".to_string(),
		statement: None,
		message: format!("Variable '{}' is not defined", name),
		column: None,
		fragment: OwnedFragment::None,
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
		fragment: OwnedFragment::None,
		label: None,
		help: Some(format!(
			"Extract a scalar value from the dataframe using '${} | only()', '${} | first()', or '${} | first_or_null()'",
			name, name, name
		)),
		notes: vec![
			"Dataframes must be explicitly converted to scalar values before use in expressions"
				.to_string(),
			"Use .only() for exactly 1 row × 1 column dataframes".to_string(),
			"Use .first() to take the first value from the first column".to_string(),
		],
		cause: None,
	}
}
