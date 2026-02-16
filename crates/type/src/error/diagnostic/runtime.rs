// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{error::diagnostic::Diagnostic, fragment::Fragment};

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
		operator_chain: None,
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
			"Extract a scalar value from the dataframe using '${} | only()', '${} | first()', or '${} | first_or_none()'",
			name, name, name
		)),
		notes: vec![
			"Dataframes must be explicitly converted to scalar values before use in expressions"
				.to_string(),
			"Use only() for exactly 1 row × 1 column dataframes".to_string(),
			"Use first() to take the first value from the first column".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// BREAK used outside of a loop
pub fn break_outside_loop() -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_004".to_string(),
		statement: None,
		message: "BREAK can only be used inside a loop".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Use BREAK inside a LOOP, WHILE, or FOR block".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// CONTINUE used outside of a loop
pub fn continue_outside_loop() -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_005".to_string(),
		statement: None,
		message: "CONTINUE can only be used inside a loop".to_string(),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Use CONTINUE inside a LOOP, WHILE, or FOR block".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Loop exceeded maximum iteration limit
pub fn max_iterations_exceeded(limit: usize) -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_006".to_string(),
		statement: None,
		message: format!("Loop exceeded maximum iteration limit of {}", limit),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Add a BREAK condition or use WHILE with a terminating condition".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
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
		operator_chain: None,
	}
}

/// Function is not defined
pub fn undefined_function(name: Fragment) -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_007".to_string(),
		statement: None,
		message: format!("Function '{}' is not defined", name.text()),
		column: None,
		fragment: name,
		label: None,
		help: Some("Define the function using 'DEF name [] { ... }' before calling it".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// APPEND target variable is not a Frame
pub fn append_target_not_frame(name: &str) -> Diagnostic {
	Diagnostic {
		code: "RUNTIME_008".to_string(),
		statement: None,
		message: format!("Cannot APPEND to variable '{}' because it is not a Frame", name),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("APPEND can only target Frame variables. Use a new variable name or ensure the target was created by APPEND or FROM".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
