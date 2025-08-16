// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{Type, DiagnosticOrigin, result::error::diagnostic::Diagnostic};

/// Function is not recognized or does not exist
pub fn unknown_function(name: String) -> Diagnostic {
	Diagnostic {
		code: "FN_001".to_string(),
		statement: None,
		message: format!("Unknown function: {}", name),
		column: None,
		origin: DiagnosticOrigin::None,
		label: None,
		help: Some("Check the function name and available functions"
			.to_string()),
		notes: vec![],
		cause: None,
	}
}

/// Function called with wrong number of arguments
pub fn arity_mismatch(
	function: String,
	expected: usize,
	actual: usize,
) -> Diagnostic {
	Diagnostic {
		code: "FN_002".to_string(),
		statement: None,
		message: format!(
			"Function {} expects {} arguments, got {}",
			function, expected, actual
		),
		column: None,
		origin: DiagnosticOrigin::None,
		label: None,
		help: Some(format!(
			"Provide exactly {} arguments to function {}",
			expected, function
		)),
		notes: vec![],
		cause: None,
	}
}

/// Too many arguments provided to function
pub fn too_many_arguments(
	function: String,
	max_args: usize,
	actual: usize,
) -> Diagnostic {
	Diagnostic {
		code: "FN_003".to_string(),
		statement: None,
		message: format!(
			"Function {} accepts at most {} arguments, got {}",
			function, max_args, actual
		),
		column: None,
		origin: DiagnosticOrigin::None,
		label: None,
		help: Some(format!(
			"Provide at most {} arguments to function {}",
			max_args, function
		)),
		notes: vec![],
		cause: None,
	}
}

/// Argument has invalid type for function
pub fn invalid_argument_type(
	function: String,
	index: usize,
	expected_one_of: Vec<Type>,
	actual: Type,
) -> Diagnostic {
	let expected_types = expected_one_of
		.iter()
		.map(|t| format!("{:?}", t))
		.collect::<Vec<_>>()
		.join(", ");

	Diagnostic {
		code: "FN_004".to_string(),
		statement: None,
		message: format!(
			"Function {} argument {} has invalid type: expected one of [{}], got {:?}",
			function,
			index + 1,
			expected_types,
			actual
		),
		column: None,
		origin: DiagnosticOrigin::None,
		label: None,
		help: Some(format!(
			"Provide an argument of type: {}",
			expected_types
		)),
		notes: vec![],
		cause: None,
	}
}

/// Argument is undefined/null when a value is required
pub fn undefined_argument(function: String, index: usize) -> Diagnostic {
	Diagnostic {
		code: "FN_005".to_string(),
		statement: None,
		message: format!(
			"Function {} argument {} is undefined",
			function,
			index + 1
		),
		column: None,
		origin: DiagnosticOrigin::None,
		label: None,
		help: Some(
			"Provide a defined value for this argument".to_string()
		),
		notes: vec![],
		cause: None,
	}
}

/// Function requires input but none was provided
pub fn missing_input(function: String) -> Diagnostic {
	Diagnostic {
		code: "FN_006".to_string(),
		statement: None,
		message: format!(
			"Function {} requires input but none was provided",
			function
		),
		column: None,
		origin: DiagnosticOrigin::None,
		label: None,
		help: Some("Provide input data to the function".to_string()),
		notes: vec![],
		cause: None,
	}
}

/// Function execution failed with a specific reason
pub fn execution_failed(function: String, reason: String) -> Diagnostic {
	Diagnostic {
		code: "FN_007".to_string(),
		statement: None,
		message: format!(
			"Function {} execution failed: {}",
			function, reason
		),
		column: None,
		origin: DiagnosticOrigin::None,
		label: None,
		help: Some("Check function arguments and data".to_string()),
		notes: vec![],
		cause: None,
	}
}

/// Internal function error - should not normally occur
pub fn internal_error(function: String, details: String) -> Diagnostic {
	Diagnostic {
		code: "FN_008".to_string(),
		statement: None,
		message: format!(
			"Internal error in function {}: {}",
			function, details
		),
		column: None,
		origin: DiagnosticOrigin::None,
		label: None,
		help: Some(
			"This is an internal error - please report this issue"
				.to_string(),
		),
		notes: vec![],
		cause: None,
	}
}
