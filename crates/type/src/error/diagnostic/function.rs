// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{error::diagnostic::Diagnostic, fragment::Fragment, value::r#type::Type};

/// Function is not recognized or does not exist
pub fn unknown_function(function: Fragment) -> Diagnostic {
	let name = function.text().to_string();
	Diagnostic {
		code: "FUNCTION_001".to_string(),
		statement: None,
		message: format!("Unknown function: {}", name),
		column: None,
		fragment: function,
		label: Some("unknown function".to_string()),
		help: Some("Check the function name and available functions".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Function called with wrong number of arguments
pub fn arity_mismatch(function: Fragment, expected: usize, actual: usize) -> Diagnostic {
	let name = function.text().to_string();
	Diagnostic {
		code: "FUNCTION_002".to_string(),
		statement: None,
		message: format!("Function {} expects {} arguments, got {}", name, expected, actual),
		column: None,
		fragment: function,
		label: Some("wrong number of arguments".to_string()),
		help: Some(format!("Provide exactly {} arguments to function {}", expected, name)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Too many arguments provided to function
pub fn too_many_arguments(function: Fragment, max_args: usize, actual: usize) -> Diagnostic {
	let name = function.text().to_string();
	Diagnostic {
		code: "FUNCTION_003".to_string(),
		statement: None,
		message: format!("Function {} accepts at most {} arguments, got {}", name, max_args, actual),
		column: None,
		fragment: function,
		label: Some("too many arguments".to_string()),
		help: Some(format!("Provide at most {} arguments to function {}", max_args, name)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Argument has invalid type for function
pub fn invalid_argument_type(function: Fragment, index: usize, expected_one_of: Vec<Type>, actual: Type) -> Diagnostic {
	let name = function.text().to_string();
	let expected_types = expected_one_of.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ");

	Diagnostic {
		code: "FUNCTION_004".to_string(),
		statement: None,
		message: format!(
			"Function {} argument {} has invalid type: expected one of [{}], got {:?}",
			name,
			index + 1,
			expected_types,
			actual
		),
		column: None,
		fragment: function,
		label: Some("invalid argument type".to_string()),
		help: Some(format!("Provide an argument of type: {}", expected_types)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Argument is undefined/null when a value is required
pub fn undefined_argument(function: Fragment, index: usize) -> Diagnostic {
	let name = function.text().to_string();
	Diagnostic {
		code: "FUNCTION_005".to_string(),
		statement: None,
		message: format!("Function {} argument {} is none", name, index + 1),
		column: None,
		fragment: function,
		label: Some("none argument".to_string()),
		help: Some("Provide a defined value for this argument".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Function requires input but none was provided
pub fn missing_input(function: Fragment) -> Diagnostic {
	let name = function.text().to_string();
	Diagnostic {
		code: "FUNCTION_006".to_string(),
		statement: None,
		message: format!("Function {} requires input but none was provided", name),
		column: None,
		fragment: function,
		label: Some("missing input".to_string()),
		help: Some("Provide input data to the function".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Function execution failed with a specific reason
pub fn execution_failed(function: Fragment, reason: String) -> Diagnostic {
	let name = function.text().to_string();
	Diagnostic {
		code: "FUNCTION_007".to_string(),
		statement: None,
		message: format!("Function {} execution failed: {}", name, reason),
		column: None,
		fragment: function,
		label: Some("execution failed".to_string()),
		help: Some("Check function arguments and data".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Internal function error - should not normally occur
pub fn internal_error(function: Fragment, details: String) -> Diagnostic {
	let name = function.text().to_string();
	Diagnostic {
		code: "FUNCTION_008".to_string(),
		statement: None,
		message: format!("Internal error in function {}: {}", name, details),
		column: None,
		fragment: function,
		label: Some("internal error".to_string()),
		help: Some("This is an internal error - please report this issue".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Generator function is not recognized or does not exist
pub fn generator_not_found(function: Fragment) -> Diagnostic {
	let name = function.text().to_string();
	Diagnostic {
		code: "FUNCTION_009".to_string(),
		statement: None,
		message: format!("Generator function '{}' not found", name),
		column: None,
		fragment: function,
		label: Some("unknown generator function".to_string()),
		help: Some("Check the generator function name and ensure it is registered".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
