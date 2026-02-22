// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::r#type::Type,
};

#[derive(Debug, thiserror::Error)]
pub enum FunctionError {
	#[error("function {} expects {expected} arguments, got {actual}", function.text())]
	ArityMismatch {
		function: Fragment,
		expected: usize,
		actual: usize,
	},

	#[error("function {} argument {} has invalid type: got {actual:?}", function.text(), argument_index + 1)]
	InvalidArgumentType {
		function: Fragment,
		argument_index: usize,
		expected: Vec<Type>,
		actual: Type,
	},

	#[error("function {} execution failed: {reason}", function.text())]
	ExecutionFailed {
		function: Fragment,
		reason: String,
	},

	#[error("generator function '{}' not found", function.text())]
	NotFound {
		function: Fragment,
	},

	#[error(transparent)]
	Wrapped(Box<Error>),
}

impl From<Error> for FunctionError {
	fn from(err: Error) -> Self {
		FunctionError::Wrapped(Box::new(err))
	}
}

impl From<FunctionError> for Error {
	fn from(err: FunctionError) -> Self {
		Error(err.into_diagnostic())
	}
}

impl IntoDiagnostic for FunctionError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			FunctionError::ArityMismatch {
				function,
				expected,
				actual,
			} => {
				let name = function.text().to_string();
				Diagnostic {
					code: "FUNCTION_002".to_string(),
					statement: None,
					message: format!(
						"Function {} expects {} arguments, got {}",
						name, expected, actual
					),
					column: None,
					fragment: function,
					label: Some("wrong number of arguments".to_string()),
					help: Some(format!(
						"Provide exactly {} arguments to function {}",
						expected, name
					)),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
			FunctionError::InvalidArgumentType {
				function,
				argument_index,
				expected,
				actual,
			} => {
				let name = function.text().to_string();
				let expected_types =
					expected.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ");
				Diagnostic {
					code: "FUNCTION_004".to_string(),
					statement: None,
					message: format!(
						"Function {} argument {} has invalid type: expected one of [{}], got {:?}",
						name,
						argument_index + 1,
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
			FunctionError::ExecutionFailed {
				function,
				reason,
			} => {
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
			FunctionError::NotFound {
				function,
			} => {
				let name = function.text().to_string();
				Diagnostic {
					code: "FUNCTION_009".to_string(),
					statement: None,
					message: format!("Generator function '{}' not found", name),
					column: None,
					fragment: function,
					label: Some("unknown generator function".to_string()),
					help: Some("Check the generator function name and ensure it is registered"
						.to_string()),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
			FunctionError::Wrapped(err) => err.0,
		}
	}
}

pub type ScalarFunctionError = FunctionError;
pub type AggregateFunctionError = FunctionError;
pub type GeneratorFunctionError = FunctionError;

pub type ScalarFunctionResult<T> = Result<T, FunctionError>;
pub type AggregateFunctionResult<T> = Result<T, FunctionError>;
pub type GeneratorFunctionResult<T> = Result<T, FunctionError>;
