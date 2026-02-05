// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use reifydb_type::{fragment::Fragment, value::r#type::Type};

/// Errors that can occur when executing a scalar function
#[derive(Debug)]
pub enum ScalarFunctionError {
	/// Function called with wrong number of arguments
	ArityMismatch {
		function: Fragment,
		expected: usize,
		actual: usize,
	},
	/// Argument has invalid type
	InvalidArgumentType {
		function: Fragment,
		argument_index: usize,
		expected: Vec<Type>,
		actual: Type,
	},
	/// Function execution failed
	ExecutionFailed {
		function: Fragment,
		reason: String,
	},
	/// Wrapped error from underlying operation
	Wrapped(Box<reifydb_type::error::Error>),
}

impl Display for ScalarFunctionError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			ScalarFunctionError::ArityMismatch {
				function,
				expected,
				actual,
			} => write!(f, "Function {} expects {} arguments, got {}", function.text(), expected, actual),
			ScalarFunctionError::InvalidArgumentType {
				function,
				argument_index,
				expected,
				actual,
			} => {
				let expected_types =
					expected.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ");
				write!(
					f,
					"Function {} argument {} has invalid type: expected one of [{}], got {:?}",
					function.text(),
					argument_index + 1,
					expected_types,
					actual
				)
			}
			ScalarFunctionError::ExecutionFailed {
				function,
				reason,
			} => {
				write!(f, "Function {} execution failed: {}", function.text(), reason)
			}
			ScalarFunctionError::Wrapped(err) => {
				write!(f, "{}", err)
			}
		}
	}
}

impl From<reifydb_type::error::Error> for ScalarFunctionError {
	fn from(err: reifydb_type::error::Error) -> Self {
		ScalarFunctionError::Wrapped(Box::new(err))
	}
}

impl std::error::Error for ScalarFunctionError {}

impl From<ScalarFunctionError> for reifydb_type::error::Error {
	fn from(err: ScalarFunctionError) -> Self {
		use reifydb_type::error::diagnostic::function::{
			arity_mismatch, execution_failed, invalid_argument_type,
		};

		match err {
			ScalarFunctionError::ArityMismatch {
				function,
				expected,
				actual,
			} => reifydb_type::error!(arity_mismatch(function, expected, actual)),
			ScalarFunctionError::InvalidArgumentType {
				function,
				argument_index,
				expected,
				actual,
			} => reifydb_type::error!(invalid_argument_type(function, argument_index, expected, actual)),
			ScalarFunctionError::ExecutionFailed {
				function,
				reason,
			} => reifydb_type::error!(execution_failed(function, reason)),
			ScalarFunctionError::Wrapped(err) => *err,
		}
	}
}

/// Errors that can occur when executing an aggregate function
#[derive(Debug)]
pub enum AggregateFunctionError {
	/// Function called with wrong number of arguments
	ArityMismatch {
		function: Fragment,
		expected: usize,
		actual: usize,
	},
	/// Argument has invalid type
	InvalidArgumentType {
		function: Fragment,
		argument_index: usize,
		expected: Vec<Type>,
		actual: Type,
	},
	/// Function execution failed
	ExecutionFailed {
		function: Fragment,
		reason: String,
	},
	/// Wrapped error from underlying operation
	Wrapped(Box<reifydb_type::error::Error>),
}

impl Display for AggregateFunctionError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			AggregateFunctionError::ArityMismatch {
				function,
				expected,
				actual,
			} => write!(f, "Function {} expects {} arguments, got {}", function.text(), expected, actual),
			AggregateFunctionError::InvalidArgumentType {
				function,
				argument_index,
				expected,
				actual,
			} => {
				let expected_types =
					expected.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ");
				write!(
					f,
					"Function {} argument {} has invalid type: expected one of [{}], got {:?}",
					function.text(),
					argument_index + 1,
					expected_types,
					actual
				)
			}
			AggregateFunctionError::ExecutionFailed {
				function,
				reason,
			} => {
				write!(f, "Function {} execution failed: {}", function.text(), reason)
			}
			AggregateFunctionError::Wrapped(err) => {
				write!(f, "{}", err)
			}
		}
	}
}

impl std::error::Error for AggregateFunctionError {}

impl From<reifydb_type::error::Error> for AggregateFunctionError {
	fn from(err: reifydb_type::error::Error) -> Self {
		AggregateFunctionError::Wrapped(Box::new(err))
	}
}

impl From<AggregateFunctionError> for reifydb_type::error::Error {
	fn from(err: AggregateFunctionError) -> Self {
		use reifydb_type::error::diagnostic::function::{
			arity_mismatch, execution_failed, invalid_argument_type,
		};

		match err {
			AggregateFunctionError::ArityMismatch {
				function,
				expected,
				actual,
			} => reifydb_type::error!(arity_mismatch(function, expected, actual)),
			AggregateFunctionError::InvalidArgumentType {
				function,
				argument_index,
				expected,
				actual,
			} => reifydb_type::error!(invalid_argument_type(function, argument_index, expected, actual)),
			AggregateFunctionError::ExecutionFailed {
				function,
				reason,
			} => reifydb_type::error!(execution_failed(function, reason)),
			AggregateFunctionError::Wrapped(err) => *err,
		}
	}
}

/// Errors that can occur when executing a generator function
#[derive(Debug)]
pub enum GeneratorFunctionError {
	/// Function called with wrong number of arguments
	ArityMismatch {
		function: Fragment,
		expected: usize,
		actual: usize,
	},
	/// Argument has invalid type
	InvalidArgumentType {
		function: Fragment,
		argument_index: usize,
		expected: Vec<Type>,
		actual: Type,
	},
	/// Function execution failed
	ExecutionFailed {
		function: Fragment,
		reason: String,
	},
	/// Generator function not found
	NotFound {
		function: Fragment,
	},
	/// Wrapped error from underlying operation
	Wrapped(Box<reifydb_type::error::Error>),
}

impl Display for GeneratorFunctionError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			GeneratorFunctionError::ArityMismatch {
				function,
				expected,
				actual,
			} => write!(f, "Function {} expects {} arguments, got {}", function.text(), expected, actual),
			GeneratorFunctionError::InvalidArgumentType {
				function,
				argument_index,
				expected,
				actual,
			} => {
				let expected_types =
					expected.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ");
				write!(
					f,
					"Function {} argument {} has invalid type: expected one of [{}], got {:?}",
					function.text(),
					argument_index + 1,
					expected_types,
					actual
				)
			}
			GeneratorFunctionError::ExecutionFailed {
				function,
				reason,
			} => {
				write!(f, "Function {} execution failed: {}", function.text(), reason)
			}
			GeneratorFunctionError::NotFound {
				function,
			} => {
				write!(f, "Generator function '{}' not found", function.text())
			}
			GeneratorFunctionError::Wrapped(err) => {
				write!(f, "{}", err)
			}
		}
	}
}

impl std::error::Error for GeneratorFunctionError {}

impl From<reifydb_type::error::Error> for GeneratorFunctionError {
	fn from(err: reifydb_type::error::Error) -> Self {
		GeneratorFunctionError::Wrapped(Box::new(err))
	}
}

impl From<GeneratorFunctionError> for reifydb_type::error::Error {
	fn from(err: GeneratorFunctionError) -> Self {
		use reifydb_type::error::diagnostic::function::{
			arity_mismatch, execution_failed, generator_not_found, invalid_argument_type,
		};

		match err {
			GeneratorFunctionError::ArityMismatch {
				function,
				expected,
				actual,
			} => reifydb_type::error!(arity_mismatch(function, expected, actual)),
			GeneratorFunctionError::InvalidArgumentType {
				function,
				argument_index,
				expected,
				actual,
			} => reifydb_type::error!(invalid_argument_type(function, argument_index, expected, actual)),
			GeneratorFunctionError::ExecutionFailed {
				function,
				reason,
			} => reifydb_type::error!(execution_failed(function, reason)),
			GeneratorFunctionError::NotFound {
				function,
			} => reifydb_type::error!(generator_not_found(function)),
			GeneratorFunctionError::Wrapped(err) => *err,
		}
	}
}

/// Result type for scalar function operations
pub type ScalarFunctionResult<T> = Result<T, ScalarFunctionError>;

/// Result type for aggregate function operations
pub type AggregateFunctionResult<T> = Result<T, AggregateFunctionError>;

/// Result type for generator function operations
pub type GeneratorFunctionResult<T> = Result<T, GeneratorFunctionError>;
