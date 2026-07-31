// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::error::CatalogError;
use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic, TypeError},
	fragment::Fragment,
	value::value_type::ValueType,
};

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
	#[error("procedure {procedure} received a malformed queue token: {token}")]
	TokenInvalid {
		procedure: &'static str,
		fragment: Fragment,
		token: String,
	},

	#[error("queue lease {token} can no longer be extended: {reason}")]
	ExtendStale {
		procedure: &'static str,
		fragment: Fragment,
		token: String,
		reason: String,
	},

	#[error("procedure {procedure} found no item {item} in queue {queue}")]
	ReplayUnknownItem {
		procedure: &'static str,
		fragment: Fragment,
		queue: String,
		item: u64,
	},

	#[error("procedure {procedure} cannot replay item {item} of queue {queue}: it is {status}, not dead")]
	ReplayNotDead {
		procedure: &'static str,
		fragment: Fragment,
		queue: String,
		item: u64,
		status: String,
	},
}

impl IntoDiagnostic for QueueError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			QueueError::TokenInvalid {
				procedure,
				fragment,
				token,
			} => Diagnostic {
				code: "QUEUE_003".to_string(),
				rql: None,
				message: format!("Procedure {} received a malformed queue token", procedure),
				column: None,
				fragment,
				label: Some("malformed token".to_string()),
				help: Some("Pass the token exactly as queue::claim returned it".to_string()),
				notes: vec![format!("token: {}", token)],
				cause: None,
				operator_chain: None,
			},
			QueueError::ExtendStale {
				procedure: _,
				fragment,
				token,
				reason,
			} => Diagnostic {
				code: "QUEUE_002".to_string(),
				rql: None,
				message: format!("Queue lease can no longer be extended: {}", reason),
				column: None,
				fragment,
				label: Some("stale lease".to_string()),
				help: Some("abandon the task and claim again".to_string()),
				notes: vec![format!("token: {}", token)],
				cause: None,
				operator_chain: None,
			},
			QueueError::ReplayUnknownItem {
				procedure: _,
				fragment,
				queue,
				item,
			} => Diagnostic {
				code: "QUEUE_004".to_string(),
				rql: None,
				message: format!("Queue {} holds no item {}", queue, item),
				column: None,
				fragment,
				label: Some("unknown item".to_string()),
				help: Some(
					"Check the item number; retention may already have swept it, which closes its replay window"
						.to_string(),
				),
				notes: vec![format!("queue: {}", queue), format!("item: {}", item)],
				cause: None,
				operator_chain: None,
			},
			QueueError::ReplayNotDead {
				procedure: _,
				fragment,
				queue,
				item,
				status,
			} => Diagnostic {
				code: "QUEUE_005".to_string(),
				rql: None,
				message: format!("Queue item {} is {}, not dead", item, status),
				column: None,
				fragment,
				label: Some("item is not dead".to_string()),
				help: Some("Only a dead item can be replayed".to_string()),
				notes: vec![
					format!("queue: {}", queue),
					format!("item: {}", item),
					format!("status: {}", status),
				],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<QueueError> for Error {
	fn from(err: QueueError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

#[derive(Debug, thiserror::Error)]
pub enum RoutineError {
	#[error("function {} expects {expected} arguments, got {actual}", function.text())]
	FunctionArityMismatch {
		function: Fragment,
		expected: usize,
		actual: usize,
	},

	#[error("function {} argument {} has invalid type: got {actual:?}", function.text(), argument_index + 1)]
	FunctionInvalidArgumentType {
		function: Fragment,
		argument_index: usize,
		expected: Vec<ValueType>,
		actual: ValueType,
	},

	#[error("function {} execution failed: {reason}", function.text())]
	FunctionExecutionFailed {
		function: Fragment,
		reason: String,
	},

	#[error("generator function '{}' not found", function.text())]
	FunctionNotFound {
		function: Fragment,
	},

	#[error("procedure {} expects {expected} arguments, got {actual}", procedure.text())]
	ProcedureArityMismatch {
		procedure: Fragment,
		expected: usize,
		actual: usize,
	},

	#[error("procedure {} argument {} has invalid type: got {actual:?}", procedure.text(), argument_index + 1)]
	ProcedureInvalidArgumentType {
		procedure: Fragment,
		argument_index: usize,
		expected: Vec<ValueType>,
		actual: ValueType,
	},

	#[error("procedure {} execution failed: {reason}", procedure.text())]
	ProcedureExecutionFailed {
		procedure: Fragment,
		reason: String,
	},

	#[error(transparent)]
	Queue(#[from] QueueError),

	#[error("operation '{op}' is not supported by accumulator '{accumulator}'")]
	Unsupported {
		op: &'static str,
		accumulator: &'static str,
	},

	#[error(transparent)]
	Wrapped(Box<Error>),
}

impl From<Error> for RoutineError {
	fn from(err: Error) -> Self {
		RoutineError::Wrapped(Box::new(err))
	}
}

impl From<CatalogError> for RoutineError {
	fn from(err: CatalogError) -> Self {
		RoutineError::Wrapped(Box::new(Error::from(err)))
	}
}

impl From<TypeError> for RoutineError {
	fn from(err: TypeError) -> Self {
		RoutineError::Wrapped(Box::new(Error::from(err)))
	}
}

impl From<Box<TypeError>> for RoutineError {
	fn from(err: Box<TypeError>) -> Self {
		RoutineError::Wrapped(Box::new(Error::from(err)))
	}
}

impl From<RoutineError> for Error {
	fn from(err: RoutineError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

impl IntoDiagnostic for RoutineError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			RoutineError::FunctionArityMismatch {
				function,
				expected,
				actual,
			} => {
				let name = function.text().to_string();
				Diagnostic {
					code: "FUNCTION_002".to_string(),
					rql: None,
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
			RoutineError::FunctionInvalidArgumentType {
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
					rql: None,
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
			RoutineError::FunctionExecutionFailed {
				function,
				reason,
			} => {
				let name = function.text().to_string();
				Diagnostic {
					code: "FUNCTION_007".to_string(),
					rql: None,
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
			RoutineError::FunctionNotFound {
				function,
			} => {
				let name = function.text().to_string();
				Diagnostic {
					code: "FUNCTION_009".to_string(),
					rql: None,
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
			RoutineError::ProcedureArityMismatch {
				procedure,
				expected,
				actual,
			} => {
				let name = procedure.text().to_string();
				Diagnostic {
					code: "PROCEDURE_001".to_string(),
					rql: None,
					message: format!(
						"Procedure {} expects {} arguments, got {}",
						name, expected, actual
					),
					column: None,
					fragment: procedure,
					label: Some("wrong number of arguments".to_string()),
					help: Some(format!(
						"Provide exactly {} arguments to procedure {}",
						expected, name
					)),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
			RoutineError::ProcedureInvalidArgumentType {
				procedure,
				argument_index,
				expected,
				actual,
			} => {
				let name = procedure.text().to_string();
				let expected_types =
					expected.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ");
				Diagnostic {
					code: "PROCEDURE_002".to_string(),
					rql: None,
					message: format!(
						"Procedure {} argument {} has invalid type: expected one of [{}], got {:?}",
						name,
						argument_index + 1,
						expected_types,
						actual
					),
					column: None,
					fragment: procedure,
					label: Some("invalid argument type".to_string()),
					help: Some(format!("Provide an argument of type: {}", expected_types)),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
			RoutineError::ProcedureExecutionFailed {
				procedure,
				reason,
			} => {
				let name = procedure.text().to_string();
				Diagnostic {
					code: "PROCEDURE_003".to_string(),
					rql: None,
					message: format!("Procedure {} execution failed: {}", name, reason),
					column: None,
					fragment: procedure,
					label: Some("execution failed".to_string()),
					help: Some("Check procedure arguments and context".to_string()),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
			RoutineError::Queue(err) => err.into_diagnostic(),
			RoutineError::Unsupported {
				op,
				accumulator,
			} => Diagnostic {
				code: "ACCUMULATOR_001".to_string(),
				rql: None,
				message: format!(
					"operation '{}' is not supported by accumulator '{}'",
					op, accumulator
				),
				column: None,
				fragment: Fragment::none(),
				label: Some("unsupported operation".to_string()),
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			RoutineError::Wrapped(err) => *err.0,
		}
	}
}

impl RoutineError {
	pub fn with_context(self, fragment: Fragment, is_procedure: bool) -> Error {
		match self {
			RoutineError::Wrapped(inner) => {
				let name = fragment.text().to_string();
				let mut cause = *inner.0;
				cause.with_fragment(fragment.clone());
				let (code, message, help) = if is_procedure {
					(
						"PROCEDURE_003",
						format!("Procedure {} execution failed", name),
						"Check procedure arguments and context",
					)
				} else {
					(
						"FUNCTION_007",
						format!("Function {} execution failed", name),
						"Check function arguments and data",
					)
				};
				Error(Box::new(Diagnostic {
					code: code.to_string(),
					rql: None,
					message,
					column: None,
					fragment,
					label: Some("execution failed".to_string()),
					help: Some(help.to_string()),
					notes: vec![],
					cause: Some(Box::new(cause)),
					operator_chain: None,
				}))
			}
			other => {
				let mut diagnostic = other.into_diagnostic();
				if diagnostic.fragment().is_none() {
					diagnostic.with_fragment(fragment);
				}
				Error(Box::new(diagnostic))
			}
		}
	}
}
