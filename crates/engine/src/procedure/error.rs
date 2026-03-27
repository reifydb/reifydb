// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_sdk::error::FFIError;
use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic, TypeError},
	fragment::Fragment,
	value::r#type::Type,
};

#[derive(Debug, thiserror::Error)]
pub enum ProcedureError {
	#[error("procedure {} expects {expected} arguments, got {actual}", procedure.text())]
	ArityMismatch {
		procedure: Fragment,
		expected: usize,
		actual: usize,
	},

	#[error("procedure {} argument {} has invalid type: got {actual:?}", procedure.text(), argument_index + 1)]
	InvalidArgumentType {
		procedure: Fragment,
		argument_index: usize,
		expected: Vec<Type>,
		actual: Type,
	},

	#[error("procedure {} execution failed: {reason}", procedure.text())]
	ExecutionFailed {
		procedure: Fragment,
		reason: String,
	},

	#[error(transparent)]
	Wrapped(Box<Error>),
}

impl From<Error> for ProcedureError {
	fn from(err: Error) -> Self {
		ProcedureError::Wrapped(Box::new(err))
	}
}

impl From<TypeError> for ProcedureError {
	fn from(err: TypeError) -> Self {
		ProcedureError::Wrapped(Box::new(Error::from(err)))
	}
}

impl From<FFIError> for ProcedureError {
	fn from(err: FFIError) -> Self {
		ProcedureError::Wrapped(Box::new(err.into()))
	}
}

impl ProcedureError {
	/// Attach procedure name context to Wrapped errors.
	/// Named variants already carry the procedure name and convert normally.
	/// Wrapped variants become PROCEDURE_003 with the inner error as `cause`.
	pub fn with_context(self, procedure: Fragment) -> Error {
		match self {
			ProcedureError::Wrapped(inner) => {
				let name = procedure.text().to_string();
				let mut cause = inner.0;
				cause.with_fragment(procedure.clone());
				Error(Diagnostic {
					code: "PROCEDURE_003".to_string(),
					statement: None,
					message: format!("Procedure {} execution failed", name),
					column: None,
					fragment: procedure,
					label: Some("execution failed".to_string()),
					help: Some("Check procedure arguments and context".to_string()),
					notes: vec![],
					cause: Some(Box::new(cause)),
					operator_chain: None,
				})
			}
			other => Error(other.into_diagnostic()),
		}
	}
}

impl From<ProcedureError> for Error {
	fn from(err: ProcedureError) -> Self {
		Error(err.into_diagnostic())
	}
}

impl IntoDiagnostic for ProcedureError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			ProcedureError::ArityMismatch {
				procedure,
				expected,
				actual,
			} => {
				let name = procedure.text().to_string();
				Diagnostic {
					code: "PROCEDURE_001".to_string(),
					statement: None,
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
			ProcedureError::InvalidArgumentType {
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
					statement: None,
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
			ProcedureError::ExecutionFailed {
				procedure,
				reason,
			} => {
				let name = procedure.text().to_string();
				Diagnostic {
					code: "PROCEDURE_003".to_string(),
					statement: None,
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
			ProcedureError::Wrapped(err) => err.0,
		}
	}
}
