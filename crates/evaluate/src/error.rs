// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

#[derive(Debug, thiserror::Error)]
pub enum EvaluateError {
	#[error("Unknown function: {name}")]
	UnknownFunction {
		name: String,
		fragment: Fragment,
	},

	#[error("Variable '{name}' is not defined")]
	VariableNotFound {
		name: String,
	},

	#[error("Cannot reassign immutable variable '{name}'")]
	VariableIsImmutable {
		name: String,
	},
}

impl IntoDiagnostic for EvaluateError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			EvaluateError::UnknownFunction {
				name,
				fragment,
			} => Diagnostic {
				code: "FUNCTION_001".to_string(),
				rql: None,
				message: format!("Unknown function: {}", name),
				column: None,
				fragment,
				label: Some("unknown function".to_string()),
				help: Some("Check the function name and available functions".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EvaluateError::VariableNotFound {
				name,
			} => Diagnostic {
				code: "RUNTIME_001".to_string(),
				rql: None,
				message: format!("Variable '{}' is not defined", name),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some(format!(
					"Define the variable using 'let {} = <value>' before using it",
					name
				)),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EvaluateError::VariableIsImmutable {
				name,
			} => Diagnostic {
				code: "RUNTIME_003".to_string(),
				rql: None,
				message: format!("Cannot reassign immutable variable '{}'", name),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Use 'let mut $name := value' to declare a mutable variable".to_string()),
				notes: vec!["Only mutable variables can be reassigned".to_string()],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<EvaluateError> for Error {
	fn from(err: EvaluateError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}
