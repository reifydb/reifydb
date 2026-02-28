// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::r#type::Type,
};

#[derive(Debug, thiserror::Error)]
pub enum CastError {
	#[error("unsupported cast from {from_type} to {to_type}")]
	UnsupportedCast {
		fragment: Fragment,
		from_type: Type,
		to_type: Type,
	},

	#[error("failed to cast to {target}")]
	InvalidNumber {
		fragment: Fragment,
		target: Type,
		cause: Diagnostic,
	},

	#[error("failed to cast to bool")]
	InvalidBoolean {
		fragment: Fragment,
		cause: Diagnostic,
	},

	#[error("failed to cast to {target}")]
	InvalidUuid {
		fragment: Fragment,
		target: Type,
		cause: Diagnostic,
	},

	#[error("failed to cast to {target}")]
	InvalidTemporal {
		fragment: Fragment,
		target: Type,
		cause: Diagnostic,
	},

	#[error("failed to cast BLOB to UTF8")]
	InvalidBlobToUtf8 {
		fragment: Fragment,
		cause: Diagnostic,
	},
}

impl IntoDiagnostic for CastError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			CastError::UnsupportedCast { fragment, from_type, to_type } => {
				let label = Some(format!("cannot cast {} of type {} to {}", fragment.text(), from_type, to_type));
				Diagnostic {
					code: "CAST_001".to_string(),
					statement: None,
					message: format!("unsupported cast from {} to {}", from_type, to_type),
					fragment,
					label,
					help: Some("ensure the source and target types are compatible for casting".to_string()),
					notes: vec!["supported casts include: numeric to numeric, string to temporal, boolean to numeric"
						.to_string()],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}
			CastError::InvalidNumber { fragment, target, cause } => Diagnostic {
				code: "CAST_002".to_string(),
				statement: None,
				message: format!("failed to cast to {}", target),
				fragment,
				label: Some(format!("failed to cast to {}", target)),
				help: None,
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
			CastError::InvalidBoolean { fragment, cause } => Diagnostic {
				code: "CAST_004".to_string(),
				statement: None,
				message: "failed to cast to bool".to_string(),
				fragment,
				label: Some("failed to cast to bool".to_string()),
				help: None,
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
			CastError::InvalidUuid { fragment, target, cause } => Diagnostic {
				code: "CAST_005".to_string(),
				statement: None,
				message: format!("failed to cast to {}", target),
				fragment,
				label: Some(format!("failed to cast to {}", target)),
				help: None,
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
			CastError::InvalidTemporal { fragment, target, cause } => Diagnostic {
				code: "CAST_003".to_string(),
				statement: None,
				message: format!("failed to cast to {}", target),
				fragment,
				label: Some(format!("failed to cast to {}", target)),
				help: None,
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
			CastError::InvalidBlobToUtf8 { fragment, cause } => Diagnostic {
				code: "CAST_006".to_string(),
				statement: None,
				message: "failed to cast BLOB to UTF8".to_string(),
				fragment,
				label: Some("failed to cast BLOB to UTF8".to_string()),
				help: Some("BLOB contains invalid UTF-8 bytes. Consider using to_utf8_lossy() function instead"
					.to_string()),
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
		}
	}
}

impl From<CastError> for Error {
	fn from(err: CastError) -> Self {
		Error(err.into_diagnostic())
	}
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
	#[error("column `{column}` not found in `{table_name}`")]
	BulkInsertColumnNotFound {
		fragment: Fragment,
		table_name: String,
		column: String,
	},

	#[error("too many values: expected {expected} columns, got {actual}")]
	BulkInsertTooManyValues {
		fragment: Fragment,
		expected: usize,
		actual: usize,
	},

	#[error("Frame must have a __ROW__ID__ column for UPDATE operations")]
	MissingRowNumberColumn,

	#[error("assertion failed: {message}")]
	AssertionFailed {
		fragment: Fragment,
		message: String,
		expression: Option<String>,
	},

	#[error("Cannot insert none into non-optional column of type {column_type}")]
	NoneNotAllowed {
		fragment: Fragment,
		column_type: Type,
	},

	#[error("Unknown function: {name}")]
	UnknownFunction {
		name: String,
		fragment: Fragment,
	},

	#[error("Generator function '{name}' not found")]
	GeneratorNotFound {
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

impl IntoDiagnostic for EngineError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			EngineError::BulkInsertColumnNotFound {
				fragment,
				table_name,
				column,
			} => Diagnostic {
				code: "BI_001".to_string(),
				statement: None,
				message: format!("column `{}` not found in `{}`", column, table_name),
				column: None,
				fragment,
				label: Some("unknown column".to_string()),
				help: Some("check that the column name matches the schema".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EngineError::BulkInsertTooManyValues {
				fragment,
				expected,
				actual,
			} => Diagnostic {
				code: "BI_003".to_string(),
				statement: None,
				message: format!("too many values: expected {} columns, got {}", expected, actual),
				column: None,
				fragment,
				label: Some("value count mismatch".to_string()),
				help: Some("ensure the number of values matches the column count".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EngineError::MissingRowNumberColumn => Diagnostic {
				code: "ENG_003".to_string(),
				statement: None,
				message: "Frame must have a __ROW__ID__ column for UPDATE operations".to_string(),
				column: None,
				fragment: Fragment::None,
				label: Some("missing required column".to_string()),
				help: Some("Ensure the query includes the encoded ID in the result set".to_string()),
				notes: vec!["UPDATE operations require encoded identifiers to locate existing rows"
					.to_string()],
				cause: None,
				operator_chain: None,
			},
			EngineError::AssertionFailed {
				fragment,
				message,
				expression,
			} => {
				let base_msg = if !message.is_empty() {
					message.clone()
				} else if let Some(ref expr) = expression {
					format!("assertion failed: {}", expr)
				} else {
					"assertion failed".to_string()
				};
				let label = expression
					.as_ref()
					.map(|expr| format!("this expression is false: {}", expr))
					.or_else(|| Some("assertion failed".to_string()));
				Diagnostic {
					code: "ASSERT".to_string(),
					statement: None,
					message: base_msg,
					fragment,
					label,
					help: None,
					notes: vec![],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}
			EngineError::NoneNotAllowed {
				fragment,
				column_type,
			} => Diagnostic {
				code: "CONSTRAINT_007".to_string(),
				statement: None,
				message: format!(
					"Cannot insert none into non-optional column of type {}. Declare the column as Option({}) to allow none values.",
					column_type, column_type
				),
				column: None,
				fragment,
				label: Some("constraint violation".to_string()),
				help: Some(format!(
					"The column type is {} which does not accept none. Use Option({}) if the column should be nullable.",
					column_type, column_type
				)),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EngineError::UnknownFunction {
				name,
				fragment,
			} => Diagnostic {
				code: "FUNCTION_001".to_string(),
				statement: None,
				message: format!("Unknown function: {}", name),
				column: None,
				fragment,
				label: Some("unknown function".to_string()),
				help: Some("Check the function name and available functions".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EngineError::GeneratorNotFound {
				name,
				fragment,
			} => Diagnostic {
				code: "FUNCTION_009".to_string(),
				statement: None,
				message: format!("Generator function '{}' not found", name),
				column: None,
				fragment,
				label: Some("unknown generator function".to_string()),
				help: Some("Check the generator function name and ensure it is registered".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EngineError::VariableNotFound {
				name,
			} => Diagnostic {
				code: "RUNTIME_001".to_string(),
				statement: None,
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
			EngineError::VariableIsImmutable {
				name,
			} => Diagnostic {
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
			},
		}
	}
}

impl From<EngineError> for Error {
	fn from(err: EngineError) -> Self {
		Error(err.into_diagnostic())
	}
}
