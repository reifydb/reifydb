// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::object::ObjectId;
use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::value_type::ValueType,
};

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
		column_type: ValueType,
	},

	#[error("Unknown callable: {name}")]
	UnknownCallable {
		name: String,
		fragment: Fragment,
	},

	#[error("Generator function '{name}' not found")]
	GeneratorNotFound {
		name: String,
		fragment: Fragment,
	},

	#[error(
		"cannot locate partitioned rows for {operation} on object {object}: query object carries no partition address"
	)]
	MissingPartitionAddress {
		object: ObjectId,
		operation: &'static str,
	},

	#[error("cannot change partition column via UPDATE on object {object}: partition columns are immutable")]
	ImmutablePartitionColumn {
		object: ObjectId,
	},

	#[error(
		"partition hash collision on object {object}: hash {hash:032x} maps to two distinct partition value tuples"
	)]
	PartitionHashCollision {
		object: ObjectId,
		hash: u128,
	},

	#[error("`{object}` declares `{column}` as its #time populator but has no such column")]
	TimePopulatorMissing {
		object: String,
		column: String,
	},

	#[error("`{object}.{column}` is the declared #time populator but holds {found} on this row")]
	TimePopulatorNotDateTime {
		object: String,
		column: String,
		found: String,
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
				rql: None,
				message: format!("column `{}` not found in `{}`", column, table_name),
				column: None,
				fragment,
				label: Some("unknown column".to_string()),
				help: Some("check that the column name matches the shape".to_string()),
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
				rql: None,
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
				rql: None,
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
					rql: None,
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
				rql: None,
				message: format!(
					"Cannot insert none into non-optional column of type {}. Declare the column as Option({}) to allow none values.",
					column_type, column_type
				),
				column: None,
				fragment,
				label: Some("constraint violation".to_string()),
				help: Some(format!(
					"The column type is {} which does not accept none. Use Option({}) if the column should be optional.",
					column_type, column_type
				)),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EngineError::UnknownCallable {
				name,
				fragment,
			} => Diagnostic {
				code: "CALLABLE_001".to_string(),
				rql: None,
				message: format!("Unknown callable: {}", name),
				column: None,
				fragment,
				label: Some("unknown callable".to_string()),
				help: Some(
					"Check the name and available functions, procedures, and closures".to_string()
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EngineError::GeneratorNotFound {
				name,
				fragment,
			} => Diagnostic {
				code: "FUNCTION_009".to_string(),
				rql: None,
				message: format!("Generator function '{}' not found", name),
				column: None,
				fragment,
				label: Some("unknown generator function".to_string()),
				help: Some("Check the generator function name and ensure it is registered".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			EngineError::MissingPartitionAddress {
				object,
				operation,
			} => Diagnostic {
				code: "PART_001".to_string(),
				rql: None,
				message: format!(
					"cannot locate partitioned rows for {} on object {}: query object carries no partition address",
					operation, object
				),
				column: None,
				fragment: Fragment::None,
				label: Some("missing partition address".to_string()),
				help: Some(
					"the query must carry the row's partition alongside its row number; rewrite the query so the partitioned source's columns flow through unmodified"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			EngineError::ImmutablePartitionColumn {
				object,
			} => Diagnostic {
				code: "PART_002".to_string(),
				rql: None,
				message: format!(
					"cannot change partition column via UPDATE on object {}: partition columns are immutable",
					object
				),
				column: None,
				fragment: Fragment::None,
				label: Some("partition column change rejected".to_string()),
				help: Some(
					"partition columns determine a row's physical location and cannot be updated; delete and re-insert the row instead"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			EngineError::PartitionHashCollision {
				object,
				hash,
			} => Diagnostic {
				code: "PART_003".to_string(),
				rql: None,
				message: format!(
					"partition hash collision on object {}: hash {:032x} maps to two distinct partition value tuples",
					object, hash
				),
				column: None,
				fragment: Fragment::None,
				label: Some("128-bit hash collision".to_string()),
				help: Some(
					"two distinct partition value tuples produced the same 128-bit hash; this is astronomically unlikely and points to a hashing bug or data corruption, report it as a bug"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			EngineError::TimePopulatorMissing {
				object,
				column,
			} => Diagnostic {
				code: "TIME_001".to_string(),
				rql: None,
				message: format!(
					"`{}` declares `{}` as its #time populator but has no such column",
					object, column
				),
				column: None,
				fragment: Fragment::None,
				label: Some("declared populator does not exist".to_string()),
				help: Some(
					"the populator must name one of the object's own columns; correct the `ts` key in the object's WITH clause"
						.to_string(),
				),
				notes: vec![
					"definition-time validation rejects a populator naming an absent column, so reaching this means the object was stored with a populator it does not have"
						.to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			EngineError::TimePopulatorNotDateTime {
				object,
				column,
				found,
			} => Diagnostic {
				code: "TIME_002".to_string(),
				rql: None,
				message: format!(
					"`{}.{}` is the declared #time populator but holds {} on this row",
					object, column, found
				),
				column: None,
				fragment: Fragment::None,
				label: Some("populator is not a non-none DateTime".to_string()),
				help: Some(
					"every row of an event-time object must carry a non-none DateTime in its populator column"
						.to_string(),
				),
				notes: vec![
					"#time is substrate-owned and cannot fall back to the write clock; silently substituting arrival time would date a replayed row to now"
						.to_string(),
				],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<EngineError> for Error {
	fn from(err: EngineError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}
