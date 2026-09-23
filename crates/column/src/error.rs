// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::value_type::ValueType,
};

#[derive(Debug, thiserror::Error)]
pub enum ColumnError {
	#[error("{operation}: column '{name}' not in schema")]
	ColumnNotInSchema {
		operation: &'static str,
		name: String,
	},

	#[error("{operation}: only FixedArray storage supported in v1")]
	FixedArrayRequired {
		operation: &'static str,
	},

	#[error("Canonical::from_column_buffer: {variant} not yet supported")]
	FromColumnDataUnsupported {
		variant: &'static str,
	},

	#[error("Canonical::to_column_buffer: unexpected VarLen type {ty}")]
	ToColumnDataUnexpectedVarLen {
		ty: ValueType,
	},

	#[error("Canonical::to_column_buffer: unexpected BigNum type {ty}")]
	ToColumnDataUnexpectedBigNum {
		ty: ValueType,
	},

	#[error("Canonical::to_column_buffer: invalid UTF-8: {reason}")]
	ToColumnDataInvalidUtf8 {
		reason: String,
	},

	#[error("compare: column storage `{storage}` requires rhs `{expected}`")]
	CompareRhsTypeMismatch {
		storage: &'static str,
		expected: &'static str,
	},

	#[error("compare: BigNum comparison not yet implemented")]
	CompareBigNumUnsupported,

	#[error("predicate::evaluate: compare did not return a bool array")]
	PredicateCompareNotBool,

	#[error("min_max: empty array has no min/max")]
	MinMaxEmpty,

	#[error("min_max: all rows are None")]
	MinMaxAllNone,

	#[error("persist: failed to serialize column block: {reason}")]
	PersistSerialize {
		reason: String,
	},

	#[error("persist: failed to deserialize column block: {reason}")]
	PersistDeserialize {
		reason: String,
	},

	#[error("persist: unsupported column block format version {version}")]
	PersistVersionUnsupported {
		version: u16,
	},
}

impl From<ColumnError> for Error {
	fn from(err: ColumnError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

impl IntoDiagnostic for ColumnError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			ColumnError::ColumnNotInSchema {
				operation,
				name,
			} => Diagnostic {
				code: "COL_003".to_string(),
				rql: None,
				message: format!("{operation}: column '{name}' not in schema"),
				column: None,
				fragment: Fragment::None,
				label: Some("column not found".to_string()),
				help: Some("Verify the column name matches the block's schema".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::FixedArrayRequired {
				operation,
			} => Diagnostic {
				code: "COL_004".to_string(),
				rql: None,
				message: format!("{operation}: only FixedArray storage supported in v1"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::FromColumnDataUnsupported {
				variant,
			} => Diagnostic {
				code: "COL_007".to_string(),
				rql: None,
				message: format!("Canonical::from_column_buffer: {variant} not yet supported"),
				column: None,
				fragment: Fragment::None,
				label: Some(format!("{variant} column")),
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::ToColumnDataUnexpectedVarLen {
				ty,
			} => Diagnostic {
				code: "COL_008".to_string(),
				rql: None,
				message: format!("Canonical::to_column_buffer: unexpected VarLen type {ty}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::ToColumnDataUnexpectedBigNum {
				ty,
			} => Diagnostic {
				code: "COL_009".to_string(),
				rql: None,
				message: format!("Canonical::to_column_buffer: unexpected BigNum type {ty}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::ToColumnDataInvalidUtf8 {
				reason,
			} => Diagnostic {
				code: "COL_010".to_string(),
				rql: None,
				message: format!("Canonical::to_column_buffer: invalid UTF-8: {reason}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::CompareRhsTypeMismatch {
				storage,
				expected,
			} => Diagnostic {
				code: "COL_011".to_string(),
				rql: None,
				message: format!("compare: column storage `{storage}` requires rhs `{expected}`"),
				column: None,
				fragment: Fragment::None,
				label: Some("rhs type mismatch".to_string()),
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::CompareBigNumUnsupported => Diagnostic {
				code: "COL_012".to_string(),
				rql: None,
				message: "compare: BigNum comparison not yet implemented".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::PredicateCompareNotBool => Diagnostic {
				code: "COL_013".to_string(),
				rql: None,
				message: "predicate::evaluate: compare did not return a bool array".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::MinMaxEmpty => Diagnostic {
				code: "COL_015".to_string(),
				rql: None,
				message: "min_max: empty array has no min/max".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::MinMaxAllNone => Diagnostic {
				code: "COL_016".to_string(),
				rql: None,
				message: "min_max: all rows are None".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::PersistSerialize {
				reason,
			} => Diagnostic {
				code: "COL_018".to_string(),
				rql: None,
				message: format!("persist: failed to serialize column block: {reason}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::PersistDeserialize {
				reason,
			} => Diagnostic {
				code: "COL_019".to_string(),
				rql: None,
				message: format!("persist: failed to deserialize column block: {reason}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::PersistVersionUnsupported {
				version,
			} => Diagnostic {
				code: "COL_020".to_string(),
				rql: None,
				message: format!("persist: unsupported column block format version {version}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("the column block was written by an incompatible version of ReifyDB"
					.to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}
