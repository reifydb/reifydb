// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::io::fs::FsError;
use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};
use vortex_error::VortexError;

#[derive(Debug, thiserror::Error)]
pub enum ColumnError {
	#[error("{operation}: column '{name}' not in schema")]
	ColumnNotInSchema {
		operation: &'static str,
		name: String,
	},

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

	#[error("{operation}: vortex failed: {reason}")]
	Vortex {
		operation: &'static str,
		reason: String,
	},

	#[error("predicate: {value} cannot be compared with column '{column}' of type {ty}")]
	PredicateValue {
		column: String,
		ty: String,
		value: String,
	},

	#[error("persist: column '{column}' is stored as {stored} but its field type reads as {expected}")]
	DTypeMismatch {
		column: String,
		stored: String,
		expected: String,
	},

	#[error("{operation}: {source}")]
	Fs {
		operation: &'static str,
		source: FsError,
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

			ColumnError::Vortex {
				operation,
				reason,
			} => Diagnostic {
				code: "COL_021".to_string(),
				rql: None,
				message: format!("{operation}: vortex failed: {reason}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::PredicateValue {
				column,
				ty,
				value,
			} => Diagnostic {
				code: "COL_022".to_string(),
				rql: None,
				message: format!(
					"predicate: {value} cannot be compared with column '{column}' of type {ty}"
				),
				column: None,
				fragment: Fragment::None,
				label: Some("value type does not match the column".to_string()),
				help: Some("compare the column with a value of its own type".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::DTypeMismatch {
				column,
				stored,
				expected,
			} => Diagnostic {
				code: "COL_023".to_string(),
				rql: None,
				message: format!(
					"persist: column '{column}' is stored as {stored} but its field type reads as {expected}"
				),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("the column block was written with a different type mapping".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			ColumnError::Fs {
				operation,
				source,
			} => Diagnostic {
				code: "COL_024".to_string(),
				rql: None,
				message: format!("{operation}: {source}"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

pub(crate) fn vortex(operation: &'static str) -> impl FnOnce(VortexError) -> Error {
	move |err| {
		ColumnError::Vortex {
			operation,
			reason: err.to_string(),
		}
		.into()
	}
}

pub(crate) fn fs(operation: &'static str) -> impl FnOnce(FsError) -> Error {
	move |source| {
		ColumnError::Fs {
			operation,
			source,
		}
		.into()
	}
}
