// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::id::{NamespaceId, SeriesId};
use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

#[derive(Debug, thiserror::Error)]
pub enum SubColumnError {
	#[error("column_block_from_batches: scan output missing column '{column}'")]
	MissingColumnInBatch {
		column: String,
	},

	#[error("column_block_from_batches: no batches to materialize column '{column}'")]
	NoBatchesForMaterialization {
		column: String,
	},

	#[error("series materialization: namespace {namespace:?} missing for series {series:?}")]
	NamespaceMissing {
		namespace: NamespaceId,
		series: SeriesId,
	},
}

impl From<SubColumnError> for Error {
	fn from(err: SubColumnError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

impl IntoDiagnostic for SubColumnError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			SubColumnError::MissingColumnInBatch {
				column,
			} => Diagnostic {
				code: "SCOL_001".to_string(),
				rql: None,
				message: format!("column_block_from_batches: scan output missing column '{column}'"),
				column: None,
				fragment: Fragment::None,
				label: Some("column missing in scan batch".to_string()),
				help: Some("the scan output schema must include every column named in the target schema".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			SubColumnError::NoBatchesForMaterialization {
				column,
			} => Diagnostic {
				code: "SCOL_002".to_string(),
				rql: None,
				message: format!("column_block_from_batches: no batches to materialize column '{column}'"),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			SubColumnError::NamespaceMissing {
				namespace,
				series,
			} => Diagnostic {
				code: "SCOL_003".to_string(),
				rql: None,
				message: format!("series materialization: namespace {namespace:?} missing for series {series:?}"),
				column: None,
				fragment: Fragment::None,
				label: Some("namespace not found in catalog".to_string()),
				help: Some("the series references a namespace that is no longer present; catalog may be out of sync".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}
