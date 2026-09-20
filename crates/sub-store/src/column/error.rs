// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::{NamespaceId, SeriesId};
use reifydb_value::value::sumtype::SumTypeId;
use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

#[derive(Debug, thiserror::Error)]
pub enum SubStoreError {
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

	#[error("series materialization: sum type {sumtype:?} missing for series {series:?}")]
	SumTypeMissing {
		sumtype: SumTypeId,
		series: SeriesId,
	},

	#[error("{}", time_mismatch_message(*.timed, *.time, *.rows))]
	TimeMismatch {
		timed: bool,
		time: usize,
		rows: usize,
	},
}

impl From<SubStoreError> for Error {
	fn from(err: SubStoreError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

impl IntoDiagnostic for SubStoreError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			SubStoreError::MissingColumnInBatch {
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

			SubStoreError::NoBatchesForMaterialization {
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

			SubStoreError::NamespaceMissing {
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

			SubStoreError::SumTypeMissing {
				sumtype,
				series,
			} => Diagnostic {
				code: "SCOL_005".to_string(),
				rql: None,
				message: format!("series materialization: sum type {sumtype:?} missing for series {series:?}"),
				column: None,
				fragment: Fragment::None,
				label: Some("sum type not found in catalog".to_string()),
				help: Some("the series declares a tag whose sum type is no longer present; catalog may be out of sync".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			SubStoreError::TimeMismatch {
				timed,
				time,
				rows,
			} => Diagnostic {
				code: "SCOL_004".to_string(),
				rql: None,
				message: time_mismatch_message(timed, time, rows),
				column: None,
				fragment: Fragment::None,
				label: Some("#time does not match the time declaration".to_string()),
				help: Some("an object that declares a time source stamps #time on every row, and a timeless object stamps it on none".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

fn time_mismatch_message(timed: bool, time: usize, rows: usize) -> String {
	let declaration = if timed {
		"declares a time source"
	} else {
		"declares no time source"
	};
	format!("column_block_from_batches: #time holds {time} entries for {rows} rows but the object {declaration}")
}
