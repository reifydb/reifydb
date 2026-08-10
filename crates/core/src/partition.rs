// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

use crate::interface::catalog::{column::Column, object::ObjectId};

pub fn partition_col_indices(columns: &[Column], partition_by: &[String]) -> Vec<usize> {
	partition_by
		.iter()
		.map(|pb| {
			columns.iter()
				.position(|c| c.name == *pb)
				.expect("partition column must exist (validated during planning)")
		})
		.collect()
}

#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
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
}

impl IntoDiagnostic for PartitionError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			PartitionError::ImmutablePartitionColumn {
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

			PartitionError::PartitionHashCollision {
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
		}
	}
}

impl From<PartitionError> for Error {
	fn from(err: PartitionError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}
