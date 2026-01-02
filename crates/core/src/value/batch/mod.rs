// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod lazy;

pub use lazy::{LazyBatch, LazyColumnMeta};
use reifydb_type::Value;

use crate::{BitVec, value::column::Columns};

/// A batch of rows that can be lazy (encoded) or materialized (decoded).
///
/// This enables operators to work with data in the most efficient form:
/// - Lazy batches keep data encoded, enabling filters without materialization
/// - Materialized batches have decoded columnar data ready for computation
#[derive(Debug, Clone)]
pub enum Batch {
	/// Fully lazy batch - all data remains encoded
	Lazy(LazyBatch),

	/// Fully materialized batch - all columns decoded
	FullyMaterialized(Columns),
}

impl Batch {
	/// Create a lazy batch
	pub fn lazy(lazy: LazyBatch) -> Self {
		Batch::Lazy(lazy)
	}

	/// Create a materialized batch
	pub fn fully_materialized(columns: Columns) -> Self {
		Batch::FullyMaterialized(columns)
	}

	/// Get the number of rows in this batch
	pub fn row_count(&self) -> usize {
		match self {
			Batch::Lazy(lazy) => lazy.valid_row_count(),
			Batch::FullyMaterialized(columns) => columns.row_count(),
		}
	}

	/// Get the number of columns
	pub fn column_count(&self) -> usize {
		match self {
			Batch::Lazy(lazy) => lazy.column_count(),
			Batch::FullyMaterialized(columns) => columns.len(),
		}
	}

	/// Get a value from the batch
	pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Value {
		match self {
			Batch::Lazy(lazy) => lazy.get_value(row_idx, col_idx),
			Batch::FullyMaterialized(columns) => columns[col_idx].data().get_value(row_idx),
		}
	}

	/// Convert to fully materialized Columns
	pub fn into_columns(self) -> Columns {
		match self {
			Batch::Lazy(lazy) => lazy.into_columns(),
			Batch::FullyMaterialized(columns) => columns,
		}
	}

	/// Try to get as lazy batch reference
	pub fn as_lazy(&self) -> Option<&LazyBatch> {
		match self {
			Batch::Lazy(lazy) => Some(lazy),
			_ => None,
		}
	}

	/// Try to get as mutable lazy batch reference
	pub fn as_lazy_mut(&mut self) -> Option<&mut LazyBatch> {
		match self {
			Batch::Lazy(lazy) => Some(lazy),
			_ => None,
		}
	}

	/// Apply a filter mask to the batch
	///
	/// For lazy batches: updates validity bitmap without materialization
	/// For materialized batches: filters columns
	pub fn apply_filter(&mut self, filter: &BitVec) -> crate::Result<()> {
		match self {
			Batch::Lazy(lazy) => {
				lazy.apply_filter(filter);
				Ok(())
			}
			Batch::FullyMaterialized(columns) => columns.filter(filter),
		}
	}

	/// Extract rows by indices, creating a new batch
	pub fn extract_by_indices(&self, indices: &[usize]) -> Batch {
		match self {
			Batch::Lazy(lazy) => {
				// For lazy batches, we need to create a new lazy batch with only selected rows
				// For now, materialize then extract
				// TODO: Implement true lazy extraction
				let columns = lazy.clone().into_columns();
				Batch::FullyMaterialized(columns.extract_by_indices(indices))
			}
			Batch::FullyMaterialized(columns) => {
				Batch::FullyMaterialized(columns.extract_by_indices(indices))
			}
		}
	}
}
