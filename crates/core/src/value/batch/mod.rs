// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod lazy;

use lazy::LazyBatch;
use reifydb_type::{Result, util::bitvec::BitVec, value::Value};

use crate::value::column::columns::Columns;

#[derive(Debug, Clone)]
pub enum Batch {
	Lazy(LazyBatch),

	FullyMaterialized(Columns),
}

impl Batch {
	pub fn lazy(lazy: LazyBatch) -> Self {
		Batch::Lazy(lazy)
	}

	pub fn fully_materialized(columns: Columns) -> Self {
		Batch::FullyMaterialized(columns)
	}

	pub fn row_count(&self) -> usize {
		match self {
			Batch::Lazy(lazy) => lazy.valid_row_count(),
			Batch::FullyMaterialized(columns) => columns.row_count(),
		}
	}

	pub fn column_count(&self) -> usize {
		match self {
			Batch::Lazy(lazy) => lazy.column_count(),
			Batch::FullyMaterialized(columns) => columns.len(),
		}
	}

	pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Value {
		match self {
			Batch::Lazy(lazy) => lazy.get_value(row_idx, col_idx),
			Batch::FullyMaterialized(columns) => columns[col_idx].get_value(row_idx),
		}
	}

	pub fn into_columns(self) -> Columns {
		match self {
			Batch::Lazy(lazy) => lazy.into_columns(),
			Batch::FullyMaterialized(columns) => columns,
		}
	}

	pub fn as_lazy(&self) -> Option<&LazyBatch> {
		match self {
			Batch::Lazy(lazy) => Some(lazy),
			_ => None,
		}
	}

	pub fn as_lazy_mut(&mut self) -> Option<&mut LazyBatch> {
		match self {
			Batch::Lazy(lazy) => Some(lazy),
			_ => None,
		}
	}

	pub fn apply_filter(&mut self, filter: &BitVec) -> Result<()> {
		match self {
			Batch::Lazy(lazy) => {
				lazy.apply_filter(filter);
				Ok(())
			}
			Batch::FullyMaterialized(columns) => columns.filter(filter),
		}
	}

	pub fn extract_by_indices(&self, indices: &[usize]) -> Batch {
		match self {
			Batch::Lazy(lazy) => {
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
