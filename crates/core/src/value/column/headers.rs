// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_value::fragment::Fragment;

#[derive(Debug, Clone)]
pub struct ColumnHeaders {
	pub columns: Vec<Fragment>,
}

impl ColumnHeaders {
	pub fn from_batch(batch: &RecordBatch) -> Self {
		Self {
			columns: batch
				.schema_ref()
				.fields()
				.iter()
				.map(|field| Fragment::internal(field.name()))
				.collect(),
		}
	}

	pub fn empty() -> Self {
		Self {
			columns: Vec::new(),
		}
	}
}
