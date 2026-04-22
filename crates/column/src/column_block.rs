// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_type::value::r#type::Type;

use crate::chunked::ChunkedArray;

pub type Schema = Arc<Vec<(String, Type, bool)>>;

// The column container used by a `Snapshot` — a schema plus one
// `ChunkedArray` per user column. The schema's tuple entries are
// `(name, ty, nullable)` in positional order.
#[derive(Clone)]
pub struct ColumnBlock {
	pub schema: Schema,
	pub columns: Vec<ChunkedArray>,
}

impl ColumnBlock {
	pub fn new(schema: Schema, columns: Vec<ChunkedArray>) -> Self {
		debug_assert_eq!(schema.len(), columns.len(), "ColumnBlock::new: schema and columns length mismatch");
		Self {
			schema,
			columns,
		}
	}

	pub fn len(&self) -> usize {
		self.columns.first().map(|c| c.len()).unwrap_or(0)
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn column_by_name(&self, name: &str) -> Option<(usize, &ChunkedArray)> {
		self.schema.iter().position(|(n, _, _)| n == name).map(|i| (i, &self.columns[i]))
	}
}
