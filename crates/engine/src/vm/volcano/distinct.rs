// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use reifydb_core::{
	interface::resolved::ResolvedColumn,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_runtime::hash::{Hash128, xxh3_128};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::vm::volcano::query::{QueryContext, QueryNode};

pub(crate) struct DistinctNode {
	input: Box<dyn QueryNode>,
	columns: Vec<ResolvedColumn>,
	headers: Option<ColumnHeaders>,
}

impl DistinctNode {
	pub fn new(input: Box<dyn QueryNode>, columns: Vec<ResolvedColumn>) -> Self {
		Self {
			input,
			columns,
			headers: None,
		}
	}
}

impl QueryNode for DistinctNode {
	#[instrument(level = "trace", skip_all, name = "volcano::distinct::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		// Only emit once (like AggregateNode)
		if self.headers.is_some() {
			return Ok(None);
		}

		// 1. Collect all input rows into a single batch
		let mut all_columns: Option<Columns> = None;
		while let Some(cols) = self.input.next(rx, ctx)? {
			if cols.row_count() == 0 {
				continue;
			}
			match &mut all_columns {
				None => all_columns = Some(cols),
				Some(existing) => existing.append_columns(cols)?,
			}
		}

		let all_columns = match all_columns {
			Some(cols) => cols,
			None => {
				self.headers = Some(ColumnHeaders::empty());
				return Ok(None);
			}
		};

		// 2. Determine which column names to use for hashing
		let distinct_col_names: Vec<&str> = self.columns.iter().map(|c| c.name()).collect();

		// 3. For each row, hash the distinct column values and track first occurrences
		let row_count = all_columns.row_count();
		let mut seen = HashSet::<Hash128>::new();
		let mut kept_indices = Vec::new();

		for row_idx in 0..row_count {
			let mut data = Vec::new();
			for col_name in &distinct_col_names {
				if let Some(col) = all_columns.column(col_name) {
					let value = col.data().get_value(row_idx);
					let value_str = value.to_string();
					data.extend_from_slice(value_str.as_bytes());
				}
			}
			let hash = xxh3_128(&data);
			if seen.insert(hash) {
				kept_indices.push(row_idx);
			}
		}

		// 4. Extract kept rows
		let result = all_columns.extract_by_indices(&kept_indices);
		self.headers = Some(ColumnHeaders::from_columns(&result));

		Ok(Some(result))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}
