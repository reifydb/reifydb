// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use reifydb_core::{
	common::JoinType,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, row_number::RowNumber},
};
use tracing::instrument;

use super::common::{JoinContext, load_and_merge_all, resolve_column_names};
use crate::vm::volcano::query::{QueryContext, QueryNode};

pub struct NaturalJoinNode {
	left: Box<dyn QueryNode>,
	right: Box<dyn QueryNode>,
	join_type: JoinType,
	alias: Option<Fragment>,
	headers: Option<ColumnHeaders>,
	context: JoinContext,
}

impl NaturalJoinNode {
	pub(crate) fn new(
		left: Box<dyn QueryNode>,
		right: Box<dyn QueryNode>,
		join_type: JoinType,
		alias: Option<Fragment>,
	) -> Self {
		Self {
			left,
			right,
			join_type,
			alias,
			headers: None,
			context: JoinContext::new(),
		}
	}

	fn find_common_columns(left_columns: &Columns, right_columns: &Columns) -> Vec<(String, usize, usize)> {
		let mut common_columns = Vec::new();

		for (left_idx, left_col) in left_columns.iter().enumerate() {
			for (right_idx, right_col) in right_columns.iter().enumerate() {
				if left_col.name() == right_col.name() {
					common_columns.push((left_col.name().text().to_string(), left_idx, right_idx));
				}
			}
		}

		common_columns
	}
}

impl QueryNode for NaturalJoinNode {
	#[instrument(name = "volcano::join::natural::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.context.set(ctx);
		self.left.initialize(rx, ctx)?;
		self.right.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(name = "volcano::join::natural::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_initialized(), "NaturalJoinNode::next() called before initialize()");

		if self.headers.is_some() {
			return Ok(None);
		}

		let left_columns = load_and_merge_all(&mut self.left, rx, ctx)?;
		let right_columns = load_and_merge_all(&mut self.right, rx, ctx)?;

		let left_rows = left_columns.row_count();
		let right_rows = right_columns.row_count();
		let left_row_numbers = left_columns.row_numbers.to_vec();

		// Find common columns between left and right columns
		let common_columns = Self::find_common_columns(&left_columns, &right_columns);

		if common_columns.is_empty() {
			return Ok(None);
		}

		// Build set of right column indices to exclude (common columns)
		let excluded_right_cols: HashSet<usize> =
			common_columns.iter().map(|(_, _, right_idx)| *right_idx).collect();

		// Convert to Vec for resolve_column_names
		let excluded_indices: Vec<usize> = excluded_right_cols.iter().copied().collect();

		// Resolve column names, excluding common columns from right
		let resolved =
			resolve_column_names(&left_columns, &right_columns, &self.alias, Some(&excluded_indices));

		let mut result_rows = Vec::new();
		let mut result_row_numbers: Vec<RowNumber> = Vec::new();

		for i in 0..left_rows {
			let left_row = left_columns.get_row(i);
			let mut matched = false;

			for j in 0..right_rows {
				let right_row = right_columns.get_row(j);

				// Check if all common columns match
				let all_match = common_columns
					.iter()
					.all(|(_, left_idx, right_idx)| left_row[*left_idx] == right_row[*right_idx]);

				if all_match {
					// Combine rows, excluding duplicate columns from right
					let mut combined = left_row.clone();
					for (idx, value) in right_row.iter().enumerate() {
						if !excluded_right_cols.contains(&idx) {
							combined.push(value.clone());
						}
					}
					result_rows.push(combined);
					matched = true;
					if !left_row_numbers.is_empty() {
						result_row_numbers.push(left_row_numbers[i]);
					}
				}
			}

			// Handle LEFT natural join - include unmatched left rows
			if !matched && matches!(self.join_type, JoinType::Left) {
				let mut combined = left_row.clone();
				// Add undefined data for non-common right columns
				let undefined_count = right_columns.len() - excluded_right_cols.len();
				combined.extend(vec![Value::Undefined; undefined_count]);
				result_rows.push(combined);
				if !left_row_numbers.is_empty() {
					result_row_numbers.push(left_row_numbers[i]);
				}
			}
		}

		// Create columns with conflict-resolved names
		let names_refs: Vec<&str> = resolved.qualified_names.iter().map(|s| s.as_str()).collect();
		let columns = if result_row_numbers.is_empty() {
			Columns::from_rows(&names_refs, &result_rows)
		} else {
			Columns::from_rows_with_row_numbers(&names_refs, &result_rows, result_row_numbers)
		};

		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
