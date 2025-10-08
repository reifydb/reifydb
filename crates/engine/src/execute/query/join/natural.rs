// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashSet;

use reifydb_core::{
	JoinType,
	value::column::{Columns, headers::ColumnHeaders},
};
use reifydb_type::{Fragment, Value};

use super::common::{JoinContext, load_and_merge_all, resolve_column_names};
use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub struct NaturalJoinNode<'a> {
	left: Box<ExecutionPlan<'a>>,
	right: Box<ExecutionPlan<'a>>,
	join_type: JoinType,
	alias: Option<Fragment<'a>>,
	headers: Option<ColumnHeaders<'a>>,
	context: JoinContext<'a>,
}

impl<'a> NaturalJoinNode<'a> {
	pub fn new(
		left: Box<ExecutionPlan<'a>>,
		right: Box<ExecutionPlan<'a>>,
		join_type: JoinType,
		alias: Option<Fragment<'a>>,
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

impl<'a> QueryNode<'a> for NaturalJoinNode<'a> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context.set(ctx);
		self.left.initialize(rx, ctx)?;
		self.right.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_initialized(), "NaturalJoinNode::next() called before initialize()");

		if self.headers.is_some() {
			return Ok(None);
		}

		let left_columns = load_and_merge_all(&mut self.left, rx, ctx)?;
		let right_columns = load_and_merge_all(&mut self.right, rx, ctx)?;

		let left_rows = left_columns.row_count();
		let right_rows = right_columns.row_count();

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
				}
			}

			// Handle LEFT natural join - include unmatched left rows
			if !matched && matches!(self.join_type, JoinType::Left) {
				let mut combined = left_row.clone();
				// Add undefined data for non-common right columns
				let undefined_count = right_columns.len() - excluded_right_cols.len();
				combined.extend(vec![Value::Undefined; undefined_count]);
				result_rows.push(combined);
			}
		}

		// Create columns with conflict-resolved names
		let names_refs: Vec<&str> = resolved.qualified_names.iter().map(|s| s.as_str()).collect();
		let columns = Columns::from_rows(&names_refs, &result_rows);

		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		self.headers.clone()
	}
}
