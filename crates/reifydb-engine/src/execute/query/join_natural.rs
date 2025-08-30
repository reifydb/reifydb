// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashSet;

use reifydb_core::{
	JoinType, Value,
	interface::{QueryTransaction, Transaction},
};

use crate::{
	StandardCommandTransaction,
	columnar::{
		Column, ColumnQualified, Columns, SourceQualified,
		layout::ColumnsLayout,
	},
	execute::{Batch, ExecutionContext, ExecutionPlan},
};

pub(crate) struct NaturalJoinNode {
	left: Box<ExecutionPlan>,
	right: Box<ExecutionPlan>,
	join_type: JoinType,
	layout: Option<ColumnsLayout>,
}

impl NaturalJoinNode {
	pub fn new(
		left: Box<ExecutionPlan>,
		right: Box<ExecutionPlan>,
		join_type: JoinType,
	) -> Self {
		Self {
			left,
			right,
			join_type,
			layout: None,
		}
	}

	fn load_and_merge_all<T: Transaction>(
		node: &mut Box<ExecutionPlan>,
		ctx: &ExecutionContext,
		rx: &mut StandardCommandTransaction<T>,
	) -> crate::Result<Columns> {
		let mut result: Option<Columns> = None;

		while let Some(Batch {
			columns,
		}) = node.next(ctx, rx)?
		{
			if let Some(mut acc) = result.take() {
				acc.append_columns(columns)?;
				result = Some(acc);
			} else {
				result = Some(columns);
			}
		}
		let result = result.unwrap_or_else(Columns::empty);
		Ok(result)
	}

	fn find_common_columns(
		left_columns: &Columns,
		right_columns: &Columns,
	) -> Vec<(String, usize, usize)> {
		let mut common_columns = Vec::new();

		for (left_idx, left_col) in left_columns.iter().enumerate() {
			for (right_idx, right_col) in
				right_columns.iter().enumerate()
			{
				if left_col.name() == right_col.name() {
					common_columns.push((
						left_col.name().to_string(),
						left_idx,
						right_idx,
					));
				}
			}
		}

		common_columns
	}
}

impl NaturalJoinNode {
	pub(crate) fn next<T: Transaction>(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut StandardCommandTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		if self.layout.is_some() {
			return Ok(None);
		}

		let left_columns =
			Self::load_and_merge_all(&mut self.left, ctx, rx)?;
		let right_columns =
			Self::load_and_merge_all(&mut self.right, ctx, rx)?;

		let left_rows = left_columns.row_count();
		let right_rows = right_columns.row_count();

		// Find common columns between left and right columnss
		let common_columns = Self::find_common_columns(
			&left_columns,
			&right_columns,
		);

		if common_columns.is_empty() {
			// If no common columns, natural join behaves like a
			// cross join For now, return an error as this is
			// unusual
			panic!(
				"Natural join requires at least one common column"
			);
		}

		// Build set of right column indices to exclude (common columns)
		let excluded_right_cols: HashSet<usize> = common_columns
			.iter()
			.map(|(_, _, right_idx)| *right_idx)
			.collect();

		// Build qualified column names for the result (excluding
		// duplicates from right)
		let qualified_names: Vec<String> = left_columns
			.iter()
			.map(|col| col.qualified_name())
			.chain(right_columns
				.iter()
				.enumerate()
				.filter(|(idx, _)| {
					!excluded_right_cols.contains(idx)
				})
				.map(|(_, col)| col.qualified_name()))
			.collect();

		let mut result_rows = Vec::new();

		for i in 0..left_rows {
			let left_row = left_columns.get_row(i);
			let mut matched = false;

			for j in 0..right_rows {
				let right_row = right_columns.get_row(j);

				// Check if all common columns match
				let all_match = common_columns.iter().all(
					|(_, left_idx, right_idx)| {
						left_row[*left_idx]
							== right_row[*right_idx]
					},
				);

				if all_match {
					// Combine rows, excluding duplicate
					// columns from right
					let mut combined = left_row.clone();
					for (idx, value) in
						right_row.iter().enumerate()
					{
						if !excluded_right_cols
							.contains(&idx)
						{
							combined.push(
								value.clone()
							);
						}
					}
					result_rows.push(combined);
					matched = true;
				}
			}

			// Handle LEFT natural join - include unmatched left
			// rows
			if !matched && matches!(self.join_type, JoinType::Left)
			{
				let mut combined = left_row.clone();
				// Add undefined data for non-common right
				// columns
				let undefined_count = right_columns.len()
					- excluded_right_cols.len();
				combined.extend(vec![
					Value::Undefined;
					undefined_count
				]);
				result_rows.push(combined);
			}
		}

		// Create columns with proper qualified column structure
		let mut column_metadata: Vec<&Column> =
			left_columns.iter().collect();
		for (idx, col) in right_columns.iter().enumerate() {
			if !excluded_right_cols.contains(&idx) {
				column_metadata.push(col);
			}
		}

		let names_refs: Vec<&str> =
			qualified_names.iter().map(|s| s.as_str()).collect();
		let mut columns = Columns::from_rows(&names_refs, &result_rows);

		// Update columns columns with proper metadata
		for (i, col_meta) in column_metadata.iter().enumerate() {
			let old_column = &columns[i];
			columns[i] = match col_meta.table() {
				Some(source) => Column::SourceQualified(
					SourceQualified {
						source: source.to_string(),
						name: col_meta
							.name()
							.to_string(),
						data: old_column.data().clone(),
					},
				),
				None => Column::ColumnQualified(
					ColumnQualified {
						name: col_meta
							.name()
							.to_string(),
						data: old_column.data().clone(),
					},
				),
			};
		}

		self.layout = Some(ColumnsLayout::from_columns(&columns));
		Ok(Some(Batch {
			columns,
		}))
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone()
	}
}
