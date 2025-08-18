// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Value,
	interface::{
		VersionedQueryTransaction, evaluate::expression::Expression,
	},
};

use crate::{
	columnar::{
		Column, ColumnData, ColumnQualified, Columns, SourceQualified,
		layout::ColumnsLayout,
	},
	evaluate::{EvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan},
};

pub(crate) struct InnerJoinNode {
	left: Box<dyn ExecutionPlan>,
	right: Box<dyn ExecutionPlan>,
	on: Vec<Expression>,
	layout: Option<ColumnsLayout>,
}

impl InnerJoinNode {
	pub fn new(
		left: Box<dyn ExecutionPlan>,
		right: Box<dyn ExecutionPlan>,
		on: Vec<Expression>,
	) -> Self {
		Self {
			left,
			right,
			on,
			layout: None,
		}
	}

	fn load_and_merge_all(
		node: &mut Box<dyn ExecutionPlan>,
		ctx: &ExecutionContext,
		rx: &mut dyn VersionedQueryTransaction,
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
}

impl ExecutionPlan for InnerJoinNode {
	fn next(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut dyn VersionedQueryTransaction,
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

		// Build qualified column names for the join result
		let qualified_names: Vec<String> = left_columns
			.iter()
			.chain(right_columns.iter())
			.map(|col| col.qualified_name())
			.collect();

		let mut result_rows = Vec::new();

		for i in 0..left_rows {
			let left_row = left_columns.get_row(i);

			for j in 0..right_rows {
				let right_row = right_columns.get_row(j);

				let all_data = left_row
					.iter()
					.cloned()
					.chain(right_row.iter().cloned())
					.collect::<Vec<_>>();

				let eval_ctx = EvaluationContext {
                    target_column: None,
                    column_policies: Vec::new(),
                    columns: Columns::new(
                        all_data
                            .iter()
                            .cloned()
                            .zip(left_columns.iter().chain(right_columns.iter()))
                            .map(|(v, col)| match col.table() {
                                Some(source) => Column::SourceQualified(SourceQualified {
                                    source: source.to_string(),
                                    name: col.name().to_string(),
                                    data: ColumnData::from(v),
                                }),
                                None => Column::ColumnQualified(ColumnQualified {
                                    name: col.name().to_string(),
                                    data: ColumnData::from(v),
                                }),
                            })
                            .collect(),
                    ),
                    row_count: 1,
                    take: Some(1),
                    params: &ctx.params,
                };

				let all_true = self.on.iter().fold(
					true,
					|acc, cond| {
						let col = evaluate(
							&eval_ctx, cond,
						)
						.unwrap();
						matches!(
							col.data().get_value(0),
							Value::Bool(true)
						) && acc
					},
				);

				if all_true {
					let mut combined = left_row.clone();
					combined.extend(right_row.clone());
					result_rows.push(combined);
				}
			}
		}

		// Create columns with proper qualified column structure
		let column_metadata: Vec<_> = left_columns
			.iter()
			.chain(right_columns.iter())
			.collect();
		let names_refs: Vec<&str> =
			qualified_names.iter().map(|s| s.as_str()).collect();
		let mut columns = Columns::from_rows(&names_refs, &result_rows);

		// Update columns with proper metadata
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

	fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone()
	}
}
