// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	columnar::{layout::ColumnsLayout, ColumnData},
	evaluate::{evaluate, EvaluationContext},
	execute::{Batch, ExecutionContext, ExecutionPlan},
};
use reifydb_core::interface::QueryTransaction;
use reifydb_core::{
	interface::{
		evaluate::expression::Expression,
	},
	BitVec,
};

pub(crate) struct FilterNode {
	input: Box<ExecutionPlan>,
	expressions: Vec<Expression>,
}

impl FilterNode {
	pub fn new(
		input: Box<ExecutionPlan>,
		expressions: Vec<Expression>,
	) -> Self {
		Self {
			input,
			expressions,
		}
	}
}

impl FilterNode {
	pub(crate) fn next(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut impl QueryTransaction,
	) -> crate::Result<Option<Batch>> {
		while let Some(Batch {
			mut columns,
		}) = self.input.next(ctx, rx)?
		{
			let mut row_count = columns.row_count();

			// Apply each filter expression sequentially
			for filter_expr in &self.expressions {
				// Early exit if no rows remain
				if row_count == 0 {
					break;
				}

				// Create evaluation context for all current
				// rows
				let eval_ctx = EvaluationContext {
					target_column: None,
					column_policies: Vec::new(),
					columns: columns.clone(),
					row_count,
					take: None,
					params: &ctx.params,
				};

				// Evaluate the filter expression
				let result = evaluate(&eval_ctx, filter_expr)?;

				// Create filter mask from result
				let filter_mask =
					match result.data() {
						ColumnData::Bool(container) => {
							let mut mask = BitVec::repeat(row_count, false);
							for i in 0..row_count {
								if i < container.data().len() && i < container.bitvec().len() {
                                let valid = container.is_defined(i);
                                let filter_result = container.data().get(i);
                                mask.set(i, valid & filter_result);
                            }
							}
							mask
						}
						_ => panic!(
							"filter expression must evaluate to a boolean column"
						),
					};

				columns.filter(&filter_mask)?;
				row_count = columns.row_count();
			}

			if row_count > 0 {
				return Ok(Some(Batch {
					columns,
				}));
			}
		}
		Ok(None)
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		self.input.layout()
	}
}
