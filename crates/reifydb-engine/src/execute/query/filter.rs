// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	BitVec,
	interface::{Transaction, evaluate::expression::Expression},
};

use crate::{
	StandardTransaction,
	columnar::{ColumnData, layout::ColumnsLayout},
	evaluate::{EvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct FilterNode<'a, T: Transaction> {
	input: Box<ExecutionPlan<'a, T>>,
	expressions: Vec<Expression<'a>>,
	context: Option<Arc<ExecutionContext>>,
}

impl<'a, T: Transaction> FilterNode<'a, T> {
	pub fn new(
		input: Box<ExecutionPlan<'a, T>>,
		expressions: Vec<Expression<'a>>,
	) -> Self {
		Self {
			input,
			expressions,
			context: None,
		}
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for FilterNode<'a, T> {
	fn initialize(
		&mut self,
		rx: &mut StandardTransaction<'a, T>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut StandardTransaction<'a, T>,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(
			self.context.is_some(),
			"FilterNode::next() called before initialize()"
		);
		let ctx = self.context.as_ref().unwrap();

		while let Some(Batch {
			mut columns,
		}) = self.input.next(rx)?
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

	fn layout(&self) -> Option<ColumnsLayout> {
		self.input.layout()
	}
}
