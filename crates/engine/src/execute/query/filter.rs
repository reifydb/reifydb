// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	BitVec,
	value::column::{ColumnData, headers::ColumnHeaders},
};
use reifydb_rql::expression::Expression;

use crate::{
	StandardTransaction,
	evaluate::column::{ColumnEvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct FilterNode<'a> {
	input: Box<ExecutionPlan<'a>>,
	expressions: Vec<Expression<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
}

impl<'a> FilterNode<'a> {
	pub fn new(input: Box<ExecutionPlan<'a>>, expressions: Vec<Expression<'a>>) -> Self {
		Self {
			input,
			expressions,
			context: None,
		}
	}
}

impl<'a> QueryNode<'a> for FilterNode<'a> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "FilterNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		while let Some(Batch {
			mut columns,
		}) = self.input.next(rx, ctx)?
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
				let eval_ctx = ColumnEvaluationContext {
					target: None,
					columns: columns.clone(),
					row_count,
					take: None,
					params: &stored_ctx.params,
					stack: &stored_ctx.stack,
				};

				// Evaluate the filter expression
				let result = evaluate(&eval_ctx, filter_expr)?;

				// Create filter mask from result
				let filter_mask = match result.data() {
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
					_ => panic!("filter expression must column to a boolean column"),
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

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		self.input.headers()
	}
}
