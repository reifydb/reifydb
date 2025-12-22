// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	BitVec,
	value::column::{ColumnData, headers::ColumnHeaders},
};
use reifydb_rql::expression::Expression;
use tracing::instrument;

use crate::{
	StandardTransaction,
	evaluate::column::{ColumnEvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct FilterNode {
	input: Box<ExecutionPlan>,
	expressions: Vec<Expression>,
	context: Option<Arc<ExecutionContext>>,
}

impl FilterNode {
	pub fn new(input: Box<ExecutionPlan>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			context: None,
		}
	}
}

#[async_trait]
impl QueryNode for FilterNode {
	#[instrument(level = "trace", skip_all, name = "query::filter::initialize")]
	async fn initialize<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx).await?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "query::filter::next")]
	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "FilterNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		while let Some(Batch {
			mut columns,
		}) = self.input.next(rx, ctx).await?
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
					is_aggregate_context: false,
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

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}
