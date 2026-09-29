// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::{
	expression::Expression,
	value::{batch::batch, column::headers::ColumnHeaders},
};
use reifydb_evaluate::expression::{
	compile::compile_expression,
	context::{CompileContext, EvalContext},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::{Value, column_view::ColumnView},
};
use tracing::instrument;

use super::common::{
	JoinContext, JoinSlot, build_eval_columns, load_and_merge_all, materialize_join, resolve_column_names,
	user_row, user_views,
};
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode, eval_context_from_query},
};

#[derive(Clone, Copy, PartialEq)]
enum NestedLoopMode {
	Inner,
	Left,
}

pub struct NestedLoopJoinNode {
	left: Box<dyn QueryNode>,
	right: Box<dyn QueryNode>,
	on: Vec<Expression>,
	alias: Option<Fragment>,
	mode: NestedLoopMode,
	headers: Option<ColumnHeaders>,
	context: JoinContext,
}

impl NestedLoopJoinNode {
	pub(crate) fn new_inner(
		left: Box<dyn QueryNode>,
		right: Box<dyn QueryNode>,
		on: Vec<Expression>,
		alias: Option<Fragment>,
	) -> Self {
		Self {
			left,
			right,
			on,
			alias,
			mode: NestedLoopMode::Inner,
			headers: None,
			context: JoinContext::new(),
		}
	}

	pub(crate) fn new_left(
		left: Box<dyn QueryNode>,
		right: Box<dyn QueryNode>,
		on: Vec<Expression>,
		alias: Option<Fragment>,
	) -> Self {
		Self {
			left,
			right,
			on,
			alias,
			mode: NestedLoopMode::Left,
			headers: None,
			context: JoinContext::new(),
		}
	}
}

impl QueryNode for NestedLoopJoinNode {
	#[instrument(level = "trace", skip_all, name = "volcano::join::nested_loop::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		self.context.compiled =
			self.on.iter().map(|e| compile_expression(&compile_ctx, e)).collect::<Result<_>>()?;
		self.context.set(ctx);
		self.left.initialize(rx, ctx)?;
		self.right.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::join::nested_loop::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_initialized(), "NestedLoopJoinNode::next() called before initialize()");
		}
		let _stored_ctx = self.context.get();

		if self.headers.is_some() {
			return Ok(None);
		}

		let left_columns = load_and_merge_all(&mut self.left, rx, ctx)?;
		let right_columns = load_and_merge_all(&mut self.right, rx, ctx)?;

		let left_rows = left_columns.num_rows();
		let right_rows = right_columns.num_rows();

		let resolved = resolve_column_names(&left_columns, &right_columns, &self.alias, None);

		let session = eval_context_from_query(ctx);
		let (left_picks, right_picks) =
			self.probe(&session, &left_columns, &right_columns, left_rows, right_rows)?;

		let columns = materialize_join(
			&resolved.qualified_names,
			&[JoinSlot {
				columns: &left_columns,
				picks: &left_picks,
			}],
			&right_columns,
			&[],
			&right_picks,
			0,
		)?;

		self.headers = Some(ColumnHeaders::from_batch(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}

impl NestedLoopJoinNode {
	#[instrument(level = "trace", skip_all, name = "volcano::join::nested_loop::probe")]
	fn probe(
		&self,
		session: &EvalContext,
		left_columns: &RecordBatch,
		right_columns: &RecordBatch,
		left_rows: usize,
		right_rows: usize,
	) -> Result<(Vec<usize>, Vec<Option<usize>>)> {
		let mut left_picks: Vec<usize> = Vec::new();
		let mut right_picks: Vec<Option<usize>> = Vec::new();
		let left_views = user_views(left_columns)?;
		let right_views = user_views(right_columns)?;

		for i in 0..left_rows {
			let left_row = user_row(&left_views, i);

			let mut matched = false;
			for j in 0..right_rows {
				let right_row = user_row(&right_views, j);

				let eval_columns = build_eval_columns(
					&left_views,
					&right_views,
					&left_row,
					&right_row,
					&self.alias,
				);

				let exec_ctx = session.with_eval_join(batch(eval_columns)?);

				let mut all_true = true;
				for compiled_expr in &self.context.compiled {
					let col = compiled_expr.execute(&exec_ctx)?;
					all_true &= matches!(
						ColumnView::try_from(&col)?.get_value(0),
						Value::Boolean(true)
					);
				}

				if all_true {
					left_picks.push(i);
					right_picks.push(Some(j));
					matched = true;
				}
			}

			if self.mode == NestedLoopMode::Left && !matched {
				left_picks.push(i);
				right_picks.push(None);
			}
		}

		Ok((left_picks, right_picks))
	}
}
