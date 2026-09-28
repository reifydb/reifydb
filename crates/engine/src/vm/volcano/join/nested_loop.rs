// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_evaluate::expression::{
	compile::compile_expression,
	context::{CompileContext, EvalContext},
};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, reifydb_assertions, value::Value};
use tracing::instrument;

use super::common::{
	JoinContext, JoinSlot, NO_MATCH, build_eval_columns, load_and_merge_all, materialize_join, resolve_column_names,
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
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_initialized(), "NestedLoopJoinNode::next() called before initialize()");
		}
		let _stored_ctx = self.context.get();

		if self.headers.is_some() {
			return Ok(None);
		}

		let left_columns = load_and_merge_all(&mut self.left, rx, ctx)?;
		let right_columns = load_and_merge_all(&mut self.right, rx, ctx)?;

		let left_rows = left_columns.row_count();
		let right_rows = right_columns.row_count();

		let resolved = resolve_column_names(&left_columns, &right_columns, &self.alias, None);

		let session = eval_context_from_query(ctx);
		let (left_picks, right_picks) =
			self.probe(&session, &left_columns, &right_columns, left_rows, right_rows)?;

		let left_rownum = self.left.headers().is_some_and(|h| h.row_numbers);
		let columns = materialize_join(
			&resolved.qualified_names,
			&[JoinSlot {
				columns: &left_columns.columns,
				system: &left_columns.system,
				picks: &left_picks,
			}],
			&right_columns.columns,
			&right_picks,
			right_columns.time(),
			left_rownum,
			0,
		)?;

		self.headers = Some(ColumnHeaders::from_columns(&columns));
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
		left_columns: &Columns,
		right_columns: &Columns,
		left_rows: usize,
		right_rows: usize,
	) -> Result<(Vec<usize>, Vec<usize>)> {
		let mut left_picks: Vec<usize> = Vec::new();
		let mut right_picks: Vec<usize> = Vec::new();

		for i in 0..left_rows {
			let left_row = left_columns.get_row(i);

			let mut matched = false;
			for j in 0..right_rows {
				let right_row = right_columns.get_row(j);

				let eval_columns = build_eval_columns(
					left_columns,
					right_columns,
					&left_row,
					&right_row,
					&self.alias,
				);

				let exec_ctx = session.with_eval_join(Columns::new(eval_columns));

				let mut all_true = true;
				for compiled_expr in &self.context.compiled {
					let col = compiled_expr.execute(&exec_ctx)?;
					all_true &= matches!(col.data().get_value(0), Value::Boolean(true));
				}

				if all_true {
					left_picks.push(i);
					right_picks.push(j);
					matched = true;
				}
			}

			if self.mode == NestedLoopMode::Left && !matched {
				left_picks.push(i);
				right_picks.push(NO_MATCH);
			}
		}

		Ok((left_picks, right_picks))
	}
}
