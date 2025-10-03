// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::evaluate::expression::Expression,
	value::column::{Columns, headers::ColumnHeaders},
};
use reifydb_type::{Fragment, Value};

use super::common::{JoinContext, build_eval_columns, load_and_merge_all, resolve_column_names};
use crate::{
	StandardTransaction,
	evaluate::column::{ColumnEvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub struct InnerJoinNode<'a> {
	left: Box<ExecutionPlan<'a>>,
	right: Box<ExecutionPlan<'a>>,
	on: Vec<Expression<'a>>,
	alias: Option<Fragment<'a>>,
	headers: Option<ColumnHeaders<'a>>,
	context: JoinContext<'a>,
}

impl<'a> InnerJoinNode<'a> {
	pub fn new(
		left: Box<ExecutionPlan<'a>>,
		right: Box<ExecutionPlan<'a>>,
		on: Vec<Expression<'a>>,
		alias: Option<Fragment<'a>>,
	) -> Self {
		Self {
			left,
			right,
			on,
			alias,
			headers: None,
			context: JoinContext::new(),
		}
	}
}

impl<'a> QueryNode<'a> for InnerJoinNode<'a> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context.set(ctx);
		self.left.initialize(rx, ctx)?;
		self.right.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(&mut self, rx: &mut StandardTransaction<'a>) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_initialized(), "InnerJoinNode::next() called before initialize()");
		let ctx = self.context.get();

		if self.headers.is_some() {
			return Ok(None);
		}

		let left_columns = load_and_merge_all(&mut self.left, rx)?;
		let right_columns = load_and_merge_all(&mut self.right, rx)?;

		let left_rows = left_columns.row_count();
		let right_rows = right_columns.row_count();

		// Resolve column names with conflict detection
		let resolved = resolve_column_names(&left_columns, &right_columns, &self.alias, None);

		let mut result_rows = Vec::new();

		for i in 0..left_rows {
			let left_row = left_columns.get_row(i);

			for j in 0..right_rows {
				let right_row = right_columns.get_row(j);

				// Build evaluation columns
				let eval_columns = build_eval_columns(
					&left_columns,
					&right_columns,
					&left_row,
					&right_row,
					&self.alias,
				);

				let eval_ctx = ColumnEvaluationContext {
					target: None,
					columns: Columns::new(eval_columns),
					row_count: 1,
					take: Some(1),
					params: &ctx.params,
				};

				let all_true = self.on.iter().fold(true, |acc, cond| {
					let col = evaluate(&eval_ctx, cond).unwrap();
					matches!(col.data().get_value(0), Value::Boolean(true)) && acc
				});

				if all_true {
					let mut combined = left_row.clone();
					combined.extend(right_row.clone());
					result_rows.push(combined);
				}
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
