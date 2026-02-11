// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, row_number::RowNumber},
};
use tracing::instrument;

use super::common::{JoinContext, build_eval_columns, load_and_merge_all, resolve_column_names};
use crate::{
	evaluate::compiled::{CompileContext, EvalContext, compile_expression},
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct InnerJoinNode {
	left: Box<dyn QueryNode>,
	right: Box<dyn QueryNode>,
	on: Vec<Expression>,
	alias: Option<Fragment>,
	headers: Option<ColumnHeaders>,
	context: JoinContext,
}

impl InnerJoinNode {
	pub(crate) fn new(
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
			headers: None,
			context: JoinContext::new(),
		}
	}
}

impl QueryNode for InnerJoinNode {
	#[instrument(level = "trace", skip_all, name = "volcano::join::inner::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		let compile_ctx = CompileContext {
			functions: &ctx.services.functions,
			symbol_table: &ctx.stack,
		};
		self.context.compiled =
			self.on.iter().map(|e| compile_expression(&compile_ctx, e).expect("compile")).collect();
		self.context.set(ctx);
		self.left.initialize(rx, ctx)?;
		self.right.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::join::inner::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_initialized(), "InnerJoinNode::next() called before initialize()");
		let _stored_ctx = self.context.get();

		if self.headers.is_some() {
			return Ok(None);
		}

		let left_columns = load_and_merge_all(&mut self.left, rx, ctx)?;
		let right_columns = load_and_merge_all(&mut self.right, rx, ctx)?;

		let left_rows = left_columns.row_count();
		let right_rows = right_columns.row_count();
		let left_row_numbers = left_columns.row_numbers.to_vec();

		// Resolve column names with conflict detection
		let resolved = resolve_column_names(&left_columns, &right_columns, &self.alias, None);

		let mut result_rows = Vec::new();
		let mut result_row_numbers: Vec<RowNumber> = Vec::new();

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

				let exec_ctx = EvalContext {
					target: None,
					columns: Columns::new(eval_columns),
					row_count: 1,
					take: Some(1),
					params: &ctx.params,
					symbol_table: &ctx.stack,
					is_aggregate_context: false,
					functions: &ctx.services.functions,
					clock: &ctx.services.clock,
				};

				let all_true = self.context.compiled.iter().fold(true, |acc, compiled_expr| {
					let col = compiled_expr.execute(&exec_ctx).unwrap();
					matches!(col.data().get_value(0), Value::Boolean(true)) && acc
				});

				if all_true {
					let mut combined = left_row.clone();
					combined.extend(right_row.clone());
					result_rows.push(combined);
					if !left_row_numbers.is_empty() {
						result_row_numbers.push(left_row_numbers[i]);
					}
				}
			}
		}

		// Create columns with conflict-resolved names
		let names_refs: Vec<&str> = resolved.qualified_names.iter().map(|s| s.as_str()).collect();
		let columns = if result_row_numbers.is_empty() {
			Columns::from_rows(&names_refs, &result_rows)
		} else {
			Columns::from_rows_with_row_numbers(&names_refs, &result_rows, result_row_numbers)
		};

		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
