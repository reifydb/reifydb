// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error,
	interface::{AggregateFunction, AggregateFunctionContext, ScalarFunctionContext},
	value::column::{Column, ColumnData, Columns, GroupByView},
};
use reifydb_rql::expression::{CallExpression, Expression};
use reifydb_type::{Fragment, Value, diagnostic::function};

use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

impl StandardColumnEvaluator {
	pub(crate) fn call<'a>(&self, ctx: &ColumnEvaluationContext, call: &CallExpression) -> crate::Result<Column> {
		let function_name = call.func.0.text();

		// Check if we're in aggregation context and if function exists as aggregate
		// FIXME this is a quick hack - this should be derived from a call stack
		if ctx.is_aggregate_context {
			if let Some(aggregate_fn) = self.functions.get_aggregate(function_name) {
				return self.handle_aggregate_function(ctx, call, aggregate_fn);
			}
		}

		// Fall back to scalar function handling
		let arguments = self.evaluate_arguments(ctx, &call.args)?;
		let functor = self
			.functions
			.get_scalar(function_name)
			.ok_or(error!(function::unknown_function(function_name.to_string())))?;

		let row_count = ctx.row_count;
		Ok(Column {
			name: call.full_fragment_owned(),
			data: functor.scalar(ScalarFunctionContext {
				columns: &arguments,
				row_count,
			})?,
		})
	}

	fn handle_aggregate_function<'a>(
		&self,
		ctx: &ColumnEvaluationContext,
		call: &CallExpression,
		mut aggregate_fn: Box<dyn AggregateFunction>,
	) -> crate::Result<Column> {
		// Create a single group containing all row indices for aggregation
		let mut group_view = GroupByView::new();
		let all_indices: Vec<usize> = (0..ctx.row_count).collect();
		group_view.insert(Vec::<Value>::new(), all_indices); // Empty group key for single group

		// Determine which column to aggregate over
		let column = if call.args.is_empty() {
			// For count() with no arguments, create a dummy column
			Column {
				name: Fragment::internal("dummy"),
				data: ColumnData::int4_with_capacity(ctx.row_count),
			}
		} else {
			// For functions with arguments like sum(amount), use the first argument column
			let arguments = self.evaluate_arguments(ctx, &call.args)?;
			arguments[0].clone()
		};

		// Call the aggregate function
		aggregate_fn.aggregate(AggregateFunctionContext {
			column: &column,
			groups: &group_view,
		})?;

		// Finalize and get results
		let (_keys, result_data) = aggregate_fn.finalize()?;

		Ok(Column {
			name: call.full_fragment_owned(),
			data: result_data,
		})
	}

	fn evaluate_arguments<'a>(
		&self,
		ctx: &ColumnEvaluationContext,
		expressions: &Vec<Expression>,
	) -> crate::Result<Columns> {
		let mut result: Vec<Column> = Vec::with_capacity(expressions.len());

		for expression in expressions {
			result.push(self.evaluate(ctx, expression)?)
		}

		Ok(Columns::new(result))
	}
}
