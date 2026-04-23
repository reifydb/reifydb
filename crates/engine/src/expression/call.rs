// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{
	ColumnWithName, buffer::ColumnBuffer, columns::Columns, view::group_by::GroupByView,
};
use reifydb_routine::function::{FunctionCapability, FunctionContext, error::FunctionError, registry::Functions};
use reifydb_rql::expression::CallExpression;
use reifydb_type::{
	error::Error,
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{Result, error::EngineError, expression::context::EvalContext};

pub(crate) fn call_builtin(
	ctx: &EvalContext,
	call: &CallExpression,
	arguments: Columns,
	functions: &Functions,
) -> Result<ColumnWithName> {
	let function_name = call.func.0.text();
	let fn_fragment = call.func.0.clone();

	// UDFs are hoisted to UdfEvalNode during volcano initialization.
	// If one reaches here, it's a bug in the query plan.
	assert!(
		ctx.symbols.get_function(function_name).is_none(),
		"UDF '{}' should have been hoisted to UdfEvalNode",
		function_name
	);

	let function = functions.get(function_name).ok_or_else(|| -> Error {
		EngineError::UnknownFunction {
			name: function_name.to_string(),
			fragment: fn_fragment.clone(),
		}
		.into()
	})?;

	let fn_ctx = FunctionContext::new(fn_fragment.clone(), ctx.runtime_context, ctx.identity, ctx.row_count);

	// Check if we're in aggregation context and if function exists as aggregate
	if ctx.is_aggregate_context && function.capabilities().contains(&FunctionCapability::Aggregate) {
		let mut accumulator = function.accumulator(&fn_ctx).ok_or_else(|| FunctionError::ExecutionFailed {
			function: fn_fragment.clone(),
			reason: format!("Function {} is not an aggregate", function_name),
		})?;

		let column = if call.args.is_empty() {
			ColumnWithName {
				name: Fragment::internal("dummy"),
				data: ColumnBuffer::with_capacity(Type::Int4, ctx.row_count),
			}
		} else {
			arguments[0].clone()
		};

		let mut group_view = GroupByView::new();
		let all_indices: Vec<usize> = (0..ctx.row_count).collect();
		group_view.insert(Vec::<Value>::new(), all_indices);

		accumulator
			.update(&Columns::new(vec![column]), &group_view)
			.map_err(|e| e.with_context(fn_fragment.clone()))?;

		let (_keys, result_data) = accumulator.finalize().map_err(|e| e.with_context(fn_fragment))?;

		return Ok(ColumnWithName::new(call.full_fragment_owned(), result_data));
	}

	let result_columns = function.call(&fn_ctx, &arguments).map_err(|e| e.with_context(fn_fragment))?;

	// For scalar, we expect 1 column. For generator in scalar context, we take the first column.
	let result_column = result_columns.into_iter().next().ok_or_else(|| FunctionError::ExecutionFailed {
		function: call.func.0.clone(),
		reason: "Function returned no columns".to_string(),
	})?;

	Ok(ColumnWithName::new(call.full_fragment_owned(), result_column.data))
}
