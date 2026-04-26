// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{
	ColumnWithName, buffer::ColumnBuffer, columns::Columns, view::group_by::GroupByView,
};
use reifydb_routine::routine::{
	FunctionContext, FunctionKind, RoutineEnv, RoutineError,
};
use reifydb_rql::expression::CallExpression;
use reifydb_type::{
	error::Error,
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{Result, error::EngineError, expression::context::EvalContext};

pub(crate) fn call_builtin(ctx: &EvalContext, call: &CallExpression, arguments: Columns) -> Result<ColumnWithName> {
	let function_name = call.func.0.text();
	let fn_fragment = call.func.0.clone();

	// UDFs are hoisted to UdfEvalNode during volcano initialization.
	// If one reaches here, it's a bug in the query plan.
	assert!(
		ctx.symbols.get_function(function_name).is_none(),
		"UDF '{}' should have been hoisted to UdfEvalNode",
		function_name
	);

	let routine = ctx.routines.get_function(function_name).ok_or_else(|| -> Error {
		EngineError::UnknownFunction {
			name: function_name.to_string(),
			fragment: fn_fragment.clone(),
		}
		.into()
	})?;

	let mut fn_ctx = FunctionContext {
		env: RoutineEnv {
			fragment: fn_fragment.clone(),
			identity: ctx.identity,
			row_count: ctx.row_count,
			runtime_context: ctx.runtime_context,
		},
	};

	// Aggregate scalar-context fast path (e.g. `sum(x)` inside a SELECT projection
	// during GROUP-BY-less aggregation). Mirrors today's behaviour: build a single
	// group, run the accumulator, return the finalised value.
	if ctx.is_aggregate_context && routine.kinds().contains(&FunctionKind::Aggregate) {
		let mut accumulator = routine.accumulator(&mut fn_ctx).ok_or_else(|| RoutineError::FunctionExecutionFailed {
			function: fn_fragment.clone(),
			reason: format!("Function {} is not an aggregate", function_name),
		})?;

		let column = if call.args.is_empty() {
			ColumnWithName {
				name: Fragment::internal("dummy"),
				data: ColumnBuffer::with_capacity(Type::Int4, ctx.row_count),
			}
		} else {
			ColumnWithName::new(arguments.name_at(0).clone(), arguments[0].clone())
		};

		let mut group_view = GroupByView::new();
		let all_indices: Vec<usize> = (0..ctx.row_count).collect();
		group_view.insert(Vec::<Value>::new(), all_indices);

		accumulator
			.update(&Columns::new(vec![column]), &group_view)
			.map_err(|e| e.with_context(fn_fragment.clone(), false))?;

		let (_keys, result_data) = accumulator.finalize().map_err(|e| e.with_context(fn_fragment, false))?;

		return Ok(ColumnWithName::new(call.full_fragment_owned(), result_data));
	}

	let result_columns = routine.call(&mut fn_ctx, &arguments).map_err(|e| e.with_context(fn_fragment, false))?;

	// For scalar, we expect 1 column. For generator in scalar context, we take the first column.
	if result_columns.is_empty() {
		return Err(RoutineError::FunctionExecutionFailed {
			function: call.func.0.clone(),
			reason: "Function returned no columns".to_string(),
		}
		.into());
	}
	let result_data = result_columns.data_at(0).clone();
	Ok(ColumnWithName::new(call.full_fragment_owned(), result_data))
}
