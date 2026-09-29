// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	expression::{CallExpression, Expression, name::display_label},
	value::column::{
		builder::ColumnBuilder,
		factory::rename,
		view::group_by::{GroupId, GroupRows},
	},
};
use reifydb_routine_abi::{FunctionKind, context::FunctionContext, error::RoutineError};
use reifydb_value::{error::Error, value::value_type::ValueType};

use crate::{Result, error::EvaluateError, expression::context::EvalContext};

pub(crate) fn call_builtin(
	ctx: &EvalContext,
	call: &CallExpression,
	arguments: &[(FieldRef, ArrayRef)],
) -> Result<(FieldRef, ArrayRef)> {
	let function_name = call.func.0.text();
	let fn_fragment = call.func.0.clone();
	let result_label = display_label(&Expression::Call(call.clone()));

	assert!(
		ctx.symbols.get_function(function_name).is_none(),
		"UDF '{}' should have been hoisted to UdfEvalNode",
		function_name
	);

	let routine = ctx.routines.get_function(function_name).ok_or_else(|| -> Error {
		EvaluateError::UnknownFunction {
			name: function_name.to_string(),
			fragment: fn_fragment.clone(),
		}
		.into()
	})?;

	let mut fn_ctx = FunctionContext {
		fragment: fn_fragment.clone(),
		identity: ctx.identity,
		row_count: ctx.row_count,
		runtime_context: ctx.runtime_context,
	};

	if ctx.is_aggregate_context && routine.kinds().contains(&FunctionKind::Aggregate) {
		let mut accumulator = routine
			.accumulator(&mut fn_ctx, &[])
			.map_err(|e| e.with_context(fn_fragment.clone(), false))?
			.ok_or_else(|| RoutineError::FunctionExecutionFailed {
				function: fn_fragment.clone(),
				reason: format!("Function {} is not an aggregate", function_name),
			})?;

		let column = if call.args.is_empty() {
			ColumnBuilder::with_capacity(ValueType::Int4, ctx.row_count).finish("dummy")
		} else {
			arguments[0].clone()
		};

		let all_rows: GroupRows = vec![(GroupId(0), (0..ctx.row_count).collect())];

		accumulator.update(&[column], &all_rows).map_err(|e| e.with_context(fn_fragment.clone(), false))?;

		let (_keys, result_data) = accumulator.finalize().map_err(|e| e.with_context(fn_fragment, false))?;

		return Ok(rename(result_data, result_label.text()));
	}

	let result = routine.call(&mut fn_ctx, arguments).map_err(|e| e.with_context(fn_fragment, false))?;
	Ok(rename(result, result_label.text()))
}
