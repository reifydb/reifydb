// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{expression::CallExpression, value::column::factory::rename};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_value::error::Error;

use crate::{Result, error::EvaluateError, expression::context::EvalContext};

pub(crate) fn call_builtin(
	ctx: &EvalContext,
	call: &CallExpression,
	routine: Option<&Arc<dyn Function>>,
	result_label: &str,
	arguments: &[(FieldRef, ArrayRef)],
) -> Result<(FieldRef, ArrayRef)> {
	let function_name = call.func.0.text();
	let fn_fragment = call.func.0.clone();

	assert!(
		ctx.symbols.get_function(function_name).is_none(),
		"UDF '{}' should have been hoisted to UdfEvalNode",
		function_name
	);

	let routine = routine.ok_or_else(|| -> Error {
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

	let result = routine.call(&mut fn_ctx, arguments).map_err(|e| e.with_context(fn_fragment, false))?;
	Ok(rename(result, result_label))
}
