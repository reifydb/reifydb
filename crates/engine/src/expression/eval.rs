// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::Column;
use reifydb_function::registry::Functions;
use reifydb_rql::expression::Expression;
use reifydb_runtime::clock::Clock;

use crate::expression::context::EvalContext;

pub fn evaluate(ctx: &EvalContext, expr: &Expression, _functions: &Functions, _clock: &Clock) -> crate::Result<Column> {
	use crate::expression::{compile::compile_expression, context::CompileContext};

	let compile_ctx = CompileContext {
		functions: ctx.functions,
		symbol_table: ctx.symbol_table,
	};
	let compiled = compile_expression(&compile_ctx, expr)?;
	let column = compiled.execute(ctx)?;

	// Ensures that result column data type matches the expected target column type
	if let Some(ty) = ctx.target.as_ref().map(|c| c.column_type()) {
		let data = crate::expression::cast::cast_column_data(ctx, &column.data(), ty, &expr.lazy_fragment())?;
		Ok(Column {
			name: column.name,
			data,
		})
	} else {
		Ok(column)
	}
}
