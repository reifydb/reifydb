// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::ColumnWithName;
use reifydb_rql::expression::Expression;

use crate::{
	Result,
	expression::{
		cast::cast_column_data,
		compile::compile_expression,
		context::{CompileContext, EvalContext},
	},
};

pub fn evaluate(ctx: &EvalContext, expr: &Expression) -> Result<ColumnWithName> {
	let compile_ctx = CompileContext {		symbols: ctx.symbols,
	};
	let compiled = compile_expression(&compile_ctx, expr)?;
	let column = compiled.execute(ctx)?;

	// Ensures that result column data type matches the expected target column type
	if let Some(ty) = ctx.target.as_ref().map(|c| c.column_type()) {
		let data = cast_column_data(ctx, column.data(), ty, &expr.lazy_fragment())?;
		Ok(ColumnWithName {
			name: column.name,
			data,
		})
	} else {
		Ok(column)
	}
}
