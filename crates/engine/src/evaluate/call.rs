// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	error,
	interface::{
		Evaluator,
		evaluate::expression::{CallExpression, Expression},
	},
	value::columnar::{Column, ColumnComputed, Columns},
};
use reifydb_type::diagnostic::function;

use crate::{
	evaluate::{EvaluationContext, StandardEvaluator},
	function::ScalarFunctionContext,
};

impl StandardEvaluator {
	pub(crate) fn call<'a>(
		&self,
		ctx: &EvaluationContext<'a>,
		call: &CallExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let arguments = self.evaluate_arguments(ctx, &call.args)?;
		let function = call.func.0.text();

		let functor = self
			.functions
			.get_scalar(function)
			.ok_or(error!(function::unknown_function(function.to_string())))?;

		let row_count = ctx.row_count;
		Ok(Column::Computed(ColumnComputed {
			name: call.full_fragment_owned(),
			data: functor.scalar(ScalarFunctionContext {
				columns: &arguments,
				row_count,
			})?,
		}))
	}

	fn evaluate_arguments<'a>(
		&self,
		ctx: &EvaluationContext<'a>,
		expressions: &Vec<Expression<'a>>,
	) -> crate::Result<Columns<'a>> {
		let mut result: Vec<Column<'a>> = Vec::with_capacity(expressions.len());

		for expression in expressions {
			result.push(self.evaluate(ctx, expression)?)
		}

		Ok(Columns::new(result))
	}
}
