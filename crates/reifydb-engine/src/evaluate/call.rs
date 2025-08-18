// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	error,
	interface::{
		Evaluator,
		evaluate::expression::{CallExpression, Expression},
	},
	result::error::diagnostic::function,
};

use crate::{
	columnar::{Column, ColumnQualified, Columns},
	evaluate::{EvaluationContext, StandardEvaluator},
	function::ScalarFunctionContext,
};

impl StandardEvaluator {
	pub(crate) fn call(
		&self,
		ctx: &EvaluationContext,
		call: &CallExpression,
	) -> crate::Result<Column> {
		let arguments = self.evaluate_arguments(ctx, &call.args)?;
		let function = call.func.0.fragment();

		let functor = self
			.functions
			.get_scalar(function)
			.ok_or(error!(function::unknown_function(
				function.to_string()
			)))?;

		let row_count = ctx.row_count;
		Ok(Column::ColumnQualified(ColumnQualified {
			name: call.fragment().fragment().into(),
			data: functor.scalar(ScalarFunctionContext {
				columns: &arguments,
				row_count,
			})?,
		}))
	}

	fn evaluate_arguments<'a>(
		&self,
		ctx: &EvaluationContext,
		expressions: &Vec<Expression>,
	) -> crate::Result<Columns> {
		let mut result: Vec<Column> =
			Vec::with_capacity(expressions.len());

		for expression in expressions {
			result.push(self.evaluate(ctx, expression)?)
		}

		Ok(Columns::new(result))
	}
}
