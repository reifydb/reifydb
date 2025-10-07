// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{ColumnEvaluationContext, evaluate::expression::VariableExpression},
	stack::Variable,
	value::column::{Column, ColumnData},
};
use reifydb_type::{
	Fragment,
	diagnostic::runtime::{variable_is_dataframe, variable_not_found},
	return_error,
};

use super::StandardColumnEvaluator;

impl StandardColumnEvaluator {
	pub(super) fn variable<'a>(
		&self,
		ctx: &ColumnEvaluationContext<'a>,
		expr: &VariableExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let variable_name = expr.name();

		// Look up the variable in the stack
		match ctx.stack.get(variable_name) {
			Some(Variable::Scalar(value)) => {
				// Scalar variables can be used directly in expressions
				// Create a column containing the scalar value, repeated for each row
				let value_type = value.get_type();
				let mut data = ColumnData::with_capacity(value_type, ctx.row_count);

				for _ in 0..ctx.row_count {
					data.push_value(value.clone());
				}

				Ok(Column {
					name: Fragment::owned_internal(variable_name),
					data,
				})
			}
			Some(Variable::Frame(_)) => {
				// Frame variables cannot be used directly in scalar expressions
				// Return a clear error with helpful guidance
				return_error!(variable_is_dataframe(variable_name));
			}
			None => {
				// Variable not found - return error
				return_error!(variable_not_found(variable_name));
			}
		}
	}
}
