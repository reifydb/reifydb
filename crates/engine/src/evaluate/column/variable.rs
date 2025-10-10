// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::{Column, ColumnData};
use reifydb_rql::expression::VariableExpression;
use reifydb_type::{
	Fragment,
	diagnostic::runtime::{variable_is_dataframe, variable_not_found},
	return_error,
};

use super::StandardColumnEvaluator;
use crate::{evaluate::ColumnEvaluationContext, stack::Variable};

impl StandardColumnEvaluator {
	pub(super) fn variable<'a>(
		&self,
		ctx: &ColumnEvaluationContext<'a>,
		expr: &VariableExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let variable_name = expr.name();

		// Special case: $env variable returns environment dataframe
		if variable_name == "env" {
			// Frame variables cannot be used directly in scalar expressions
			// Return a clear error with helpful guidance
			return_error!(variable_is_dataframe(variable_name));
		}

		// Look up the variable in the stack
		match ctx.stack.get(variable_name) {
			Some(Variable::Scalar(value)) => {
				let mut data = ColumnData::with_capacity(value.get_type(), ctx.row_count);
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
