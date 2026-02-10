// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_rql::expression::VariableExpression;
use reifydb_type::{
	error::diagnostic::runtime::{variable_is_dataframe, variable_not_found},
	fragment::Fragment,
	return_error,
};

use super::StandardColumnEvaluator;
use crate::{evaluate::ColumnEvaluationContext, vm::stack::Variable};

impl StandardColumnEvaluator {
	pub(super) fn variable<'a>(
		&self,
		ctx: &ColumnEvaluationContext,
		expr: &VariableExpression,
	) -> crate::Result<Column> {
		let variable_name = expr.name();

		// Special case: $env variable returns environment dataframe
		if variable_name == "env" {
			// Columns variables cannot be used directly in scalar expressions
			// Return a clear error with helpful guidance
			return_error!(variable_is_dataframe(variable_name));
		}

		// Look up the variable in the stack
		match ctx.symbol_table.get(variable_name) {
			Some(Variable::Scalar(value)) => {
				let mut data = ColumnData::with_capacity(value.get_type(), ctx.row_count);
				for _ in 0..ctx.row_count {
					data.push_value(value.clone());
				}

				Ok(Column {
					name: Fragment::internal(variable_name),
					data,
				})
			}
			Some(Variable::Columns(_))
			| Some(Variable::ForIterator {
				..
			}) => {
				// Columns variables cannot be used directly in scalar expressions
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
