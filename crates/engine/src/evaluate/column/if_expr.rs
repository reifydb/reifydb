// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::{Column, ColumnData};
use reifydb_rql::expression::IfExpression;
use reifydb_type::{Fragment, Type, Value};

use super::StandardColumnEvaluator;
use crate::evaluate::ColumnEvaluationContext;

/// Evaluate if a value is truthy according to RQL semantics
fn is_truthy(value: &Value) -> bool {
	match value {
		Value::Boolean(true) => true,
		Value::Boolean(false) => false,
		Value::Undefined => false,
		// For numeric values, treat zero as false, non-zero as true
		Value::Int1(0) | Value::Int2(0) | Value::Int4(0) | Value::Int8(0) | Value::Int16(0) => false,
		Value::Uint1(0) | Value::Uint2(0) | Value::Uint4(0) | Value::Uint8(0) | Value::Uint16(0) => false,
		Value::Int1(_) | Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Int16(_) => true,
		Value::Uint1(_) | Value::Uint2(_) | Value::Uint4(_) | Value::Uint8(_) | Value::Uint16(_) => true,
		// For strings, treat empty as false, non-empty as true
		Value::Utf8(s) => !s.is_empty(),
		// For other values, treat as truthy (we'll handle floats separately if needed)
		_ => true,
	}
}

impl StandardColumnEvaluator {
	pub(super) fn if_expr<'a>(
		&self,
		ctx: &ColumnEvaluationContext<'a>,
		expr: &IfExpression<'a>,
	) -> crate::Result<Column<'a>> {
		// Evaluate the condition
		let condition_column = self.evaluate(ctx, &expr.condition)?;

		// Create result column data that will be populated row by row
		let mut result_data = None;
		let mut result_name = Fragment::owned_internal("if_result");

		// Process each row
		for row_idx in 0..ctx.row_count {
			// Get condition value for this row
			let condition_value = condition_column.data().get_value(row_idx);

			// Determine which branch to take based on condition
			let branch_result = if is_truthy(&condition_value) {
				self.evaluate(ctx, &expr.then_expr)?
			} else {
				let mut found_branch = false;
				let mut branch_column = None;

				for else_if in &expr.else_ifs {
					let else_if_condition = self.evaluate(ctx, &else_if.condition)?;
					let else_if_condition_value = else_if_condition.data().get_value(row_idx);

					if is_truthy(&else_if_condition_value) {
						branch_column = Some(self.evaluate(ctx, &else_if.then_expr)?);
						found_branch = true;
						break;
					}
				}

				if found_branch {
					branch_column.unwrap()
				} else if let Some(else_expr) = &expr.else_expr {
					self.evaluate(ctx, else_expr)?
				} else {
					let mut data = ColumnData::with_capacity(Type::Undefined, ctx.row_count);
					for _ in 0..ctx.row_count {
						data.push_undefined();
					}
					Column {
						name: Fragment::owned_internal("undefined"),
						data,
					}
				}
			};

			// Initialize result data with proper type on first iteration
			if result_data.is_none() {
				result_data =
					Some(ColumnData::with_capacity(branch_result.data().get_type(), ctx.row_count));
				result_name = branch_result.name.clone();
			}

			// Add the value from the selected branch to our result
			let branch_value = branch_result.data().get_value(row_idx);
			result_data.as_mut().unwrap().push_value(branch_value);
		}

		Ok(Column {
			name: result_name,
			data: result_data.unwrap_or_else(|| ColumnData::with_capacity(Type::Undefined, 0)),
		})
	}
}
