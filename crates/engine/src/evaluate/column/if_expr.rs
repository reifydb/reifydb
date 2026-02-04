// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_rql::expression::IfExpression;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};

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
	pub(super) fn if_expr(&self, ctx: &ColumnEvaluationContext, expr: &IfExpression) -> crate::Result<Column> {
		let columns = self.if_expr_multi(ctx, expr)?;
		Ok(columns.into_iter().next().unwrap_or_else(|| Column {
			name: Fragment::internal("undefined"),
			data: ColumnData::with_capacity(Type::Undefined, 0),
		}))
	}

	pub(super) fn if_expr_multi(
		&self,
		ctx: &ColumnEvaluationContext,
		expr: &IfExpression,
	) -> crate::Result<Vec<Column>> {
		let condition_column = self.evaluate(ctx, &expr.condition)?;

		let mut result_data: Option<Vec<ColumnData>> = None;
		let mut result_names: Vec<Fragment> = Vec::new();

		for row_idx in 0..ctx.row_count {
			let condition_value = condition_column.data().get_value(row_idx);

			let branch_results = if is_truthy(&condition_value) {
				self.evaluate_multi(ctx, &expr.then_expr)?
			} else {
				let mut found_branch = false;
				let mut branch_columns = None;

				for else_if in &expr.else_ifs {
					let else_if_condition = self.evaluate(ctx, &else_if.condition)?;
					let else_if_condition_value = else_if_condition.data().get_value(row_idx);

					if is_truthy(&else_if_condition_value) {
						branch_columns = Some(self.evaluate_multi(ctx, &else_if.then_expr)?);
						found_branch = true;
						break;
					}
				}

				if found_branch {
					branch_columns.unwrap()
				} else if let Some(else_expr) = &expr.else_expr {
					self.evaluate_multi(ctx, else_expr)?
				} else {
					let mut data = ColumnData::with_capacity(Type::Undefined, ctx.row_count);
					for _ in 0..ctx.row_count {
						data.push_undefined();
					}
					vec![Column {
						name: Fragment::internal("undefined"),
						data,
					}]
				}
			};

			if result_data.is_none() {
				result_data = Some(branch_results
					.iter()
					.map(|col| ColumnData::with_capacity(col.data().get_type(), ctx.row_count))
					.collect());
				result_names = branch_results.iter().map(|col| col.name.clone()).collect();
			}

			let data = result_data.as_mut().unwrap();
			for (i, branch_col) in branch_results.iter().enumerate() {
				if i < data.len() {
					let branch_value = branch_col.data().get_value(row_idx);
					data[i].push_value(branch_value);
				}
			}
		}

		let result_data = result_data.unwrap_or_default();
		let result: Vec<Column> = result_data
			.into_iter()
			.enumerate()
			.map(|(i, data)| Column {
				name: result_names.get(i).cloned().unwrap_or_else(|| Fragment::internal("column")),
				data,
			})
			.collect();

		if result.is_empty() {
			Ok(vec![Column {
				name: Fragment::internal("undefined"),
				data: ColumnData::with_capacity(Type::Undefined, 0),
			}])
		} else {
			Ok(result)
		}
	}
}
