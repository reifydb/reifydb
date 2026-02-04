// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::Column;
use reifydb_rql::expression::{Expression, ExtendExpression};
use reifydb_type::fragment::Fragment;

use super::StandardColumnEvaluator;
use crate::evaluate::ColumnEvaluationContext;

impl StandardColumnEvaluator {
	pub(super) fn extend_expr(
		&self,
		ctx: &ColumnEvaluationContext,
		expr: &ExtendExpression,
	) -> crate::Result<Column> {
		if expr.expressions.len() == 1 {
			return self.evaluate(ctx, &expr.expressions[0]);
		}
		let columns = self.extend_expr_multi(ctx, expr)?;
		Ok(columns.into_iter().next().unwrap())
	}

	pub(super) fn extend_expr_multi(
		&self,
		ctx: &ColumnEvaluationContext,
		expr: &ExtendExpression,
	) -> crate::Result<Vec<Column>> {
		let mut result = Vec::with_capacity(expr.expressions.len());

		for inner_expr in &expr.expressions {
			let column = self.evaluate(ctx, inner_expr)?;

			let name = match inner_expr {
				Expression::Alias(alias) => alias.alias.name().to_string(),
				Expression::Column(col) => col.0.name.text().to_string(),
				Expression::AccessSource(access) => access.column.name.text().to_string(),
				_ => inner_expr.full_fragment_owned().text().to_string(),
			};

			result.push(Column {
				name: Fragment::internal(name),
				data: column.data,
			});
		}

		Ok(result)
	}
}
