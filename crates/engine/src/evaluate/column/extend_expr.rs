// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::Column;
use reifydb_rql::expression::ExtendExpression;

use super::StandardColumnEvaluator;
use crate::evaluate::ColumnEvaluationContext;

impl StandardColumnEvaluator {
	pub(super) fn extend_expr<'a>(
		&self,
		ctx: &ColumnEvaluationContext<'a>,
		expr: &ExtendExpression<'a>,
	) -> crate::Result<Column<'a>> {
		if expr.expressions.len() == 1 {
			return self.evaluate(ctx, &expr.expressions[0]);
		}
		unreachable!("Multi-field EXTEND expressions are not supported")
	}
}
