// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::Column;
use reifydb_rql::expression::ExtendExpression;

use super::StandardColumnEvaluator;
use crate::evaluate::ColumnEvaluationContext;

impl StandardColumnEvaluator {
	pub(super) fn extend_expr<'a>(
		&self,
		ctx: &ColumnEvaluationContext,
		expr: &ExtendExpression,
	) -> crate::Result<Column> {
		if expr.expressions.len() == 1 {
			return self.evaluate(ctx, &expr.expressions[0]);
		}
		unreachable!("Multi-field EXTEND expressions are not supported")
	}
}
