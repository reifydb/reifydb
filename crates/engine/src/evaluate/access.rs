// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::FrameColumn;
use reifydb_core::OwnedSpan;
use reifydb_rql::expression::{AccessTableExpression, ColumnExpression, Expression};

impl Evaluator {
    pub(crate) fn access(
		&mut self,
		expr: &AccessTableExpression,
		ctx: &EvaluationContext,
    ) -> crate::evaluate::Result<FrameColumn> {
        self.evaluate(
            &Expression::Column(ColumnExpression(OwnedSpan {
                column: expr.table.column,
                line: expr.table.line,
                fragment: format!("{}_{}", expr.table.fragment, expr.column.fragment),
            })),
            &ctx,
        )
    }
}
