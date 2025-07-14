// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvalutationContext, Evaluator};
use crate::frame::Column;
use reifydb_core::Span;
use reifydb_rql::expression::{AccessTableExpression, ColumnExpression, Expression};

impl Evaluator {
    pub(crate) fn access(
		&mut self,
		expr: &AccessTableExpression,
		ctx: &EvalutationContext,
    ) -> crate::evaluate::Result<Column> {
        self.evaluate(
            &Expression::Column(ColumnExpression(Span {
                offset: expr.table.offset,
                line: expr.table.line,
                fragment: format!("{}_{}", expr.table.fragment, expr.column.fragment),
            })),
            &ctx,
        )
    }
}
