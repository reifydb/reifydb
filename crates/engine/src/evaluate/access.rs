// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::frame::{FrameColumn, TableQualified};
use reifydb_rql::expression::{AccessTableExpression, ColumnExpression, Expression};

impl Evaluator {
    pub(crate) fn access(
        &mut self,
        expr: &AccessTableExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let table = expr.table.fragment.clone();
        let column = expr.column.fragment.clone();

        let values = self
            .evaluate(
                &Expression::Column(ColumnExpression(OwnedSpan {
                    column: expr.table.column,
                    line: expr.table.line,
                    fragment: format!("{}.{}", table, column),
                })),
                &ctx,
            )?
            .values().clone();

        Ok(FrameColumn::TableQualified(TableQualified { table, name: column, values }))
    }
}
