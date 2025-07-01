// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::Column;
use reifydb_core::Span;
use reifydb_rql::expression::{AccessPropertyExpression, ColumnExpression, Expression};

impl Evaluator {
    pub(crate) fn access_property(
        &mut self,
        expr: &AccessPropertyExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<Column> {
        self.evaluate(
            &Expression::Column(ColumnExpression(Span {
                offset: expr.target.offset,
                line: expr.target.line,
                fragment: format!("{}_{}", expr.target.fragment, expr.property.fragment),
            })),
            &ctx,
        )
    }
}
