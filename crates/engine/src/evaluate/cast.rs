// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Evaluator};
use crate::frame::ColumnValues;
use reifydb_rql::expression::{CastExpression, Expression};
use std::ops::Deref;

impl Evaluator {
    pub(crate) fn cast(
        &mut self,
        cast: &CastExpression,
        ctx: &Context,
    ) -> evaluate::Result<ColumnValues> {
        let values = match cast.expression.deref() {
            Expression::Constant(expr) => self.constant_of(expr, cast.to.kind, ctx)?,
            expr => self.evaluate(expr, ctx)?,
        };

        Ok(values.adjust_column(cast.to.kind, ctx, cast.expression.lazy_span()).unwrap()) // FIXME
    }
}
