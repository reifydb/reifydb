// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::Column;
use reifydb_rql::expression::AliasExpression;

impl Evaluator {
    pub(crate) fn alias(
        &mut self,
        expr: &AliasExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<Column> {
        let evaluated = self.evaluate(&expr.expression, ctx)?;
        Ok(Column { name: expr.alias.0.fragment.clone(), values: evaluated.values })
    }
}
