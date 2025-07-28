// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::frame::{FrameColumn, TableQualified, ColumnQualified};
use reifydb_core::expression::AliasExpression;

impl Evaluator {
    pub(crate) fn alias(
        &mut self,
        expr: &AliasExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let evaluated = self.evaluate(&expr.expression, ctx)?;
        let alias_name = &expr.alias.0.fragment;

        let frame: Option<String> = ctx
            .target_column
            .as_ref()
            .and_then(|c| c.table.map(|c| c.to_string()))
            .or(ctx.columns.first().as_ref().and_then(|c| c.table().map(|f| f.to_string())));

        Ok(match frame {
            Some(table) => FrameColumn::TableQualified(TableQualified { table, name: alias_name.clone(), values: evaluated.values().clone() }),
            None => FrameColumn::ColumnQualified(ColumnQualified { name: alias_name.clone(), values: evaluated.values().clone() }),
        })
    }
}
