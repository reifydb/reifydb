// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{ColumnQualified, Column};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::error;
use reifydb_core::result::error::diagnostic::function;
use reifydb_rql::expression::{CallExpression, Expression};

impl Evaluator {
    pub(crate) fn call(
        &mut self,
        call: &CallExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let virtual_columns = self.evaluate_virtual_column(&call.args, ctx).unwrap();

        let function = &call.func.0.fragment;

        let functor = self
            .functions
            .get_scalar(function.as_str())
            .ok_or(error!(function::unknown_function(function.clone())))?;

        let row_count = ctx.row_count;
        Ok(Column::ColumnQualified(ColumnQualified {
            name: call.span().fragment.into(),
            data: functor.scalar(&virtual_columns, row_count).unwrap(),
        }))
    }

    fn evaluate_virtual_column<'a>(
        &mut self,
        expressions: &Vec<Expression>,
        ctx: &EvaluationContext,
    ) -> crate::Result<Vec<Column>> {
        let mut result: Vec<Column> = Vec::with_capacity(expressions.len());

        for expression in expressions {
            result.push(self.evaluate(&expression, ctx)?)
        }

        Ok(result)
    }
}
