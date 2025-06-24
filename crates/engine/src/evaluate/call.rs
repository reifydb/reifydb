// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Evaluator};
use crate::frame::{Column, ColumnValues};
use reifydb_rql::expression::{CallExpression, Expression};

impl Evaluator {
    pub(crate) fn call(
        &mut self,
        call: &CallExpression,
        ctx: &Context,
    ) -> evaluate::Result<ColumnValues> {
        let virtual_columns = self.evaluate_virtual_column(&call.args, ctx).unwrap();

        let functor = self.functions.get(call.func.0.fragment.as_str()).unwrap();
        let exec = functor.prepare().unwrap();

        let row_count = ctx.row_count;
        Ok(exec.eval_scalar(&virtual_columns, row_count).unwrap())
    }

    fn evaluate_virtual_column<'a>(
        &mut self,
        expressions: &Vec<Expression>,
        ctx: &Context,
    ) -> crate::Result<Vec<Column>> {
        let mut result: Vec<Column> = Vec::with_capacity(expressions.len());

        for expression in expressions {
            result.push(Column {
                name: expression.to_string(),
                data: self.evaluate(&expression, ctx)?,
            })
        }

        Ok(result)
    }
}
