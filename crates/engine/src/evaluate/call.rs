// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::columnar::{Column, ColumnQualified};
use crate::evaluate::{EvaluationContext, Evaluator};
use crate::function::ScalarFunctionContext;
use reifydb_core::error;
use reifydb_core::result::error::diagnostic::function;
use reifydb_rql::expression::{CallExpression, Expression};

impl Evaluator {
    pub(crate) fn call(
        &mut self,
        call: &CallExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let arguments = self.evaluate_arguments(&call.args, ctx)?;
        let function = &call.func.0.fragment;

        let functor = self
            .functions
            .get_scalar(function.as_str())
            .ok_or(error!(function::unknown_function(function.clone())))?;

        let row_count = ctx.row_count;
        Ok(Column::ColumnQualified(ColumnQualified {
            name: call.span().fragment.into(),
            data: functor.scalar(ScalarFunctionContext { columns: &arguments, row_count })?,
        }))
    }

    fn evaluate_arguments<'a>(
        &mut self,
        expressions: &Vec<Expression>,
        ctx: &EvaluationContext,
    ) -> crate::Result<Columns> {
        let mut result: Vec<Column> = Vec::with_capacity(expressions.len());

        for expression in expressions {
            result.push(self.evaluate(&expression, ctx)?)
        }

        Ok(Columns::new(result))
    }
}
