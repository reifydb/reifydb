// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::FrameColumn;
use crate::function::unknown_function;
use reifydb_rql::expression::{CallExpression, Expression};

impl Evaluator {
    pub(crate) fn call(
		&mut self,
		call: &CallExpression,
		ctx: &EvaluationContext,
    ) -> evaluate::Result<FrameColumn> {
        let virtual_columns = self.evaluate_virtual_column(&call.args, ctx).unwrap();

        let function = &call.func.0.fragment;

        let functor = self
            .functions
            .get_scalar(function.as_str())
            .ok_or(unknown_function(function.clone()))
            .unwrap();

        let row_count = ctx.row_count;
        Ok(FrameColumn {
            name: call.span().fragment,
            values: functor.scalar(&virtual_columns, row_count).unwrap(),
        })
    }

    fn evaluate_virtual_column<'a>(
		&mut self,
		expressions: &Vec<Expression>,
		ctx: &EvaluationContext,
    ) -> crate::Result<Vec<FrameColumn>> {
        let mut result: Vec<FrameColumn> = Vec::with_capacity(expressions.len());

        for expression in expressions {
            result.push(self.evaluate(&expression, ctx)?)
        }

        Ok(result)
    }
}
