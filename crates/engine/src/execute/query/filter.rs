// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{Context, evaluate};
use crate::execute::query::{Batch, ExecutionPlan};
use crate::frame::{ColumnValues, FrameLayout};
use reifydb_rql::expression::Expression;

pub(crate) struct FilterNode {
    input: Box<dyn ExecutionPlan>,
    expressions: Vec<Expression>,
    layout: Option<FrameLayout>,
}

impl FilterNode {
    pub fn new(input: Box<dyn ExecutionPlan>, expressions: Vec<Expression>) -> Self {
        Self { input, expressions, layout: None }
    }
}

impl ExecutionPlan for FilterNode {
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        while let Some(Batch { frame, mut mask }) = self.input.next()? {
            let row_count = frame.row_count(); // FIXME add a delegate - batch.row_count()

            let mut ctx = Context {
                column: None,
                mask,
                columns: frame.columns.clone(),
                row_count,
                take: None,
            };

            for filter_expr in &self.expressions {
                let result = evaluate(filter_expr, &ctx)?;
                match result.values {
                    ColumnValues::Bool(values, valid) => {
                        for i in 0..row_count {
                            ctx.mask.set(i, ctx.mask.get(i) & &valid[i] & &values[i]);
                        }
                    }
                    _ => panic!("filter expression must evaluate to a boolean column"),
                }
            }

            self.layout = Some(FrameLayout::from_frame(&frame));

            mask = ctx.mask;
            if mask.any() {
                return Ok(Some(Batch { frame, mask }));
            }
        }
        Ok(None)
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone().or(self.input.layout())
    }
}
