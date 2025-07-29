// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, evaluate};
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::BitVec;
use reifydb_core::expression::Expression;
use reifydb_core::frame::{ColumnValues, FrameLayout};
use reifydb_core::interface::Rx;

pub(crate) struct FilterNode {
    input: Box<dyn ExecutionPlan>,
    expressions: Vec<Expression>,
}

impl FilterNode {
    pub fn new(input: Box<dyn ExecutionPlan>, expressions: Vec<Expression>) -> Self {
        Self { input, expressions }
    }
}

impl ExecutionPlan for FilterNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        while let Some(Batch { mut frame }) = self.input.next(ctx, rx)? {
            let mut row_count = frame.row_count();

            // Apply each filter expression sequentially
            for filter_expr in &self.expressions {
                // Early exit if no rows remain
                if row_count == 0 {
                    break;
                }

                // Create evaluation context for all current rows
                let eval_ctx = EvaluationContext {
                    target_column: None,
                    column_policies: Vec::new(),
                    columns: frame.columns.clone(),
                    row_count,
                    take: None,
                    buffered: ctx.buffered.clone(),
                };

                // Evaluate the filter expression
                let result = evaluate(filter_expr, &eval_ctx)?;

                // Create filter mask from result
                let filter_mask = match result.values() {
                    ColumnValues::Bool(container) => {
                        let mut mask = BitVec::repeat(row_count, false);
                        for i in 0..row_count {
                            if i < container.values().len() && i < container.bitvec().len() {
                                let valid = container.is_defined(i);
                                let filter_result = container.values().get(i);
                                mask.set(i, valid & filter_result);
                            }
                        }
                        mask
                    }
                    _ => panic!("filter expression must evaluate to a boolean column"),
                };

                frame.filter(&filter_mask)?;
                row_count = frame.row_count();
            }

            if row_count > 0 {
                return Ok(Some(Batch { frame }));
            }
        }
        Ok(None)
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.input.layout()
    }
}
