// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, evaluate};
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
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
        while let Some(Batch { frame, mut mask }) = self.input.next(ctx, rx)? {
            let row_count = frame.row_count();

            // Apply filters lazily - stop early if mask becomes empty
            for filter_expr in &self.expressions {
                // Early exit if no rows remain
                if !mask.any() {
                    break;
                }

                // Create evaluation context with current mask state
                let eval_ctx = EvaluationContext {
                    target_column: None,
                    column_policies: Vec::new(),
                    mask: mask.clone(),
                    columns: frame.columns.clone(),
                    row_count,
                    take: None,
                    buffered: ctx.buffered.clone(),
                };

                // Evaluate the filter expression
                let result = evaluate(filter_expr, &eval_ctx)?;

                // Apply the filter result to the mask
                match result.values() {
                    ColumnValues::Bool(values, bitvec) => {
                        // The result only contains values for rows where mask was true
                        // We need to map these back to the original row indices
                        let mut result_idx = 0;
                        for i in 0..row_count {
                            if i < mask.len() && mask.get(i) {
                                // This row was visible to the filter evaluation
                                if result_idx < values.len() && result_idx < bitvec.len() {
                                    let valid = bitvec.get(result_idx);
                                    let filter_result = values.get(result_idx);
                                    mask.set(i, valid & filter_result);
                                } else {
                                    // Safety: if result is shorter than expected, exclude this row
                                    mask.set(i, false);
                                }
                                result_idx += 1;
                            }
                            // If mask.get(i) was false, this row stays false (no change needed)
                        }
                    }
                    _ => panic!("filter expression must evaluate to a boolean column"),
                }
            }

            // Only return batch if any rows remain after filtering
            if mask.any() {
                return Ok(Some(Batch { frame, mask }));
            }
        }
        Ok(None)
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.input.layout()
    }
}
