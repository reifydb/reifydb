// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use reifydb_frame::{Column, ColumnValues, RowMask};
use reifydb_rql::expression::GreaterThanExpression;

impl Evaluator {
    pub(crate) fn greater_than(
        &mut self,
        gt: &GreaterThanExpression,
        ctx: &Context,
        columns: &[&Column],
        row_count: usize,
    ) -> crate::evaluate::Result<RowMask> {
        let left = self.evaluate(&gt.left, ctx, columns, row_count)?;
        let right = self.evaluate(&gt.right, ctx, columns, row_count)?;

        match (&left, &right) {
            (ColumnValues::Int1(l_vals, l_valid), ColumnValues::Int1(r_vals, r_valid)) => {
                let mut keep = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    let is_valid = l_valid[i] && r_valid[i];
                    let predicate = is_valid && l_vals[i] > r_vals[i];
                    keep.push(predicate);
                }
                Ok(RowMask { keep })
            }
            _ => unimplemented!(),
        }
    }
}
