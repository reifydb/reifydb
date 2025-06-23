// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::frame::{ColumnValues, LazyFrame};
use reifydb_core::BitVec;

impl LazyFrame {
    pub(crate) fn compute_mask(&self) -> BitVec {
        let row_count = self.row_count();

        let mut ctx = Context {
            column: None,
            mask: BitVec::new(row_count, true),
            columns: self.frame.columns.clone(),
            row_count,
            limit: None,
        };

        for filter_expr in &self.filter {
            let result = evaluate(filter_expr, &ctx).unwrap();
            match result {
                ColumnValues::Bool(values, valid) => {
                    for i in 0..row_count {
                        ctx.mask.set(i, ctx.mask.get(i) && valid[i] && values[i]);
                    }
                }
                _ => panic!("filter expression must evaluate to a boolean column"),
            }
        }
        ctx.mask
    }
}
