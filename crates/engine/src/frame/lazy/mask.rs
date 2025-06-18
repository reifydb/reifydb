// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::frame::{ColumnValues, LazyFrame};
use reifydb_core::BitVec;

impl LazyFrame {
    pub(crate) fn compute_mask(&self) -> BitVec {
        let row_count = self.row_count();
        let mut mask = BitVec::new(row_count, true);
        let row_count = self.limit.unwrap_or(self.row_count());

        for filter_expr in &self.filter {
            let result = evaluate(
                filter_expr,
                &Context {
                    column: None,
                    mask: &mask,
                    columns: self.frame.columns.as_slice(),
                    row_count,
                    limit: self.limit,
                },
            )
            .unwrap();
            match result {
                ColumnValues::Bool(values, valid) => {
                    for i in 0..row_count {
                        mask.set(i, mask.get(i) && valid[i] && values[i]);
                    }
                }
                _ => panic!("filter expression must evaluate to a boolean column"),
            }
        }
        mask
    }
}
