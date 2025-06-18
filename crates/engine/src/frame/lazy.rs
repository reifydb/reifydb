// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::explore_lazy::{Expression, evaluate_expression};
use crate::frame::{Column, ColumnValues};
use reifydb_core::BitVec;

pub struct LazyFrame {
    pub columns: Vec<Column>,
    pub expressions: Vec<(String, Expression)>,
    pub filter: Vec<Expression>,
}

impl LazyFrame {
    pub fn evaluate(&self) -> Vec<(String, ColumnValues)> {
        let mask = self.compute_mask();
        self.expressions
            .iter()
            .map(|(alias, expr)| {
                let values = evaluate_expression(expr, &self.columns, &mask);
                (alias.clone(), values)
            })
            .collect()
    }

    fn compute_mask(&self) -> BitVec {
        let row_count = self.row_count();
        let mut mask = BitVec::new(row_count, true);

        for filter_expr in &self.filter {
            let result = evaluate_expression(filter_expr, &self.columns, &mask);
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

    fn row_count(&self) -> usize {
        self.columns.first().map(|col| col.data.len()).unwrap_or(0)
    }
}
