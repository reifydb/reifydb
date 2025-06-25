// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::execute::query::{Batch, Node};
use crate::frame::{ColumnValues, Frame};
use reifydb_rql::expression::Expression;

pub(crate) struct FilterFunctionNode<F: Fn(&Frame, usize) -> bool> {
    input: Box<dyn Node>,
    predicate: F,
}

impl<F: Fn(&Frame, usize) -> bool> FilterFunctionNode<F> {
    pub fn new(input: Box<dyn Node>, predicate: F) -> Self {
        Self { input, predicate }
    }
}

impl<F: Fn(&Frame, usize) -> bool> Node for FilterFunctionNode<F> {
    fn next_batch(&mut self) -> Option<Batch> {
        while let Some(mut batch) = self.input.next_batch() {
            for i in 0..batch.frame.row_count() {
                if batch.mask.get(i) {
                    if !(self.predicate)(&batch.frame, i) {
                        batch.mask.set(i, false);
                    }
                }
            }
            if batch.mask.any() {
                return Some(batch);
            }
        }
        None
    }
}

pub(crate) struct FilterNode {
    input: Box<dyn Node>,
    expressions: Vec<Expression>,
}

impl FilterNode {
    pub fn new(input: Box<dyn Node>, expressions: Vec<Expression>) -> Self {
        Self { input, expressions }
    }
}

impl Node for FilterNode {
    fn next_batch(&mut self) -> Option<Batch> {
        while let Some(mut batch) = self.input.next_batch() {
    
            let row_count = batch.frame.row_count(); // FIXME add a delegate - batch.row_count()

            let mut ctx = Context {
                column: None,
                mask: batch.mask,
                columns: batch.frame.columns.clone(),
                row_count,
                limit: None,
            };

            for filter_expr in &self.expressions {
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

            batch.mask = ctx.mask;
            if batch.mask.any() {
                // batch.frame.filter() ?
                return Some(batch);
            }
            
        }
        None
    }
}
