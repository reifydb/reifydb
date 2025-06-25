// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::execute::query::{Batch, Node};
use crate::frame::{Column, Frame};
use reifydb_core::BitVec;
use reifydb_rql::expression::AliasExpression;

pub(crate) struct ProjectNode {
    input: Box<dyn Node>,
    expressions: Vec<AliasExpression>,
}

impl ProjectNode {
    pub fn new(input: Box<dyn Node>, expressions: Vec<AliasExpression>) -> Self {
        Self { input, expressions }
    }
}

impl Node for ProjectNode {
    fn next_batch(&mut self) -> Option<Batch> {
        let mut batch = self.input.next_batch()?;
        let row_count = batch.frame.row_count();

        let ctx = Context {
            column: None,
            mask: batch.mask.clone(),
            columns: batch.frame.columns.clone(),
            row_count: batch.mask.count_ones(),
            limit: None,
        };

        let columns = self
            .expressions
            .iter()
            .map(|alias_expr| {
                let expr = &alias_expr.expression;
                let alias =
                    alias_expr.alias.clone().map(|a| a.0.fragment).unwrap_or(expr.span().fragment);

                let values = evaluate(expr, &ctx).unwrap();

                crate::frame::Column { name: alias.clone(), data: values }
            })
            .collect();

        batch.frame = Frame::new(columns);

        Some(batch)
    }
}

pub(crate) struct ProjectWithoutInputNode {
    expressions: Vec<AliasExpression>,
    called: bool,
}

impl ProjectWithoutInputNode {
    pub fn new(expressions: Vec<AliasExpression>) -> Self {
        Self { expressions, called: false }
    }
}

impl Node for ProjectWithoutInputNode {
    fn next_batch(&mut self) -> Option<Batch> {
        if self.called {
            return None
        }
        
        self.called = true;
    
        let mut columns = vec![];

        for (idx, expr) in self.expressions.iter().enumerate() {
            let expr = &expr.expression;

            let value = evaluate(
                &expr,
                &Context {
                    column: None,
                    mask: BitVec::new(1, true),
                    columns: Vec::new(),
                    row_count: 1,
                    limit: None,
                },
            )
            .unwrap();
            columns.push(Column { name: format!("{}", idx + 1), data: value });
        }

        let frame = Frame::new(columns);
        let row_count = frame.row_count();
        Some(Batch { frame, mask: BitVec::new(row_count, true) })
    }
}
