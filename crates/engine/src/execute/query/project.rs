// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::execute::query::{NextBatch, Node};
use crate::frame::{Column, Frame, FrameLayout};
use reifydb_core::BitVec;
use reifydb_rql::expression::AliasExpression;

pub(crate) struct ProjectNode {
    input: Box<dyn Node>,
    expressions: Vec<AliasExpression>,
    layout: Option<FrameLayout>,
}

impl ProjectNode {
    pub fn new(input: Box<dyn Node>, expressions: Vec<AliasExpression>) -> Self {
        Self { input, expressions, layout: None }
    }
}

impl Node for ProjectNode {
    fn next_batch(&mut self) -> crate::Result<NextBatch> {
        match self.input.next_batch()? {
            NextBatch::Some { frame, mask } => {
                // let mut batch = self.input.next_batch()?;
                let row_count = frame.row_count();

                let ctx = Context {
                    column: None,
                    mask: mask.clone(),
                    columns: frame.columns.clone(),
                    // row_count: mask.count_ones(),
                    row_count,
                    limit: None,
                };

                let columns = self
                    .expressions
                    .iter()
                    .map(|alias_expr| {
                        let expr = &alias_expr.expression;
                        let alias = alias_expr
                            .alias
                            .clone()
                            .map(|a| a.0.fragment)
                            .unwrap_or(expr.span().fragment);

                        let values = evaluate(expr, &ctx).unwrap();

                        crate::frame::Column { name: alias.clone(), data: values }
                    })
                    .collect();

                self.layout = Some(FrameLayout::from_frame(&frame));

                Ok(NextBatch::Some { frame: Frame::new(columns), mask })
            }
            NextBatch::None { layout } => {
                Ok(NextBatch::None { layout: self.layout.clone().unwrap_or(layout) })
            }
        }
    }
}

pub(crate) struct ProjectWithoutInputNode {
    expressions: Vec<AliasExpression>,
    layout: Option<FrameLayout>,
}

impl ProjectWithoutInputNode {
    pub fn new(expressions: Vec<AliasExpression>) -> Self {
        Self { expressions, layout: None }
    }
}

impl Node for ProjectWithoutInputNode {
    fn next_batch(&mut self) -> crate::Result<NextBatch> {
        if let Some(layout) = &self.layout {
            return Ok(NextBatch::None { layout: layout.clone() });
        }

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
        self.layout = Some(FrameLayout::from_frame(&frame));
        let row_count = frame.row_count();
        Ok(NextBatch::Some { frame, mask: BitVec::new(row_count, true) })
    }
}
