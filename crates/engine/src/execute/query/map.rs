// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, evaluate};
use crate::execute::{Batch, ExecutionPlan};
use crate::frame::{Frame, FrameLayout};
use reifydb_core::BitVec;
use reifydb_rql::expression::Expression;

pub(crate) struct MapNode {
    input: Box<dyn ExecutionPlan>,
    expressions: Vec<Expression>,
    layout: Option<FrameLayout>,
}

impl MapNode {
    pub fn new(input: Box<dyn ExecutionPlan>, expressions: Vec<Expression>) -> Self {
        Self { input, expressions, layout: None }
    }
}

impl ExecutionPlan for MapNode {
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        while let Some(Batch { frame, mask }) = self.input.next()? {
            let row_count = frame.row_count();

            let ctx = EvaluationContext {
                column: None,
                mask: mask.clone(),
                columns: frame.columns.clone(),
                row_count,
                take: None,
            };

            let mut columns = Vec::with_capacity(self.expressions.len());

            for expr in &self.expressions {
                let column = evaluate(expr, &ctx)?;
                columns.push(crate::frame::FrameColumn { name: column.name, values: column.values });
            }

            self.layout = Some(FrameLayout::from_frame(&frame));

            return Ok(Some(Batch { frame: Frame::new(columns), mask }));
        }
        Ok(None)
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone().or(self.input.layout())
    }
}

pub(crate) struct MapWithoutInputNode {
    expressions: Vec<Expression>,
    layout: Option<FrameLayout>,
}

impl MapWithoutInputNode {
    pub fn new(expressions: Vec<Expression>) -> Self {
        Self { expressions, layout: None }
    }
}

impl ExecutionPlan for MapWithoutInputNode {
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        if self.layout.is_some() {
            return Ok(None);
        }

        let mut columns = vec![];

        for expr in self.expressions.iter() {
            let column = evaluate(
                &expr,
                &EvaluationContext {
                    column: None,
                    mask: BitVec::new(1, true),
                    columns: Vec::new(),
                    row_count: 1,
                    take: None,
                },
            )?;

            columns.push(column);
        }

        let frame = Frame::new(columns);
        self.layout = Some(FrameLayout::from_frame(&frame));
        let row_count = frame.row_count();
        Ok(Some(Batch { frame, mask: BitVec::new(row_count, true) }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone()
    }
}
