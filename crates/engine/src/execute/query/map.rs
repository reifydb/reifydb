// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, evaluate};
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use crate::frame::{Frame, FrameLayout};
use reifydb_core::BitVec;
use reifydb_core::interface::Rx;
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
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
    fn next(
        &mut self,
        exec_ctx: &ExecutionContext,
        rx: &mut dyn Rx,
    ) -> crate::Result<Option<Batch>> {
        while let Some(Batch { frame, mask }) = self.input.next(exec_ctx, rx)? {
            let row_count = frame.row_count();

            let eval_ctx = EvaluationContext {
                target_column: None,
                column_policies: Vec::new(),
                mask: mask.clone(),
                columns: frame.columns.clone(),
                row_count,
                take: None,
            };

            let mut columns = Vec::with_capacity(self.expressions.len());

            // Only preserve RowId column if the execution context requires it
            if exec_ctx.preserve_row_ids {
                if let Some(row_id_column) =
                    frame.columns.iter().find(|col| col.name == ROW_ID_COLUMN_NAME)
                {
                    let mut filtered_row_id_column = row_id_column.clone();
                    filtered_row_id_column.filter(&mask)?;
                    columns.push(filtered_row_id_column);
                }
            }

            for expr in &self.expressions {
                let column = evaluate(expr, &eval_ctx)?;
                columns
                    .push(crate::frame::FrameColumn { name: column.name, values: column.values });
            }

            self.layout = Some(FrameLayout::from_frame(&frame));

            let new_frame = Frame::new(columns);
            let new_mask = BitVec::new(new_frame.row_count(), true);
            return Ok(Some(Batch { frame: new_frame, mask: new_mask }));
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
    fn next(&mut self, _ctx: &ExecutionContext, _rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        if self.layout.is_some() {
            return Ok(None);
        }

        let mut columns = vec![];

        for expr in self.expressions.iter() {
            let column = evaluate(
                &expr,
                &EvaluationContext {
                    target_column: None,
                    column_policies: Vec::new(),
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
