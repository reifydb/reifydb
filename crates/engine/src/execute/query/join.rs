// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, evaluate};
use crate::execute::{Batch, ExecutionPlan};
use crate::frame::{ColumnValues, Frame, FrameColumn, FrameLayout};
use reifydb_core::interface::Rx;
use reifydb_core::{BitVec, Value};
use reifydb_rql::expression::Expression;

pub(crate) struct LeftJoinNode {
    left: Box<dyn ExecutionPlan>,
    right: Box<dyn ExecutionPlan>,
    on: Vec<Expression>,
    layout: Option<FrameLayout>,
}

impl LeftJoinNode {
    pub fn new(
        left: Box<dyn ExecutionPlan>,
        right: Box<dyn ExecutionPlan>,
        on: Vec<Expression>,
    ) -> Self {
        Self { left, right, on, layout: None }
    }

    fn load_and_merge_all(
        node: &mut Box<dyn ExecutionPlan>,
        rx: &mut dyn Rx,
    ) -> crate::Result<Frame> {
        let mut result: Option<Frame> = None;

        while let Some(Batch { frame, mask: _ }) = node.next(rx)? {
            if let Some(mut acc) = result.take() {
                acc.append_frame(frame)?;
                result = Some(acc);
            } else {
                result = Some(frame);
            }
        }
        let mut result = result.unwrap_or_else(Frame::empty);
        result.qualify_columns();
        Ok(result)
    }
}

impl ExecutionPlan for LeftJoinNode {
    fn next(&mut self, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        if self.layout.is_some() {
            return Ok(None);
        }

        let left_frame = Self::load_and_merge_all(&mut self.left, rx)?;
        let right_frame = Self::load_and_merge_all(&mut self.right, rx)?;

        let left_rows = left_frame.row_count();
        let right_rows = right_frame.row_count();
        let right_width = right_frame.column_count();

        let names: Vec<&str> = left_frame
            .columns
            .iter()
            .chain(&right_frame.columns)
            .map(|col| col.name.as_str())
            .collect();

        let mut result_rows = Vec::new();
        let mut mask = BitVec::new(0, true);

        for i in 0..left_rows {
            let left_row = left_frame.get_row(i);

            let mut matched = false;
            for j in 0..right_rows {
                let right_row = right_frame.get_row(j);

                let all_values =
                    left_row.iter().cloned().chain(right_row.iter().cloned()).collect::<Vec<_>>();

                let ctx = EvaluationContext {
                    column: None,
                    mask: BitVec::new(1, true),
                    columns: all_values
                        .iter()
                        .cloned()
                        .zip(names.iter().cloned())
                        .map(|(v, name)| FrameColumn {
                            name: name.to_string(),
                            values: ColumnValues::from(v),
                        })
                        .collect(),
                    row_count: 1,
                    take: Some(1),
                };

                let all_true = self.on.iter().fold(true, |acc, cond| {
                    let col = evaluate(cond, &ctx).unwrap();
                    matches!(col.values.get(0), Value::Bool(true)) && acc
                });

                if all_true {
                    let mut combined = left_row.clone();
                    combined.extend(right_row.clone());
                    result_rows.push(combined);
                    mask.push(true);
                    matched = true;
                }
            }

            if !matched {
                let mut combined = left_row.clone();
                combined.extend(vec![Value::Undefined; right_width]);
                result_rows.push(combined);
                mask.push(true);
            }
        }

        let frame = Frame::from_rows(&names, &result_rows);
        self.layout = Some(FrameLayout::from_frame(&frame));
        Ok(Some(Batch { frame, mask }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone()
    }
}
