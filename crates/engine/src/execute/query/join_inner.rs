// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, evaluate};
use crate::evaluate::pool::BufferPoolManager;
use std::sync::Arc;
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::frame::{ColumnValues, Frame, FrameColumn, FrameLayout, TableQualified, ColumnQualified};
use reifydb_core::interface::Rx;
use reifydb_core::{BitVec, Value};
use reifydb_core::expression::Expression;

pub(crate) struct InnerJoinNode {
    left: Box<dyn ExecutionPlan>,
    right: Box<dyn ExecutionPlan>,
    on: Vec<Expression>,
    layout: Option<FrameLayout>,
}

impl InnerJoinNode {
    pub fn new(
        left: Box<dyn ExecutionPlan>,
        right: Box<dyn ExecutionPlan>,
        on: Vec<Expression>,
    ) -> Self {
        Self { left, right, on, layout: None }
    }

    fn load_and_merge_all(
        node: &mut Box<dyn ExecutionPlan>,
        ctx: &ExecutionContext,
        rx: &mut dyn Rx,
    ) -> crate::Result<Frame> {
        let mut result: Option<Frame> = None;

        while let Some(Batch { frame, mask: _ }) = node.next(ctx, rx)? {
            if let Some(mut acc) = result.take() {
                acc.append_frame(frame)?;
                result = Some(acc);
            } else {
                result = Some(frame);
            }
        }
        let result = result.unwrap_or_else(Frame::empty);
        Ok(result)
    }
}

impl ExecutionPlan for InnerJoinNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        if self.layout.is_some() {
            return Ok(None);
        }

        let left_frame = Self::load_and_merge_all(&mut self.left, ctx, rx)?;
        let right_frame = Self::load_and_merge_all(&mut self.right, ctx, rx)?;

        let left_rows = left_frame.row_count();
        let right_rows = right_frame.row_count();

        // Build qualified column names for the join result
        let qualified_names: Vec<String> = left_frame
            .columns
            .iter()
            .chain(&right_frame.columns)
            .map(|col| col.qualified_name())
            .collect();

        let mut result_rows = Vec::new();
        let mut mask = BitVec::new(0, true);

        for i in 0..left_rows {
            let left_row = left_frame.get_row(i);

            for j in 0..right_rows {
                let right_row = right_frame.get_row(j);

                let all_values =
                    left_row.iter().cloned().chain(right_row.iter().cloned()).collect::<Vec<_>>();

                let ctx = EvaluationContext {
                    target_column: None,
                    column_policies: Vec::new(),
                    mask: BitVec::new(1, true),
                    columns: all_values
                        .iter()
                        .cloned()
                        .zip(left_frame.columns.iter().chain(&right_frame.columns))
                        .map(|(v, col)| match col.table() {
                            Some(table) => FrameColumn::TableQualified(TableQualified {
                                table: table.to_string(),
                                name: col.name().to_string(),
                                values: ColumnValues::from(v),
                            }),
                            None => FrameColumn::ColumnQualified(ColumnQualified {
                                name: col.name().to_string(),
                                values: ColumnValues::from(v),
                            }),
                        })
                        .collect(),
                    row_count: 1,
                    take: Some(1),
                    buffer_pool: Arc::new(BufferPoolManager::default()),
                };

                let all_true = self.on.iter().fold(true, |acc, cond| {
                    let col = evaluate(cond, &ctx).unwrap();
                    matches!(col.values().get(0), Value::Bool(true)) && acc
                });

                if all_true {
                    let mut combined = left_row.clone();
                    combined.extend(right_row.clone());
                    result_rows.push(combined);
                    mask.push(true);
                }
            }
        }

        // Create frame with proper qualified column structure
        let column_metadata: Vec<_> =
            left_frame.columns.iter().chain(&right_frame.columns).collect();
        let names_refs: Vec<&str> = qualified_names.iter().map(|s| s.as_str()).collect();
        let mut frame = Frame::from_rows(&names_refs, &result_rows);

        // Update frame columns with proper metadata
        for (i, col_meta) in column_metadata.iter().enumerate() {
            let old_column = &frame.columns[i];
            frame.columns[i] = match col_meta.table() {
                Some(table) => FrameColumn::TableQualified(TableQualified {
                    table: table.to_string(),
                    name: col_meta.name().to_string(),
                    values: old_column.values().clone(),
                }),
                None => FrameColumn::ColumnQualified(ColumnQualified {
                    name: col_meta.name().to_string(),
                    values: old_column.values().clone(),
                }),
            };
        }

        // Rebuild indexes with updated column info
        frame.index =
            frame.columns.iter().enumerate().map(|(i, col)| (col.qualified_name(), i)).collect();

        frame.frame_index = frame
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, col)| col.table().map(|sf| ((sf.to_string(), col.name().to_string()), i)))
            .collect();

        self.layout = Some(FrameLayout::from_frame(&frame));
        Ok(Some(Batch { frame, mask }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone()
    }
}