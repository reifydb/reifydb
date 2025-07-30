// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::interface::Rx;
use reifydb_core::{JoinType, Value};
use std::collections::HashSet;
use crate::column::{ColumnQualified, EngineColumn, TableQualified};
use crate::column::frame::Frame;
use crate::column::layout::FrameLayout;

pub(crate) struct NaturalJoinNode {
    left: Box<dyn ExecutionPlan>,
    right: Box<dyn ExecutionPlan>,
    join_type: JoinType,
    layout: Option<FrameLayout>,
}

impl NaturalJoinNode {
    pub fn new(
        left: Box<dyn ExecutionPlan>,
        right: Box<dyn ExecutionPlan>,
        join_type: JoinType,
    ) -> Self {
        Self { left, right, join_type, layout: None }
    }

    fn load_and_merge_all(
        node: &mut Box<dyn ExecutionPlan>,
        ctx: &ExecutionContext,
        rx: &mut dyn Rx,
    ) -> crate::Result<Frame> {
        let mut result: Option<Frame> = None;

        while let Some(Batch { frame }) = node.next(ctx, rx)? {
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

    fn find_common_columns(left_frame: &Frame, right_frame: &Frame) -> Vec<(String, usize, usize)> {
        let mut common_columns = Vec::new();

        for (left_idx, left_col) in left_frame.columns.iter().enumerate() {
            for (right_idx, right_col) in right_frame.columns.iter().enumerate() {
                if left_col.name() == right_col.name() {
                    common_columns.push((left_col.name().to_string(), left_idx, right_idx));
                }
            }
        }

        common_columns
    }
}

impl ExecutionPlan for NaturalJoinNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        if self.layout.is_some() {
            return Ok(None);
        }

        let left_frame = Self::load_and_merge_all(&mut self.left, ctx, rx)?;
        let right_frame = Self::load_and_merge_all(&mut self.right, ctx, rx)?;

        let left_rows = left_frame.row_count();
        let right_rows = right_frame.row_count();

        // Find common columns between left and right frames
        let common_columns = Self::find_common_columns(&left_frame, &right_frame);

        if common_columns.is_empty() {
            // If no common columns, natural join behaves like a cross join
            // For now, return an error as this is unusual
            panic!("Natural join requires at least one common column");
        }

        // Build set of right column indices to exclude (common columns)
        let excluded_right_cols: HashSet<usize> =
            common_columns.iter().map(|(_, _, right_idx)| *right_idx).collect();

        // Build qualified column names for the result (excluding duplicates from right)
        let qualified_names: Vec<String> = left_frame
            .columns
            .iter()
            .map(|col| col.qualified_name())
            .chain(
                right_frame
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| !excluded_right_cols.contains(idx))
                    .map(|(_, col)| col.qualified_name()),
            )
            .collect();

        let mut result_rows = Vec::new();

        for i in 0..left_rows {
            let left_row = left_frame.get_row(i);
            let mut matched = false;

            for j in 0..right_rows {
                let right_row = right_frame.get_row(j);

                // Check if all common columns match
                let all_match = common_columns
                    .iter()
                    .all(|(_, left_idx, right_idx)| left_row[*left_idx] == right_row[*right_idx]);

                if all_match {
                    // Combine rows, excluding duplicate columns from right
                    let mut combined = left_row.clone();
                    for (idx, value) in right_row.iter().enumerate() {
                        if !excluded_right_cols.contains(&idx) {
                            combined.push(value.clone());
                        }
                    }
                    result_rows.push(combined);
                    matched = true;
                }
            }

            // Handle LEFT natural join - include unmatched left rows
            if !matched && matches!(self.join_type, JoinType::Left) {
                let mut combined = left_row.clone();
                // Add undefined data for non-common right columns
                let undefined_count = right_frame.column_count() - excluded_right_cols.len();
                combined.extend(vec![Value::Undefined; undefined_count]);
                result_rows.push(combined);
            }
        }

        // Create frame with proper qualified column structure
        let mut column_metadata: Vec<&EngineColumn> = left_frame.columns.iter().collect();
        for (idx, col) in right_frame.columns.iter().enumerate() {
            if !excluded_right_cols.contains(&idx) {
                column_metadata.push(col);
            }
        }

        let names_refs: Vec<&str> = qualified_names.iter().map(|s| s.as_str()).collect();
        let mut frame = Frame::from_rows(&names_refs, &result_rows);

        // Update frame columns with proper metadata
        for (i, col_meta) in column_metadata.iter().enumerate() {
            let old_column = &frame.columns[i];
            frame.columns[i] = match col_meta.table() {
                Some(table) => EngineColumn::TableQualified(TableQualified {
                    table: table.to_string(),
                    name: col_meta.name().to_string(),
                    data: old_column.data().clone(),
                }),
                None => EngineColumn::ColumnQualified(ColumnQualified {
                    name: col_meta.name().to_string(),
                    data: old_column.data().clone(),
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
            .filter_map(|(i, col)| {
                col.table().map(|sf| ((sf.to_string(), col.name().to_string()), i))
            })
            .collect();

        self.layout = Some(FrameLayout::from_frame(&frame));
        Ok(Some(Batch { frame }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone()
    }
}
