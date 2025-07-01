// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::execute::query::{Batch, Node};
use crate::frame::{Column, ColumnValues, Frame, FrameLayout};
use reifydb_core::{BitVec, Value};
use reifydb_rql::expression::Expression;

pub(crate) struct LeftJoinNode {
    left: Box<dyn Node>,
    right: Box<dyn Node>,
    conditions: Vec<Expression>,
    layout: Option<FrameLayout>,
}

impl LeftJoinNode {
    pub fn new(left: Box<dyn Node>, right: Box<dyn Node>, conditions: Vec<Expression>) -> Self {
        Self { left, right, conditions, layout: None }
    }

    fn load_and_merge_all(node: &mut Box<dyn Node>) -> crate::Result<Frame> {
        let mut result: Option<Frame> = None;

        while let Some(Batch { mut frame, mask }) = node.next()? {
            frame.filter(&mask)?;
            if let Some(mut acc) = result.take() {
                acc.append_frame(frame)?;
                result = Some(acc);
            } else {
                result = Some(frame);
            }
        }

        Ok(result.unwrap_or_else(Frame::empty))
    }
}

impl Node for LeftJoinNode {
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        if self.layout.is_some() {
            return Ok(None);
        }

        let mut left_frame = Self::load_and_merge_all(&mut self.left)?;
        let mut right_frame = Self::load_and_merge_all(&mut self.right)?;

        let left_rows = left_frame.row_count();
        let right_rows = right_frame.row_count();
        let right_width = right_frame.column_count();

        let mut result_rows = Vec::new();
        let mut mask = BitVec::new(0, true);

        left_frame.columns.get_mut(0).unwrap().name = "one_field".to_string();
        right_frame.columns.get_mut(0).unwrap().name = "two_field".to_string();

        let names = ["one_field", "two_field"];

        for i in 0..left_rows {
            let left_row = left_frame.get_row(i);

            let mut matched = false;
            for j in 0..right_rows {
                let right_row = right_frame.get_row(j);

                let all_values =
                    left_row.iter().cloned().chain(right_row.iter().cloned()).collect::<Vec<_>>();

                let ctx = Context {
                    column: None,
                    mask: BitVec::new(1, true),
                    columns: all_values
                        .into_iter()
                        .enumerate()
                        .map(|(i, v)| Column {
                            name: names[i].to_string(),
                            data: ColumnValues::from(v),
                        })
                        .collect(),
                    row_count: 1,
                    limit: Some(1),
                };

                let all_true = self.conditions.iter().fold(true, |acc, cond| {
                    let col = evaluate(cond, &ctx).unwrap();
                    match col.data.get(0) {
                        Value::Bool(true) => acc,
                        Value::Bool(false) => false,
                        _ => false,
                    }
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
