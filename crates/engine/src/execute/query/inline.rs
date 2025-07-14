// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{Context, evaluate};
use crate::execute::query::{Batch, ExecutionPlan};
use crate::frame::{Frame, FrameLayout};
use reifydb_core::BitVec;
use reifydb_rql::expression::Expression;

pub(crate) struct InlineDataNode {
    names: Vec<String>,
    columns: Vec<Vec<Expression>>,
    layout: Option<FrameLayout>,
    executed: bool,
}

impl InlineDataNode {
    pub fn new(names: Vec<String>, columns: Vec<Vec<Expression>>) -> Result<Self, String> {
        Ok(Self { names, columns, layout: None, executed: false })
    }
}

impl ExecutionPlan for InlineDataNode {
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        if self.executed {
            return Ok(None);
        }

        self.executed = true;

        if self.columns.is_empty() {
            let frame = Frame::new_with_name(vec![], "inline");
            self.layout = Some(FrameLayout::from_frame(&frame));
            return Ok(Some(Batch { frame, mask: BitVec::new(0, true) }));
        }

        // Evaluate each column
        let mut frame_columns = Vec::with_capacity(self.names.len());

        for (column_name, expressions) in self.names.iter().zip(self.columns.iter()) {
            if expressions.is_empty() {
                // Empty column
                frame_columns.push(crate::frame::Column {
                    name: column_name.clone(),
                    values: crate::frame::ColumnValues::undefined(0),
                });
                continue;
            }

            // Build column incrementally using push_value
            let mut column_values = crate::frame::ColumnValues::undefined(0);

            for expr in expressions {

                let ctx = Context {
                    column: None,
                    mask: BitVec::new(1, true),
                    columns: Vec::new(),
                    row_count: 1,
                    take: None,
                };

                let evaluated = evaluate(expr, &ctx)?;

                let mut iter = evaluated.values.iter();
                if let Some(value) = iter.next() {
                    column_values.push_value(value);
                }
            }

            frame_columns
                .push(crate::frame::Column { name: column_name.clone(), values: column_values });
        }

        let frame = Frame::new_with_name(frame_columns, "inline");
        self.layout = Some(FrameLayout::from_frame(&frame));
        let mask = BitVec::new(frame.row_count(), true);

        Ok(Some(Batch { frame, mask }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone()
    }
}
