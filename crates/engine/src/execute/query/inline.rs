// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{Context, evaluate};
use crate::execute::{Batch, ExecutionPlan};
use crate::frame::{Frame, FrameLayout};
use reifydb_core::{BitVec, Value};
use reifydb_rql::expression::KeyedExpression;
use std::collections::HashMap;

pub(crate) struct InlineDataNode {
    rows: Vec<Vec<KeyedExpression>>,
    layout: Option<FrameLayout>,
    executed: bool,
}

impl InlineDataNode {
    pub fn new(rows: Vec<Vec<KeyedExpression>>) -> Self {
        Self { rows, layout: None, executed: false }
    }
}

impl ExecutionPlan for InlineDataNode {
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        if self.executed {
            return Ok(None);
        }

        self.executed = true;

        if self.rows.is_empty() {
            let frame = Frame::new_with_name(vec![], "inline");
            self.layout = Some(FrameLayout::from_frame(&frame));
            return Ok(Some(Batch { frame, mask: BitVec::new(0, true) }));
        }

        // Collect all unique column names across all rows
        let mut all_columns: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        
        for row in &self.rows {
            for keyed_expr in row {
                let column_name = keyed_expr.key.0.fragment.clone();
                all_columns.insert(column_name);
            }
        }
        
        // Convert each row to a HashMap for easier lookup
        let mut rows_data: Vec<HashMap<String, &KeyedExpression>> = Vec::new();
        
        for row in &self.rows {
            let mut row_map: HashMap<String, &KeyedExpression> = HashMap::new();
            for keyed_expr in row {
                let column_name = keyed_expr.key.0.fragment.clone();
                row_map.insert(column_name, keyed_expr);
            }
            rows_data.push(row_map);
        }
        
        // Create frame columns with equal length
        let mut frame_columns = Vec::new();
        
        for column_name in all_columns {
            let mut column_values = crate::frame::ColumnValues::undefined(0);
            
            for row_data in &rows_data {
                if let Some(keyed_expr) = row_data.get(&column_name) {
                    let ctx = Context {
                        column: None,
                        mask: BitVec::new(1, true),
                        columns: Vec::new(),
                        row_count: 1,
                        take: None,
                    };

                    let evaluated = evaluate(&keyed_expr.expression, &ctx)?;

                    // Take the first value from the evaluated result
                    let mut iter = evaluated.values.iter();
                    if let Some(value) = iter.next() {
                        column_values.push_value(value);
                    } else {
                        column_values.push_value(Value::Undefined);
                    }
                } else {
                    // Missing column for this row, use Undefined
                    column_values.push_value(Value::Undefined);
                }
            }

            frame_columns.push(crate::frame::Column { name: column_name, values: column_values });
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
