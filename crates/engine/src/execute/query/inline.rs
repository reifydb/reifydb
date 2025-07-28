// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::pool::BufferPoolManager;
use crate::evaluate::{EvaluationContext, evaluate};
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_catalog::table::Table;
use reifydb_core::frame::{
    ColumnQualified, ColumnValues, Frame, FrameColumn, FrameColumnLayout, FrameLayout,
};
use reifydb_core::interface::Rx;
use reifydb_core::{BitVec, ColumnDescriptor, Value};
use reifydb_rql::expression::KeyedExpression;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct InlineDataNode {
    rows: Vec<Vec<KeyedExpression>>,
    layout: Option<FrameLayout>,
    context: Arc<ExecutionContext>,
    executed: bool,
}

impl InlineDataNode {
    pub fn new(rows: Vec<Vec<KeyedExpression>>, context: Arc<ExecutionContext>) -> Self {
        let layout =
            context.table.as_ref().map(|table| Self::create_frame_layout_from_table(table));

        Self { rows, layout, context, executed: false }
    }

    fn create_frame_layout_from_table(table: &Table) -> FrameLayout {
        let columns = table
            .columns
            .iter()
            .map(|col| FrameColumnLayout { schema: None, table: None, name: col.name.clone() })
            .collect();

        FrameLayout { columns }
    }
}

impl ExecutionPlan for InlineDataNode {
    fn next(&mut self, _ctx: &ExecutionContext, _rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        if self.executed {
            return Ok(None);
        }

        self.executed = true;

        if self.rows.is_empty() {
            let frame = Frame::new_with_name(vec![], "inline");
            if self.layout.is_none() {
                self.layout = Some(FrameLayout::from_frame(&frame));
            }
            return Ok(Some(Batch { frame, mask: BitVec::new(0, true) }));
        }

        // Choose execution path based on whether we have table schema
        if self.layout.is_some() { self.next_with_table_schema() } else { self.next_infer_schema() }
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone()
    }
}

impl InlineDataNode {
    fn next_infer_schema(&mut self) -> crate::Result<Option<Batch>> {
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
            let mut column_values = ColumnValues::undefined(0);

            for row_data in &rows_data {
                if let Some(keyed_expr) = row_data.get(&column_name) {
                    let ctx = EvaluationContext {
                        target_column: None,
                        column_policies: Vec::new(),
                        mask: BitVec::new(1, true),
                        columns: Vec::new(),
                        row_count: 1,
                        take: None,
                        buffer_pool: Arc::new(BufferPoolManager::default()),
                    };

                    let evaluated = evaluate(&keyed_expr.expression, &ctx)?;

                    // Take the first value from the evaluated result
                    let mut iter = evaluated.values().iter();
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

            frame_columns.push(FrameColumn::ColumnQualified(ColumnQualified {
                name: column_name,
                values: column_values,
            }));
        }

        let frame = Frame::new_with_name(frame_columns, "inline");
        self.layout = Some(FrameLayout::from_frame(&frame));
        let mask = BitVec::new(frame.row_count(), true);

        Ok(Some(Batch { frame, mask }))
    }

    fn next_with_table_schema(&mut self) -> crate::Result<Option<Batch>> {
        let table = self.context.table.as_ref().unwrap(); // Safe because layout is Some
        let layout = self.layout.as_ref().unwrap(); // Safe because we're in this path

        // Convert rows to HashMap for easier column lookup
        let mut rows_data: Vec<HashMap<String, &KeyedExpression>> = Vec::new();

        for row in &self.rows {
            let mut row_map: HashMap<String, &KeyedExpression> = HashMap::new();
            for keyed_expr in row {
                let column_name = keyed_expr.key.0.fragment.clone();
                row_map.insert(column_name, keyed_expr);
            }
            rows_data.push(row_map);
        }

        // Create frame columns based on table schema
        let mut frame_columns = Vec::new();

        for column_layout in &layout.columns {
            let mut column_values = ColumnValues::undefined(0);

            // Find the corresponding table column for policies
            let table_column =
                table.columns.iter().find(|col| col.name == column_layout.name).unwrap(); // Safe because layout came from table

            for row_data in &rows_data {
                if let Some(keyed_expr) = row_data.get(&column_layout.name) {
                    // Create ColumnDescriptor with table context
                    let column_descriptor = ColumnDescriptor::new()
                        .with_table(&table.name)
                        .with_column(&table_column.name)
                        .with_column_type(table_column.ty)
                        .with_policies(
                            table_column.policies.iter().map(|cp| cp.policy.clone()).collect(),
                        );

                    let ctx = EvaluationContext {
                        target_column: Some(column_descriptor),
                        column_policies: table_column
                            .policies
                            .iter()
                            .map(|cp| cp.policy.clone())
                            .collect(),
                        mask: BitVec::new(1, true),
                        columns: Vec::new(),
                        row_count: 1,
                        take: None,
                        buffer_pool: Arc::new(BufferPoolManager::default()),
                    };

                    column_values
                        .extend(evaluate(&keyed_expr.expression, &ctx)?.values().clone())?;
                } else {
                    column_values.push_value(Value::Undefined);
                }
            }

            frame_columns.push(FrameColumn::ColumnQualified(ColumnQualified {
                name: column_layout.name.clone(),
                values: column_values,
            }));
        }

        let frame = Frame::new_with_name(frame_columns, "inline");
        let mask = BitVec::new(frame.row_count(), true);

        Ok(Some(Batch { frame, mask }))
    }
}
