// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, evaluate};
use crate::evaluate::pool::BufferPoolManager;
use std::sync::Arc;
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::frame::{Frame, FrameColumn, FrameLayout};
use reifydb_core::interface::Rx;
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
use reifydb_core::{BitVec, ColumnDescriptor};
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

    /// Creates an EvaluationContext for a specific expression, injecting target column information
    /// when the expression is an alias expression that targets a table column during UPDATE/INSERT operations.
    fn create_evaluation_context<'a>(
        &self,
        expr: &Expression,
        ctx: &'a ExecutionContext,
        mask: BitVec,
        columns: Vec<FrameColumn>,
        row_count: usize,
    ) -> EvaluationContext<'a> {
        let mut result = EvaluationContext {
            target_column: None,
            column_policies: Vec::new(),
            mask,
            columns,
            row_count,
            take: None,
            buffer_pool: Arc::new(BufferPoolManager::default()),
        };

        // Check if this is an alias expression and we have table information
        if let (Expression::Alias(alias_expr), Some(table)) = (expr, &ctx.table) {
            let alias_name = alias_expr.alias.name();

            // Find the matching column in the table schema
            if let Some(table_column) = table.columns.iter().find(|col| col.name == alias_name) {
                // Extract ColumnPolicyKind from ColumnPolicy
                let policy_kinds: Vec<_> =
                    table_column.policies.iter().map(|policy| policy.policy.clone()).collect();

                let target_column = ColumnDescriptor::new()
                    .with_table(&table.name)
                    .with_column(&table_column.name)
                    .with_column_type(table_column.ty)
                    .with_policies(policy_kinds.clone());

                result.target_column = Some(target_column);
                result.column_policies = policy_kinds;
            }
        }

        result
    }
}

impl ExecutionPlan for MapNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        while let Some(Batch { frame, mask }) = self.input.next(ctx, rx)? {
            let mut columns = Vec::with_capacity(self.expressions.len());

            // Only preserve RowId column if the execution context requires it
            if ctx.preserve_row_ids {
                if let Some(row_id_column) =
                    frame.columns.iter().find(|col| col.name() == ROW_ID_COLUMN_NAME)
                {
                    let mut filtered_row_id_column = row_id_column.clone();
                    filtered_row_id_column.filter(&mask)?;
                    columns.push(filtered_row_id_column);
                }
            }

            let filtered_row_count = mask.count_ones();

            for expr in &self.expressions {
                let column = evaluate(
                    expr,
                    &self.create_evaluation_context(
                        expr,
                        ctx,
                        mask.clone(),
                        frame.columns.clone(),
                        filtered_row_count,
                    ),
                )?;

                columns.push(column);
            }

            let layout =
                Expression::derive_frame_column_layout(&self.expressions, ctx.preserve_row_ids);

            self.layout = Some(layout);

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
                    buffer_pool: Arc::new(BufferPoolManager::default()),
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
