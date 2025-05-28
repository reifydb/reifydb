// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod catalog;
mod display;
mod query;
mod write;

use crate::function::{FunctionRegistry, math};
use base::{Row, Value, ValueKind};
use frame::{ColumnValues, Frame};
use rql::plan::{Plan, QueryPlan};
use transaction::{Rx, Tx};

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub value: ValueKind,
}

#[derive(Debug)]
pub enum ExecutionResult {
    CreateSchema { schema: String },
    CreateSeries { schema: String, series: String },
    CreateTable { schema: String, table: String },
    InsertIntoTable { schema: String, table: String, inserted: usize },
    InsertIntoSeries { schema: String, series: String, inserted: usize },
    Query { columns: Vec<Column>, rows: Vec<Row> },
}

impl From<Frame> for ExecutionResult {
    fn from(value: Frame) -> Self {
        let columns: Vec<Column> = value
            .columns
            .iter()
            .map(|c| {
                let value = match &c.data {
                    ColumnValues::Float8(_, _) => ValueKind::Float8,
                    ColumnValues::Int2(_, _) => ValueKind::Int2,
                    ColumnValues::Text(_, _) => ValueKind::Text,
                    ColumnValues::Bool(_, _) => ValueKind::Bool,
                    ColumnValues::Undefined(_) => ValueKind::Undefined,
                };

                Column { name: c.name.clone(), value }
            })
            .collect();

        let row_count = value.columns.first().map_or(0, |col| col.data.len());
        let mut rows = Vec::with_capacity(row_count);

        for row_idx in 0..row_count {
            let mut row = Vec::with_capacity(value.columns.len());

            for col in &value.columns {
                let value = match &col.data {
                    ColumnValues::Float8(vals, valid) => {
                        if valid[row_idx] {
                            Value::float8(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }

                    ColumnValues::Int2(vals, valid) => {
                        if valid[row_idx] {
                            Value::Int2(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Text(vals, valid) => {
                        if valid[row_idx] {
                            Value::Text(vals[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Bool(vals, valid) => {
                        if valid[row_idx] {
                            Value::Bool(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Undefined(_) => Value::Undefined,
                };
                row.push(value);
            }

            rows.push(row);
        }

        ExecutionResult::Query { columns, rows }
    }
}

pub(crate) struct Executor {
    functions: FunctionRegistry,
    frame: Frame,
}

pub fn execute(plan: QueryPlan, rx: &impl Rx) -> crate::Result<ExecutionResult> {
    let mut executor = Executor {
        functions: FunctionRegistry::new(), // FIXME receive functions from RX
        frame: Frame::new(vec![]),
    };

    executor.functions.register(math::AbsFunction {});
    executor.functions.register(math::AvgFunction {});

    executor.execute(plan, rx)
}

pub fn execute_mut(plan: Plan, tx: &mut impl Tx) -> crate::Result<ExecutionResult> {
    let mut executor = Executor {
        functions: FunctionRegistry::new(), // FIXME receive functions from TX
        frame: Frame::new(vec![]),
    };

    executor.functions.register(math::AbsFunction {});
    executor.functions.register(math::AvgFunction {});

    executor.execute_mut(plan, tx)
}

impl Executor {
    pub(crate) fn execute(
        mut self,
        plan: QueryPlan,
        rx: &impl Rx,
    ) -> crate::Result<ExecutionResult> {
        let next = match plan {
            QueryPlan::Aggregate { group_by, project, next } => {
                self.aggregate(&group_by, &project)?;
                next
            }
            QueryPlan::Scan { schema, store, next } => {
                self.scan(rx, &schema, &store)?;
                next
            }
            QueryPlan::Project { expressions, next } => {
                self.project(expressions)?;
                next
            }
            QueryPlan::Sort { keys, next } => {
                self.sort(&keys)?;
                next
            }
            QueryPlan::Limit { limit, next } => {
                self.limit(limit)?;
                next
            }
        };

        if let Some(next) = next { self.execute(*next, rx) } else { Ok(self.frame.into()) }
    }

    pub(crate) fn execute_mut(
        mut self,
        plan: Plan,
        tx: &mut impl Tx,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            Plan::CreateSchema(plan) => self.create_schema(tx, plan),
            Plan::CreateSeries(plan) => self.create_series(tx, plan),
            Plan::CreateTable(plan) => self.create_table(tx, plan),
            Plan::InsertIntoSeries(plan) => self.insert_into_series(tx, plan),
            Plan::InsertIntoTable(plan) => self.insert_into_table(tx, plan),
            Plan::Query(plan) => self.execute(plan, tx),
        }
    }
}
