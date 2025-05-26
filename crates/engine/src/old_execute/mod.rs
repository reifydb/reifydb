// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod call;
mod display;

use crate::function::FunctionRegistry;
use base::expression::Expression;
use base::{Row, Value, ValueKind};
use dataframe::{ColumnValues, DataFrame};
use rql::plan::Plan;
use std::ops::Deref;
use transaction::{CatalogTx, Rx, SchemaRx, SchemaTx, StoreRx, StoreToCreate, Tx};

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
    Query { columns: Vec<Column>, rows: Vec<Row> },
}

impl From<DataFrame> for ExecutionResult {
    fn from(value: DataFrame) -> Self {
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

pub struct Executor {
    pub functions: FunctionRegistry,
}

pub fn execute_plan_mut(plan: Plan, tx: &mut impl Tx) -> crate::Result<ExecutionResult> {
    Ok(match plan {
        Plan::CreateSchema { name, if_not_exists } => {
            if if_not_exists {
                tx.catalog_mut().unwrap().create_if_not_exists(&name).unwrap();
            } else {
                tx.catalog_mut().unwrap().create(&name).unwrap();
            }
            ExecutionResult::CreateSchema { schema: name }
        }
        Plan::CreateSeries { schema, name, if_not_exists, columns } => {
            if if_not_exists {
                unimplemented!()
            } else {
                tx.schema_mut(&schema)
                    .unwrap()
                    .create(StoreToCreate::Series { name: name.clone(), columns });
            }
            ExecutionResult::CreateSeries { schema, series: name }
        }
        Plan::CreateTable { schema, name, if_not_exists, columns } => {
            if if_not_exists {
                unimplemented!()
            } else {
                tx.schema_mut(&schema)
                    .unwrap()
                    .create(StoreToCreate::Table { name: name.clone(), columns });
            }

            ExecutionResult::CreateTable { schema, table: name }
        }
        Plan::InsertIntoTableValues { schema, table: name, columns, rows_to_insert } => {
            let mut rows = Vec::with_capacity(rows_to_insert.len());

            for row in rows_to_insert {
                let mut row_values = Vec::with_capacity(row.len());
                for expr in row {
                    match expr {
                        Expression::Constant(value) => row_values.push(value),
                        _ => unimplemented!(),
                    }
                }
                rows.push(row_values);
            }

            let result = tx.insert(name.deref(), rows).unwrap();

            ExecutionResult::InsertIntoTable { schema, table: name, inserted: result.inserted }
        }
        Plan::Query(_) => execute_plan(plan, tx)?,
    })
}

pub fn execute_plan(plan: Plan, rx: &impl Rx) -> crate::Result<ExecutionResult> {
    let plan = match plan {
        Plan::Query(query) => query,
        _ => unreachable!(), // FIXME
    };
    // let (labels, iter) = execute_node(plan, rx, vec![], None, None, None)?;
    // Ok(ExecutionResult::Query { labels, rows: iter.collect() })

    Ok(crate::execute::execute(plan, rx).unwrap())
}
