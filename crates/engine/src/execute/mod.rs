// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod display;

use crate::{Transaction, TransactionMut};
use base::expression::Expression;
use base::schema::{SchemaName, StoreName};
use base::{CatalogMut, Label, Schema, SchemaMut, Store, Value, ValueType};
use base::{Row, StoreToCreate};
use rql::plan::{Plan, QueryPlan};
use std::fmt::Display;
use std::ops::Deref;
use std::vec;

#[derive(Debug)]
pub enum ExecutionResult {
    CreateSchema { schema: SchemaName },
    CreateTable { schema: SchemaName, table: StoreName },
    InsertIntoTable { schema: SchemaName, table: StoreName, inserted: usize },
    Query { labels: Vec<Label>, rows: Vec<Row> },
}

pub fn execute_plan_mut(
    plan: Plan,
    tx: &mut impl TransactionMut,
) -> crate::Result<ExecutionResult> {
    Ok(match plan {
        Plan::CreateSchema { name, if_not_exists } => {
            if if_not_exists {
                tx.catalog_mut()?.create_if_not_exists(&name).unwrap();
            } else {
                tx.catalog_mut()?.create(&name).unwrap();
            }
            ExecutionResult::CreateSchema { schema: name }
        }
        Plan::CreateTable { schema, name, if_not_exists, columns } => {
            if if_not_exists {
                unimplemented!()
            } else {
                tx.schema_mut(&schema)?
                    .create(StoreToCreate::Table { name: name.clone(), columns });
            }

            ExecutionResult::CreateTable { schema, table: name }
        }
        Plan::InsertIntoTableValues { schema, store: name, columns, rows_to_insert } => {
            let mut values = Vec::with_capacity(rows_to_insert.len());

            for row in rows_to_insert {
                let mut row_values = Vec::with_capacity(row.len());
                for expr in row {
                    match expr {
                        Expression::Constant(value) => row_values.push(value),
                        _ => unimplemented!(),
                    }
                }
                values.push(row_values);
            }

            let result = tx.insert(name.deref(), values).unwrap();

            ExecutionResult::InsertIntoTable { schema, table: name, inserted: result.inserted }
        }
        Plan::Query(_) => execute_plan(plan, tx)?,
    })
}

pub fn execute_plan(plan: Plan, rx: &impl Transaction) -> crate::Result<ExecutionResult> {
    let plan = match plan {
        Plan::Query(query) => query,
        _ => unreachable!(), // FIXME
    };
    let (labels, iter) = execute_node(plan, rx, vec![], None, None, None)?;
    Ok(ExecutionResult::Query { labels, rows: iter.collect() })
}

fn execute_node(
    node: QueryPlan,
    rx: &impl Transaction,
    current_labels: Vec<Label>,
    current_schema: Option<String>,
    current_store: Option<String>,
    input: Option<Box<dyn Iterator<Item = Vec<Value>>>>,
) -> crate::Result<(Vec<Label>, Box<dyn Iterator<Item = Vec<Value>>>)> {
    let (labels, result_iter, schema, store, next): (
        Vec<Label>,
        Box<dyn Iterator<Item = Vec<Value>>>,
        Option<String>,
        Option<String>,
        Option<Box<QueryPlan>>,
    ) = match node {
        QueryPlan::Scan { schema, store, next, .. } => {
            // let table = db.tables.get(source).ok_or("Table not found")?;
            // (Box::new(table.scan()), Some(source.to_string()))
            (
                current_labels,
                Box::new(rx.scan(store.clone(), None).unwrap()),
                Some(schema.to_string()),
                Some(store.to_string()),
                next,
            )
        }

        QueryPlan::Limit { limit: count, next, .. } => {
            let input_iter = input.ok_or("Missing input for Limit").unwrap();
            (current_labels, Box::new(input_iter.take(count)), current_schema, current_store, next)
        }

        QueryPlan::Project { expressions, next, .. } => {
            if input.is_none() {
                // free standing projection like select 1

                let mut labels = vec![];
                let mut values = vec![];

                for (idx, expr) in expressions.into_iter().enumerate() {
                    match expr {
                        Expression::Constant(value) => {
                            labels.push(Label::Custom {
                                value: ValueType::from(&value),
                                label: format!("{}", idx + 1),
                            });
                            values.push(value)
                        }
                        _ => unimplemented!(),
                    }
                }

                return Ok((labels, Box::new(vec![values].into_iter())));
            }

            let input_iter = input.ok_or("missing rows").unwrap();
            let schema_name = current_schema.as_ref().ok_or("missing schema").unwrap();
            let store_name = current_store.as_ref().ok_or("missing store").unwrap();

            let store = rx.schema(schema_name.deref()).unwrap().get(store_name.deref()).unwrap();

            let column_labels: Vec<Label> = expressions
                .iter()
                .filter_map(|expr| {
                    if let Expression::Identifier(name) = expr {
                        store.get_column(name).ok().map(|c| Label::Full {
                            value: c.value,
                            schema: SchemaName::from(schema_name.as_str()),
                            store: StoreName::from(schema_name.as_str()),
                            column: c.name,
                        })
                    } else {
                        None
                    }
                })
                .collect();

            let column_indexes: Vec<usize> = expressions
                .iter()
                .filter_map(|expr| {
                    if let Expression::Identifier(name) = expr {
                        store.get_column_index(name).ok()
                    } else {
                        None
                    }
                })
                .collect();

            (
                column_labels,
                Box::new(
                    input_iter
                        .map(move |row| column_indexes.iter().map(|&i| row[i].clone()).collect()),
                ),
                current_schema,
                current_store,
                next,
            )
        }
    };

    if let Some(next_node) = next {
        execute_node(*next_node, rx, labels, schema, store, Some(result_iter))
    } else {
        Ok((labels, result_iter))
    }
}
