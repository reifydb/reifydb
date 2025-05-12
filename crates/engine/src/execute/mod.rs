// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Transaction, TransactionMut};
use base::expression::Expression;
use base::schema::{SchemaName, StoreName};
use base::{CatalogMut, Label, Schema, SchemaMut, Store, Value};
use base::{Row, StoreToCreate};
use rql::plan::{Plan, QueryPlan};
use std::ops::Deref;

#[derive(Debug)]
pub enum ExecutionResult {
    CreateSchema { name: SchemaName },
    CreateTable { schema: SchemaName, name: StoreName },
    InsertIntoTable { schema: SchemaName, name: StoreName },
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
            ExecutionResult::CreateSchema { name }
        }
        Plan::CreateTable { schema, name, if_not_exists, columns } => {
            if if_not_exists {
                unimplemented!()
            } else {
                tx.schema_mut(&schema)?
                    .create(StoreToCreate::Table { name: name.clone(), columns });
            }

            ExecutionResult::CreateTable { schema, name }
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

            tx.insert(name.deref(), values).unwrap();

            ExecutionResult::InsertIntoTable { schema, name }
        }
        Plan::Query(_) => unimplemented!(),
    })
}

pub fn execute_plan(plan: Plan, rx: &impl Transaction) -> crate::Result<ExecutionResult> {
    let plan = match plan {
        Plan::Query(query) => query,
        _ => unreachable!(), // FIXME
    };
    let (labels, iter) = execute_node(&plan, rx, vec![], None, None, None)?;
    Ok(ExecutionResult::Query { labels, rows: iter.collect() })
}

fn execute_node(
    node: &QueryPlan,
    rx: &impl Transaction,
    current_labels: Vec<Label>,
    current_schema: Option<String>,
    current_store: Option<String>,
    input: Option<Box<dyn Iterator<Item = Vec<Value>>>>,
) -> crate::Result<(Vec<Label>, Box<dyn Iterator<Item = Vec<Value>>>)> {
    let (labels, result_iter, schema, store): (
        Vec<Label>,
        Box<dyn Iterator<Item = Vec<Value>>>,
        Option<String>,
        Option<String>,
    ) = match node {
        QueryPlan::Scan { schema, store, .. } => {
            // let table = db.tables.get(source).ok_or("Table not found")?;
            // (Box::new(table.scan()), Some(source.to_string()))
            (
                current_labels,
                Box::new(rx.scan(store, None).unwrap()),
                Some(schema.to_string()),
                Some(store.to_string()),
            )
        }

        QueryPlan::Limit { limit: count, .. } => {
            let input_iter = input.ok_or("Missing input for Limit").unwrap();
            (current_labels, Box::new(input_iter.take(*count)), current_schema, current_store)
        }

        QueryPlan::Project { expressions, .. } => {
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
            )
        }
    };

    if let Some(next_node) = match node {
        QueryPlan::Scan { next, .. }
        | QueryPlan::Project { next, .. }
        | QueryPlan::Limit { next, .. } => next.as_deref(),
    } {
        execute_node(next_node, rx, labels, schema, store, Some(result_iter))
    } else {
        Ok((labels, result_iter))
    }
}
