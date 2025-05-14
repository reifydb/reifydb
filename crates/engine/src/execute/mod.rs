// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod display;

use crate::{Transaction, TransactionMut};
use base::expression::Expression;
use base::schema::{SchemaName, StoreName};
use base::{CatalogMut, Label, NopStore, Schema, SchemaMut, Store, Value, ValueType};
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

fn execute_node<'a>(
    node: QueryPlan,
    rx: &'a impl Transaction,
    current_labels: Vec<Label>,
    current_schema: Option<String>,
    current_store: Option<String>,
    input: Option<Box<dyn Iterator<Item = Vec<Value>> + 'a>>,
) -> crate::Result<(Vec<Label>, Box<dyn Iterator<Item = Vec<Value>> + 'a>)> {
    let (labels, result_iter, schema, store, next): (
        Vec<Label>,
        Box<dyn Iterator<Item = Vec<Value>> + 'a>,
        Option<String>,
        Option<String>,
        Option<Box<QueryPlan>>,
    ) = match node {
        QueryPlan::Scan { schema, store, next, .. } => (
            current_labels,
            Box::new(rx.scan(store.clone(), None).unwrap()),
            Some(schema.to_string()),
            Some(store.to_string()),
            next,
        ),

        QueryPlan::Limit { limit: count, next, .. } => {
            let input_iter = input.ok_or("Missing input for Limit").unwrap();
            (current_labels, Box::new(input_iter.take(count)), current_schema, current_store, next)
        }

        QueryPlan::Project { expressions, next, .. } => {
            if input.is_none() {
                // Free-standing projection like `SELECT 1`
                let mut labels = vec![];
                let mut values = vec![];

                for (idx, expr) in expressions.into_iter().enumerate() {
                    let value = evaluate::<NopStore>(expr, None, None).unwrap();
                    labels.push(Label::Custom {
                        value: ValueType::from(&value),
                        label: format!("{}", idx + 1),
                    });
                    values.push(value);
                }

                return Ok((labels, Box::new(vec![values].into_iter())));
            }

            let input_iter = input.ok_or("missing rows").unwrap();
            let schema_name = current_schema.as_ref().ok_or("missing schema").unwrap();
            let store_name = current_store.as_ref().ok_or("missing store").unwrap();

            let store = rx.schema(schema_name.deref()).unwrap().get(store_name.deref()).unwrap();

            let labels: Vec<Label> = expressions
                .iter()
                .enumerate()
                .map(|(idx, expr)| match expr {
                    Expression::Identifier(name) => store
                        .get_column(name)
                        .ok()
                        .map(|c| Label::Column { value: c.value, column: c.name })
                        .unwrap_or(Label::Custom {
                            value: ValueType::Undefined,
                            label: name.to_string(),
                        }),
                    _ => {
                        Label::Custom { value: ValueType::Undefined, label: format!("{}", idx + 1) }
                    }
                })
                .collect();

            let store_ref = store;

            let projected_rows = input_iter.map(move |row| {
                expressions
                    .iter()
                    .map(|expr| {
                        evaluate(expr.clone(), Some(&row), Some(store_ref))
                            .unwrap_or(Value::Undefined)
                    })
                    .collect::<Vec<Value>>()
            });

            (labels, Box::new(projected_rows), current_schema, current_store, next)
        }
    };

    if let Some(next_node) = next {
        execute_node(*next_node, rx, labels, schema, store, Some(result_iter))
    } else {
        Ok((labels, result_iter))
    }
}

pub fn evaluate<S: Store>(
    expr: Expression,
    row: Option<&Row>,
    store: Option<&S>,
) -> Result<Value, String> {
    match expr {
        Expression::Identifier(name) => {
            let store = store.ok_or("Store required for identifier evaluation")?;
            let row = row.ok_or("Row required for identifier evaluation")?;
            let index =
                store.get_column_index(&name).map_err(|_| format!("Unknown column '{}'", name))?;
            row.get(index).cloned().ok_or_else(|| format!("No value for column '{}'", name))
        }

        Expression::Constant(value) => Ok(value),

        Expression::Add(left, right) => {
            let left = evaluate::<S>(*left, row, store)?;
            let right = evaluate::<S>(*right, row, store)?;
            Ok(left.add(right))
        }

        _ => Err(format!("Unsupported expression: {:?}", expr)),
    }
}
