// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::engine::StoreToCreate;
use crate::{CatalogMut, Schema, SchemaMut, Store, Transaction, TransactionMut};
use base::Row;
use base::Value;
use base::expression::Expression;
use base::schema::{SchemaName, StoreName};
use rql::plan::{Plan, QueryPlan};

#[derive(Debug)]
pub enum ExecutionResult {
    CreateSchema { name: SchemaName },
    CreateTable { schema: SchemaName, name: StoreName },
}

pub fn execute_plan_mut(
    plan: Plan,
    tx: &mut impl TransactionMut,
) -> crate::Result<ExecutionResult> {
    Ok(match plan {
        Plan::CreateSchema { name, if_not_exists } => {
            if if_not_exists {
                tx.catalog_mut()?.create_if_not_exists(&name)?;
            } else {
                tx.catalog_mut()?.create(&name)?;
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
        Plan::Query(_) => unimplemented!(),
    })
}

pub fn execute_plan_query(plan: &QueryPlan, rx: &impl Transaction) -> Result<Vec<Row>, String> {
    let iter = execute_node(plan, rx, None, None)?;
    Ok(iter.collect())
}

fn execute_node<'a>(
    node: &'a QueryPlan,
    rx: &'a impl Transaction,
    current_source: Option<String>,
    input: Option<Box<dyn Iterator<Item = Vec<Value>> + 'a>>,
) -> Result<Box<dyn Iterator<Item = Vec<Value>> + 'a>, String> {
    let (result_iter, source): (Box<dyn Iterator<Item = Vec<Value>> + 'a>, Option<String>) =
        match node {
            QueryPlan::Scan { store: source, .. } => {
                // let table = db.tables.get(source).ok_or("Table not found")?;
                // (Box::new(table.scan()), Some(source.to_string()))
                (Box::new(rx.scan(source, None).unwrap()), Some(source.to_string()))
            }

            QueryPlan::Limit { limit: count, .. } => {
                let input_iter = input.ok_or("Missing input for Limit")?;
                (Box::new(input_iter.take(*count)), current_source)
            }

            QueryPlan::Project { expressions, .. } => {
                let input_iter = input.ok_or("Missing input for Project")?;
                let source = current_source.as_ref().ok_or("Missing source for Project")?;

                let column_indexes: Vec<usize> = expressions
                    .iter()
                    .filter_map(|expr| {
                        if let Expression::Identifier(name) = expr {
                            // let table = match &rx.schema("test").unwrap().get(source).unwrap().kind
                            // {
                            //     StoreKind::Table(table) => table,
                            // };
                            //
                            // table.column_index(name)

                            rx.schema("test").unwrap().get(source).unwrap().column_index(name).ok()
                        } else {
                            None
                        }
                    })
                    .collect();

                (
                    Box::new(
                        input_iter.map(move |row| {
                            column_indexes.iter().map(|&i| row[i].clone()).collect()
                        }),
                    ),
                    current_source,
                )
            }
        };

    if let Some(next_node) = match node {
        QueryPlan::Scan { next, .. }
        | QueryPlan::Project { next, .. }
        | QueryPlan::Limit { next, .. } => next.as_deref(),
    } {
        execute_node(next_node, rx, source, Some(result_iter))
    } else {
        Ok(result_iter)
    }
}
