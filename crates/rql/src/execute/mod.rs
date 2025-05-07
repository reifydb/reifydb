// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::plan::QueryPlan;
use base::Database;
use base::Row;
use base::Value;
use base::catalog::Catalog;
use base::catalog::StoreKind;
use base::expression::Expression;

pub fn execute_plan<C: Catalog>(plan: &QueryPlan, db: &Database<C>) -> Result<Vec<Row>, String> {
    let iter = execute_node(plan, db, None, None)?;
    Ok(iter.collect())
}

fn execute_node<'a, C: Catalog>(
    node: &'a QueryPlan,
    db: &'a Database<C>,
    current_source: Option<String>,
    input: Option<Box<dyn Iterator<Item = Vec<Value>> + 'a>>,
) -> Result<Box<dyn Iterator<Item = Vec<Value>> + 'a>, String> {
    let (result_iter, source): (Box<dyn Iterator<Item = Vec<Value>> + 'a>, Option<String>) =
        match node {
            QueryPlan::Scan { source, .. } => {
                let table = db.tables.get(source).ok_or("Table not found")?;
                (Box::new(table.scan()), Some(source.to_string()))
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
                            let table = match db.catalog.get(source).unwrap().unwrap().kind {
                                StoreKind::Table(table) => table,
                            };

                            table.column_index(name)
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
        execute_node(next_node, db, source, Some(result_iter))
    } else {
        Ok(result_iter)
    }
}
