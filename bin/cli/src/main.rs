// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::rql::plan::{QueryPlan, plan};
use reifydb::rql::{Expression, ast};
use reifydb::{Row, RowIter, Value};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Table {
    pub rows: Vec<Row>,
}

impl Table {
    pub fn scan(&self) -> RowIter {
        Box::new(self.rows.clone().into_iter())
    }
}

#[derive(Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

pub fn execute_plan(plan: &QueryPlan, db: &Database) -> Result<Vec<Row>, String> {
    let iter = execute_node(plan, db, None)?;
    Ok(iter.collect())
}

fn execute_node<'a>(
    node: &'a QueryPlan,
    db: &'a Database,
    input: Option<Box<dyn Iterator<Item = Vec<Value>> + 'a>>,
) -> Result<Box<dyn Iterator<Item = Vec<Value>> + 'a>, String> {
    let result_iter: Box<dyn Iterator<Item = Vec<Value>> + 'a> = match node {
        QueryPlan::Scan { source, .. } => {
            let table = db.tables.get(source).ok_or("Table not found")?;
            Box::new(table.scan())
        }

        QueryPlan::Limit { limit: count, .. } => {
            let input_iter = input.ok_or("Missing input for Limit")?;
            Box::new(input_iter.take(*count))
        }

        QueryPlan::Project { expressions, .. } => {
            let input_iter = input.ok_or("Missing input for Project")?;

            let column_indexes: Vec<usize> = expressions
                .iter()
                .filter_map(|expr| {
                    if let Expression::Identifier(name) = expr {
                        if name == "id" {
                            Some(0)
                        } else if name == "name" {
                            Some(1)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            Box::new(
                input_iter.map(move |row| column_indexes.iter().map(|&i| row[i].clone()).collect()),
            )
        }
    };

    // Recurse if there's a next node
    if let Some(next_node) = match node {
        QueryPlan::Scan { next, .. }
        | QueryPlan::Project { next, .. }
        | QueryPlan::Limit { next, .. } => next.as_deref(),
    } {
        execute_node(next_node, db, Some(result_iter))
    } else {
        Ok(result_iter)
    }
}
fn main() {
    let mut db = Database { tables: HashMap::new() };

    db.tables.insert(
        "users".to_string(),
        Table {
            rows: vec![
                vec![Value::Int2(1), Value::Text("Alice".to_string())],
                vec![Value::Int2(2), Value::Text("Box".to_string())],
            ],
        },
    );

    let mut statements = ast::parse(
        r#"
        FROM users
        LIMIT 10
        SELECT id, name, id, name
    "#,
    );

    for statement in statements {
        let plan = plan(statement).unwrap();

        let result = execute_plan(&plan, &db).unwrap();
        for row in result {
            println!("{:?}", row);
        }
    }
}
