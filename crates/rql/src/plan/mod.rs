// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{Ast, AstFrom, AstStatement};

use base::expression::Expression;
use base::schema::{Column, SchemaName, StoreName};
pub use error::Error;

mod error;
pub mod node;
mod planner;

#[derive(Debug)]
pub enum Plan {
    /// A CREATE SCHEMA plan. Creates a new schema.
    CreateSchema { name: SchemaName, if_not_exists: bool },
    /// A CREATE TABLE plan. Creates a new schema.
    CreateTable { schema: SchemaName, name: StoreName, if_not_exists: bool, columns: Vec<Column> },
    /// A Query plan. Recursively executes the query plan tree and returns the resulting rows.
    Query(QueryPlan),
}

#[derive(Debug)]
pub enum QueryPlan {
    Scan { schema: SchemaName, store: StoreName, next: Option<Box<QueryPlan>> },
    // Filter {
    //     condition: Expression,
    //     next: Option<Box<Plan>>,
    // },
    Project { expressions: Vec<Expression>, next: Option<Box<QueryPlan>> },
    // OrderBy {
    //     keys: Vec<String>,
    //     next: Option<Box<Plan>>,
    // },
    Limit { limit: usize, next: Option<Box<QueryPlan>> },
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn plan(statement: AstStatement) -> Result<QueryPlan> {
    let mut head: Option<Box<QueryPlan>> = None;

    for ast in statement.into_iter().rev() {
        head = Some(Box::new(match ast {
            Ast::From(AstFrom { token, store }) => QueryPlan::Scan {
                // table: from.source.clone(),
                schema: SchemaName::from("test"),
                store: StoreName::new(store.value().to_string()),
                next: head,
            },
            // Ast::Where(where_clause) => Plan::Filter {
            //     condition: where_clause.condition.clone(),
            //     next: head,
            // },
            Ast::Select(select) => QueryPlan::Project {
                expressions: select
                    .columns
                    .iter()
                    .map(|c| Expression::Identifier(c.as_identifier().value().to_string()))
                    .collect(),
                next: head,
            },
            // Ast::OrderBy(order) => Plan::OrderBy {
            //     keys: order.keys.clone(),
            //     next: head,
            // },
            Ast::Limit(limit) => QueryPlan::Limit { limit: limit.limit, next: head },
            _ => unimplemented!("Unsupported AST node"),
        }));
    }

    Ok(head.map(|boxed| *boxed).unwrap())
}
