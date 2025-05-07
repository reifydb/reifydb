// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{Ast, AstFrom, AstStatement};
use std::ops::Deref;

use crate::expression::Expression;
pub use error::Error;

mod error;
pub mod node;
mod planner;

#[derive(Debug)]
pub enum Plan {
    /// A Query plan. Recursively executes the query plan tree and returns the resulting rows.
    Query(QueryPlan),
}

#[derive(Debug)]
pub enum QueryPlan {
    Scan { source: String, next: Option<Box<QueryPlan>> },
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
            Ast::From(AstFrom { token, source }) => QueryPlan::Scan {
                // table: from.source.clone(),
                source: source.value().to_string(),
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
