// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::ast::Ast;
use crate::rql::plan::node::Node;

pub use error::Error;

mod error;
pub mod node;
mod planner;

#[derive(Debug)]
pub enum Plan {
    /// A Query plan. Recursively executes the query plan tree and returns the resulting rows.
    Query { node: Node },
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn plan(ast: Ast) -> Result<Plan> {
    Ok(Plan::Query {
        node: Node::Project { input: Box::new(Node::Scan { filter: vec![] }), expressions: vec![] },
    })
}
