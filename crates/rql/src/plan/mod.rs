// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{Ast, AstCreate, AstFrom, AstStatement, AstType};

use base::ValueType;
use base::expression::Expression;
use base::schema::{Column, ColumnName, SchemaName, StoreName};
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

pub fn plan_mut(statement: AstStatement) -> Result<Plan> {
    for ast in statement.into_iter().rev() {
        match ast {
            Ast::Create(create) => {
                return match create {
                    AstCreate::Schema { name, .. } => Ok(Plan::CreateSchema {
                        name: SchemaName::new(name.value()),
                        if_not_exists: false,
                    }),
                    AstCreate::Table { schema, name, definitions, .. } => {
                        let mut columns: Vec<Column> = vec![];

                        for definition in &definitions.nodes {
                            match definition {
                                Ast::Infix(ast) => {
                                    let name = ast.left.as_identifier();
                                    let ty = ast.right.as_type();

                                    columns.push(Column {
                                        name: ColumnName::new(name.value()),
                                        value_type: match ty {
                                            AstType::Boolean(_) => ValueType::Boolean,
                                            AstType::Float4(_) => unimplemented!(),
                                            AstType::Float8(_) => unimplemented!(),
                                            AstType::Int1(_) => unimplemented!(),
                                            AstType::Int2(_) => ValueType::Int2,
                                            AstType::Int4(_) => unimplemented!(),
                                            AstType::Int8(_) => unimplemented!(),
                                            AstType::Int16(_) => unimplemented!(),
                                            AstType::Number(_) => unimplemented!(),
                                            AstType::Text(_) => ValueType::Text,
                                            AstType::Uint1(_) => unimplemented!(),
                                            AstType::Uint2(_) => ValueType::Uint2,
                                            AstType::Uint4(_) => unimplemented!(),
                                            AstType::Uint8(_) => unimplemented!(),
                                            AstType::Uint16(_) => unimplemented!(),
                                        },
                                        default: None,
                                    })
                                }
                                _ => unimplemented!(),
                            }
                        }

                        Ok(Plan::CreateTable {
                            schema: SchemaName::new(schema.0.value()),
                            name: StoreName::new(name.0.value()),
                            if_not_exists: false,
                            columns,
                        })
                    }
                };
            },
            
            node => unreachable!("{node:?}"),
        };
    }

    unreachable!()
}

pub fn plan(statement: AstStatement) -> Result<Plan> {
    let mut head: Option<Box<QueryPlan>> = None;

    for ast in statement.into_iter().rev() {
        head = Some(Box::new(match ast {
            Ast::From(from) => {
                match from {
                    AstFrom::Store { schema, store, .. } => {
                        QueryPlan::Scan {
                            // table: from.source.clone(),
                            schema: SchemaName::from(store.value()),
                            store: StoreName::new(store.value()),
                            next: head,
                        }
                    }
                    AstFrom::Query { .. } => unimplemented!(),
                }
            }
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

    Ok(head.map(|boxed| Plan::Query(*boxed)).unwrap())
}
