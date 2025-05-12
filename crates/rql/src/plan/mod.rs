// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{Ast, AstCreate, AstFrom, AstInsert, AstLiteral, AstStatement, AstType};

use base::expression::Expression;
use base::schema::{ColumnName, SchemaName, StoreName};
use base::{Value, ValueType};
pub use error::Error;

mod error;
pub mod node;
mod planner;

#[derive(Debug)]
pub struct ColumnToCreate {
    pub name: ColumnName,
    pub value: ValueType,
    pub default: Option<Expression>,
}

#[derive(Debug)]
pub struct ColumnToInsert {
    pub name: ColumnName,
    pub value: ValueType,
    pub default: Option<Expression>,
}

pub type RowToInsert = Vec<Expression>;

#[derive(Debug)]
pub enum Plan {
    /// A CREATE SCHEMA plan. Creates a new schema.
    CreateSchema { name: SchemaName, if_not_exists: bool },
    /// A CREATE TABLE plan. Creates a new table.
    CreateTable {
        schema: SchemaName,
        name: StoreName,
        if_not_exists: bool,
        columns: Vec<ColumnToCreate>,
    },
    /// A INSERT INTO TABLE plan. Inserts values into the table
    InsertIntoTableValues {
        schema: SchemaName,
        name: StoreName,
        columns: Vec<ColumnToInsert>,
        rows_to_insert: Vec<RowToInsert>,
    },
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
                        let mut columns: Vec<ColumnToCreate> = vec![];

                        for definition in &definitions.nodes {
                            match definition {
                                Ast::Infix(ast) => {
                                    let name = ast.left.as_identifier();
                                    let ty = ast.right.as_type();

                                    columns.push(ColumnToCreate {
                                        name: ColumnName::new(name.value()),
                                        value: match ty {
                                            AstType::Boolean(_) => ValueType::Bool,
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
            }
            Ast::Insert(insert) => {
                return match insert {
                    AstInsert::Values { schema, store, columns, rows, .. } => {
                        let mut columns: Vec<ColumnToInsert> = vec![
                            ColumnToInsert {
                                name: ColumnName::new("id"),
                                value: ValueType::Int2,
                                default: None,
                            },
                            ColumnToInsert {
                                name: ColumnName::new("name"),
                                value: ValueType::Text,
                                default: None,
                            },
                            ColumnToInsert {
                                name: ColumnName::new("is_premium"),
                                value: ValueType::Bool,
                                default: None,
                            },
                        ];

                        let mut rows_to_insert: Vec<Vec<Expression>> = vec![];

                        for row in rows {
                            let mut row_to_insert = vec![];
                            for row in row.nodes {
                                match row {
                                    Ast::Literal(literal) => match literal {
                                        AstLiteral::Boolean(ast) => row_to_insert
                                            .push(Expression::Constant(Value::Bool(ast.value()))),
                                        AstLiteral::Number(ast) => {
                                            row_to_insert.push(Expression::Constant(Value::Int2(
                                                ast.value().parse().unwrap(),
                                            )))
                                        }
                                        AstLiteral::Text(ast) => {
                                            row_to_insert.push(Expression::Constant(Value::Text(
                                                ast.value().to_string(),
                                            )))
                                        }
                                        AstLiteral::Undefined(_) => unimplemented!(),
                                    },
                                    _ => unimplemented!(),
                                }
                            }
                            rows_to_insert.push(row_to_insert);
                        }

                        Ok(Plan::InsertIntoTableValues {
                            schema: SchemaName::new(schema.value()),
                            name: StoreName::new(store.value()),
                            columns,
                            rows_to_insert,
                        })
                        // FIXME validate
                    }
                };
            }
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
