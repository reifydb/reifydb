// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{
    Ast, AstCreate, AstFrom, AstInfix, AstInsert, AstLiteral, AstSelect, AstStatement, AstType,
    InfixOperator,
};
use std::collections::HashMap;
use std::ops::Deref;

use base::expression::Expression;
use base::schema::{ColumnName, SchemaName, StoreName};
use base::{Value, ValueType};
pub use error::Error;
use transaction::{Catalog, ColumnToCreate, Schema, Store};

mod error;
pub mod node;
mod planner;

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
        store: StoreName,
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

pub fn plan_mut(catalog: &impl Catalog, statement: AstStatement) -> Result<Plan> {
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
                    AstInsert { schema, store, columns, rows, .. } => {
                        let schema = SchemaName::new(schema.0.value());
                        let store = StoreName::new(store.0.value());

                        // Get the store schema from the catalog once
                        let store_schema =
                            catalog.get(schema.deref()).unwrap().get(store.deref()).unwrap();

                        // Build the user-specified column name list
                        let insert_column_names: Vec<_> = columns
                            .nodes
                            .into_iter()
                            .map(|column| match column {
                                Ast::Identifier(ast) => ColumnName::new(ast.value()),
                                _ => unimplemented!(),
                            })
                            .collect::<Vec<_>>();

                        // Lookup actual columns from the store
                        let mut columns: Vec<_> = insert_column_names
                            .iter()
                            .map(|name| store_schema.get_column(name.deref()).unwrap())
                            .collect::<Vec<_>>();

                        // Create a mapping: column name -> position in insert input
                        let insert_index_map: HashMap<_, _> = insert_column_names
                            .iter()
                            .enumerate()
                            .map(|(i, name)| (name.clone(), i))
                            .collect();

                        // Now reorder the row expressions to match store_schema.column order
                        let rows_to_insert = rows
                            .into_iter()
                            .map(|row| {
                                let mut values = vec![None; columns.len()];

                                for (col_idx, col) in
                                    store_schema.list_columns().unwrap().iter().enumerate()
                                {
                                    if let Some(&input_idx) = insert_index_map.get(&col.name) {
                                        let expr = match &row.nodes[input_idx] {
                                            Ast::Literal(AstLiteral::Boolean(ast)) => {
                                                Expression::Constant(Value::Bool(ast.value()))
                                            }
                                            Ast::Literal(AstLiteral::Number(ast)) => {
                                                Expression::Constant(Value::Int2(
                                                    ast.value().parse().unwrap(),
                                                ))
                                            }
                                            Ast::Literal(AstLiteral::Text(ast)) => {
                                                Expression::Constant(Value::Text(
                                                    ast.value().to_string(),
                                                ))
                                            }
                                            _ => unimplemented!(),
                                        };
                                        values[col_idx] = Some(expr);
                                    } else {
                                        // Not provided in INSERT, use default
                                        unimplemented!()
                                    }
                                }

                                values.into_iter().map(|v| v.unwrap()).collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>();

                        Ok(Plan::InsertIntoTableValues {
                            schema,
                            store,
                            columns: columns
                                .into_iter()
                                .map(|c| ColumnToInsert {
                                    name: c.name,
                                    value: c.value,
                                    default: c.default,
                                })
                                .collect(),
                            rows_to_insert,
                        })
                        // FIXME validate
                    }
                };
            }
            Ast::Select(select) => return Ok(Plan::Query(plan_select(select, None)?)),
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
                            schema: SchemaName::from(schema.value()),
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
            Ast::Select(select) => plan_select(select, head)?,
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

fn plan_select(select: AstSelect, head: Option<Box<QueryPlan>>) -> Result<QueryPlan> {
    Ok(QueryPlan::Project {
        expressions: select
            .columns
            .into_iter()
            .map(|ast| match ast {
                // Ast::Block(_) => {}
                // Ast::Create(_) => {}
                // Ast::From(_) => {}
                Ast::Identifier(node) => Expression::Identifier(node.value().to_string()),
                Ast::Infix(node) => expression_infix(node).unwrap(),
                Ast::Literal(node) => match node {
                    AstLiteral::Boolean(node) => Expression::Constant(Value::Bool(node.value())),
                    AstLiteral::Number(node) => Expression::Constant(node.try_into().unwrap()),
                    AstLiteral::Text(node) => {
                        Expression::Constant(Value::Text(node.value().to_string()))
                    }
                    AstLiteral::Undefined(_) => Expression::Constant(Value::Undefined),
                },
                ast => unimplemented!("{:?}", ast),
            })
            .collect(),
        next: head,
    })
}

fn expression(ast: Ast) -> Result<Expression> {
    match ast {
        Ast::Literal(literal) => match literal {
            AstLiteral::Number(literal) => {
                let value = literal.try_into().unwrap();
                Ok(Expression::Constant(value))
            }
            _ => unimplemented!(),
        },
        Ast::Identifier(identifier) => Ok(Expression::Identifier(identifier.value().to_string())),
        Ast::Infix(infix) => expression_infix(infix),
        _ => unimplemented!("{ast:#?}"),
    }
}

fn expression_infix(infix: AstInfix) -> Result<Expression> {
    match infix.operator {
        InfixOperator::Add(_) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Add(Box::new(left), Box::new(right)))
        }
        _ => unimplemented!(),
        // InfixOperator::Arrow(_) => {}
        // InfixOperator::AccessPackage(_) => {}
        // InfixOperator::AccessProperty(_) => {}
        // InfixOperator::Assign(_) => {}
        // InfixOperator::Call(_) => {}
        // InfixOperator::Subtract(_) => {}
        // InfixOperator::Multiply(_) => {}
        // InfixOperator::Divide(_) => {}
        // InfixOperator::Modulo(_) => {}
        // InfixOperator::Equal(_) => {}
        // InfixOperator::NotEqual(_) => {}
        // InfixOperator::LessThan(_) => {}
        // InfixOperator::LessThanEqual(_) => {}
        // InfixOperator::GreaterThan(_) => {}
        // InfixOperator::GreaterThanEqual(_) => {}
        // InfixOperator::TypeAscription(_) => {}
    }
}
