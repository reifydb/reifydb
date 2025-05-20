// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{
    Ast, AstCreate, AstFrom, AstInfix, AstInsert, AstLiteral, AstSelect, AstStatement, AstType,
    InfixOperator,
};
use std::collections::HashMap;
use std::ops::Deref;

use crate::ast;
use base::expression::{
    CallExpression, Expression, IdentExpression, PrefixExpression, PrefixOperator, TupleExpression,
};
use base::{Value, ValueType};
pub use error::Error;
use transaction::{CatalogRx, ColumnToCreate, SchemaRx, StoreRx};

mod error;
pub mod node;
mod planner;

#[derive(Debug)]
pub struct ColumnToInsert {
    pub name: String,
    pub value: ValueType,
    pub default: Option<Expression>,
}

pub type RowToInsert = Vec<Expression>;

#[derive(Debug)]
pub enum Plan {
    /// A CREATE SCHEMA plan. Creates a new schema.
    CreateSchema { name: String, if_not_exists: bool },
    /// A CREATE TABLE plan. Creates a new table.
    CreateTable { schema: String, name: String, if_not_exists: bool, columns: Vec<ColumnToCreate> },
    /// A INSERT INTO TABLE plan. Inserts values into the table
    InsertIntoTableValues {
        schema: String,
        store: String,
        columns: Vec<ColumnToInsert>,
        rows_to_insert: Vec<RowToInsert>,
    },
    /// A Query plan. Recursively executes the query plan tree and returns the resulting rows.
    Query(QueryPlan),
}

#[derive(Debug)]
pub enum QueryPlan {
    Scan { schema: String, store: String, next: Option<Box<QueryPlan>> },
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

pub fn plan_mut(catalog: &impl CatalogRx, statement: AstStatement) -> Result<Plan> {
    for ast in statement.into_iter().rev() {
        match ast {
            Ast::Create(create) => {
                return match create {
                    AstCreate::Schema { name, .. } => Ok(Plan::CreateSchema {
                        name: name.value().to_string(),
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
                                        name: name.value().to_string(),
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
                            schema: schema.value().to_string(),
                            name: name.value().to_string(),
                            if_not_exists: false,
                            columns,
                        })
                    }
                };
            }
            Ast::Insert(insert) => {
                return match insert {
                    AstInsert { schema, store, columns, rows, .. } => {
                        let schema = schema.value().to_string();
                        let store = store.value().to_string();

                        // Get the store schema from the catalog once
                        let store_schema =
                            catalog.get(&schema).unwrap().get(store.deref()).unwrap();

                        // Build the user-specified column name list
                        let insert_column_names: Vec<_> = columns
                            .nodes
                            .into_iter()
                            .map(|column| match column {
                                Ast::Identifier(ast) => ast.value().to_string(),
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
                            .map(|(i, name)| (name.to_string(), i))
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
            Ast::From(from) => return Ok(Plan::Query(plan_from(from, None)?)),
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
            Ast::From(from) => plan_from(from, head)?,
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

fn plan_from(from: AstFrom, head: Option<Box<QueryPlan>>) -> Result<QueryPlan> {
    match from {
        AstFrom::Store { schema, store, .. } => Ok(QueryPlan::Scan {
            schema: schema.value().to_string(),
            next: head,
            store: store.value().to_string(),
        }),
        AstFrom::Query { .. } => unimplemented!(),
    }
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
        Ast::Tuple(tuple) => {
            let mut expressions = Vec::with_capacity(tuple.len());

            for ast in tuple.nodes {
                expressions.push(expression(ast)?);
            }

            Ok(Expression::Tuple(TupleExpression { expressions }))
        }
        Ast::Prefix(prefix) => {
            Ok(Expression::Prefix(PrefixExpression {
                operator: match prefix.operator {
                    ast::PrefixOperator::Plus(_) => PrefixOperator::Plus,
                    ast::PrefixOperator::Negate(_) => PrefixOperator::Minus,
                    ast::PrefixOperator::Not(_) => unimplemented!(),
                }, // FIXME ast and expression share the same operator --> use the same enum
                expression: Box::new(expression(*prefix.node)?),
            }))
        }
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
        InfixOperator::Call(_) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            let Expression::Identifier(name) = left else { panic!() };
            let Expression::Tuple(tuple) = right else { panic!() };

            Ok(Expression::Call(CallExpression {
                func: IdentExpression { name },
                args: tuple.expressions,
            }))
        }
        operator => unimplemented!("not implemented: {operator:?}"),
        // InfixOperator::Arrow(_) => {}
        // InfixOperator::AccessPackage(_) => {}
        // InfixOperator::AccessProperty(_) => {}
        // InfixOperator::Assign(_) => {}
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
