// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{
    Ast, AstCreate, AstFrom, AstGroupBy, AstInfix, AstInsert, AstLiteral, AstPrefix, AstSelect,
    AstStatement, InfixOperator,
};
use std::collections::HashMap;
use std::ops::Deref;

use crate::ast;
use crate::expression::{
    AddExpression, AliasExpression, CallExpression, ColumnExpression, ConstantExpression,
    DivideExpression, Expression, IdentExpression, ModuloExpression, MultiplyExpression,
    PrefixExpression, PrefixOperator, SubstractExpression, TupleExpression,
};
pub use error::Error;
use reifydb_catalog::{CatalogRx, Column, ColumnToCreate, SchemaRx, StoreRx};
use reifydb_core::{SortDirection, SortKey, StoreKind};

mod error;
pub mod node;
mod planner;

pub type RowToInsert = Vec<Expression>;

#[derive(Debug)]
pub enum Plan {
    /// A CREATE SCHEMA plan. Creates a new schema.
    CreateSchema(CreateSchemaPlan),
    /// A CREATE SERIES plan. Creates a new series.
    CreateSeries(CreateSeriesPlan),
    /// A CREATE TABLE plan. Creates a new table.
    CreateTable(CreateTablePlan),
    /// A INSERT INTO TABLE plan. Inserts values into the table
    InsertIntoTable(InsertIntoTablePlan),
    /// A INSERT INTO SERIES plan. Inserts values into the table
    InsertIntoSeries(InsertIntoSeriesPlan),
    /// A Query plan. Recursively executes the query plan tree and returns the resulting rows.
    Query(QueryPlan),
}

#[derive(Debug)]
pub struct CreateSchemaPlan {
    pub schema: String,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSeriesPlan {
    pub schema: String,
    pub series: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug)]
pub struct CreateTablePlan {
    pub schema: String,
    pub table: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug)]
pub enum InsertIntoTablePlan {
    Values { schema: String, table: String, columns: Vec<Column>, rows_to_insert: Vec<RowToInsert> },
}

#[derive(Debug)]
pub enum InsertIntoSeriesPlan {
    Values {
        schema: String,
        series: String,
        columns: Vec<Column>,
        rows_to_insert: Vec<RowToInsert>,
    },
}

#[derive(Debug)]
pub enum QueryPlan {
    Aggregate {
        group_by: Vec<AliasExpression>,
        project: Vec<AliasExpression>,
        next: Option<Box<QueryPlan>>,
    },
    Scan {
        schema: String,
        store: String,
        next: Option<Box<QueryPlan>>,
    },
    // Filter {
    //     condition: Expression,
    //     next: Option<Box<QueryPlan>>,
    // },
    Project {
        expressions: Vec<AliasExpression>,
        next: Option<Box<QueryPlan>>,
    },
    Sort {
        keys: Vec<SortKey>,
        next: Option<Box<QueryPlan>>,
    },
    Limit {
        limit: usize,
        next: Option<Box<QueryPlan>>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn plan_mut(catalog: &impl CatalogRx, statement: AstStatement) -> Result<Plan> {
    for ast in statement.into_iter().rev() {
        match ast {
            Ast::Create(create) => {
                return match create {
                    AstCreate::Schema { name, .. } => Ok(Plan::CreateSchema(CreateSchemaPlan {
                        schema: name.value().to_string(),
                        if_not_exists: false,
                    })),
                    AstCreate::Series { schema, name, definitions, .. } => {
                        let mut columns: Vec<ColumnToCreate> = vec![];

                        for definition in &definitions.nodes {
                            match definition {
                                Ast::Infix(ast) => {
                                    let name = ast.left.as_identifier();
                                    let ty = ast.right.as_type();

                                    columns.push(ColumnToCreate {
                                        name: name.value().to_string(),
                                        value: ty.kind(),
                                    })
                                }
                                _ => unimplemented!(),
                            }
                        }

                        Ok(Plan::CreateSeries(CreateSeriesPlan {
                            schema: schema.value().to_string(),
                            series: name.value().to_string(),
                            if_not_exists: false,
                            columns,
                        }))
                    }
                    AstCreate::Table { schema, name, definitions, .. } => {
                        let mut columns: Vec<ColumnToCreate> = vec![];

                        for definition in &definitions.nodes {
                            match definition {
                                Ast::Infix(ast) => {
                                    let name = ast.left.as_identifier();
                                    let ty = ast.right.as_type();

                                    columns.push(ColumnToCreate {
                                        name: name.value().to_string(),
                                        value: ty.kind(),
                                    })
                                }
                                _ => unimplemented!(),
                            }
                        }

                        Ok(Plan::CreateTable(CreateTablePlan {
                            schema: schema.value().to_string(),
                            table: name.value().to_string(),
                            if_not_exists: false,
                            columns,
                        }))
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
                        let mut columns_to_insert: Vec<_> = insert_column_names
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
                                let mut values = vec![None; columns_to_insert.len()];

                                for (col_idx, col) in
                                    store_schema.list_columns().unwrap().iter().enumerate()
                                {
                                    if let Some(&input_idx) = insert_index_map.get(&col.name) {
                                        let expr = match &row.nodes[input_idx] {
                                            Ast::Literal(AstLiteral::Boolean(ast)) => {
                                                Expression::Constant(ConstantExpression::Bool(
                                                    ast.value(),
                                                ))
                                            }
                                            Ast::Literal(AstLiteral::Number(ast)) => {
                                                Expression::Constant(ConstantExpression::Number(
                                                    ast.0.span.clone(),
                                                ))
                                            }
                                            Ast::Literal(AstLiteral::Text(ast)) => {
                                                Expression::Constant(ConstantExpression::Text(
                                                    ast.value().to_string(),
                                                ))
                                            }
                                            Ast::Prefix(AstPrefix { operator, node }) => {
                                                let a = node.deref();

                                                Expression::Prefix(PrefixExpression {
                                                    operator: match operator {
                                                        ast::PrefixOperator::Plus(_) => {
                                                            PrefixOperator::Plus
                                                        }
                                                        ast::PrefixOperator::Negate(_) => {
                                                            PrefixOperator::Minus
                                                        }
                                                        ast::PrefixOperator::Not(_) => {
                                                            unimplemented!()
                                                        }
                                                    },
                                                    expression: Box::new(match a {
                                                        Ast::Literal(lit) => match lit {
                                                            AstLiteral::Boolean(n) => {
                                                                Expression::Constant(
                                                                    ConstantExpression::Bool(
                                                                        n.value(),
                                                                    ),
                                                                )
                                                            }
                                                            AstLiteral::Number(n) => {
                                                                Expression::Constant(
                                                                    ConstantExpression::Number(
                                                                        n.0.span.clone(),
                                                                    ),
                                                                )
                                                            }
                                                            AstLiteral::Text(t) => {
                                                                Expression::Constant(
                                                                    ConstantExpression::Text(
                                                                        t.value().to_string(),
                                                                    ),
                                                                )
                                                            }
                                                            AstLiteral::Undefined(_) => {
                                                                Expression::Constant(
                                                                    ConstantExpression::Undefined,
                                                                )
                                                            }
                                                        },
                                                        _ => unimplemented!(),
                                                    }),
                                                })
                                            }
                                            node => unimplemented!("{node:?}"),
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

                        let s = catalog.get(&schema).unwrap().get(&store).unwrap();

                        let columns = store_schema.list_columns().unwrap();

                        match s.kind().unwrap() {
                            StoreKind::Series => {
                                Ok(Plan::InsertIntoSeries(InsertIntoSeriesPlan::Values {
                                    schema,
                                    series: store,
                                    columns,
                                    rows_to_insert,
                                }))
                            }
                            StoreKind::Table => {
                                Ok(Plan::InsertIntoTable(InsertIntoTablePlan::Values {
                                    schema,
                                    table: store,
                                    columns,
                                    rows_to_insert,
                                }))
                            }
                        }

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
            Ast::GroupBy(group) => plan_group_by(group, head)?,
            // Ast::Where(where_clause) => Plan::Filter {
            //     condition: where_clause.condition.clone(),
            //     next: head,
            // },
            Ast::Select(select) => plan_select(select, head)?,
            Ast::OrderBy(order) => QueryPlan::Sort {
                keys: order
                    .columns
                    .into_iter()
                    .map(|ast| match ast {
                        Ast::Identifier(ident) => {
                            SortKey { column: ident.name(), direction: SortDirection::Asc }
                        }
                        _ => unimplemented!(),
                    })
                    .collect(),
                next: head,
            },
            Ast::Limit(limit) => QueryPlan::Limit { limit: limit.limit, next: head },
            _ => unimplemented!("Unsupported AST node"),
        }));
    }

    Ok(head.map(|boxed| Plan::Query(*boxed)).unwrap())
}

fn plan_group_by(group: AstGroupBy, head: Option<Box<QueryPlan>>) -> Result<QueryPlan> {
    // if head is project - then use this
    // if head is something else - unhandled for now probably an error
    // if head is none -> just project with group by columns as is
    match head {
        Some(head) => match *head {
            QueryPlan::Project { next, expressions } => Ok(QueryPlan::Aggregate {
                group_by: group
                    .columns
                    .into_iter()
                    .map(|ast| match ast {
                        Ast::Identifier(node) => AliasExpression {
                            alias: Some(node.value().to_string()),
                            expression: Expression::Column(ColumnExpression(
                                node.value().to_string(),
                            )),
                        },
                        _ => unimplemented!(),
                    })
                    .collect(),

                project: expressions,
                next,
            }),
            _ => unimplemented!(),
        },
        None => {
            let columns = group
                .columns
                .into_iter()
                .map(|ast| match ast {
                    Ast::Identifier(node) => AliasExpression {
                        alias: Some(node.value().to_string()),
                        expression: Expression::Column(ColumnExpression(node.value().to_string())),
                    },
                    ast => unimplemented!("{ast:?}"),
                })
                .collect::<Vec<_>>();

            Ok(QueryPlan::Aggregate { group_by: columns.clone(), project: columns, next: None })
        }
    }
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
                Ast::Identifier(node) => AliasExpression {
                    alias: Some(node.value().to_string()),
                    expression: Expression::Column(ColumnExpression(node.value().to_string())),
                },
                Ast::Infix(node) => {
                    AliasExpression { alias: None, expression: expression_infix(node).unwrap() }
                }
                Ast::Literal(node) => match node {
                    AstLiteral::Boolean(node) => AliasExpression {
                        alias: None,
                        expression: Expression::Constant(ConstantExpression::Bool(node.value())),
                    },
                    AstLiteral::Number(node) => AliasExpression {
                        alias: None,
                        expression: Expression::Constant(ConstantExpression::Number(node.0.span)),
                    },
                    AstLiteral::Text(node) => AliasExpression {
                        alias: None,
                        expression: Expression::Constant(ConstantExpression::Text(
                            node.value().to_string(),
                        )),
                    },
                    AstLiteral::Undefined(_) => AliasExpression {
                        alias: None,
                        expression: Expression::Constant(ConstantExpression::Undefined),
                    },
                },
                Ast::Prefix(node) => AliasExpression {
                    alias: None,
                    expression: Expression::Prefix(PrefixExpression {
                        operator: match node.operator {
                            ast::PrefixOperator::Plus(_) => PrefixOperator::Plus,
                            ast::PrefixOperator::Negate(_) => PrefixOperator::Minus,
                            ast::PrefixOperator::Not(_) => unimplemented!(),
                        },
                        expression: Box::new(expression(*node.node).unwrap()), //FIXME
                    }),
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
                Ok(Expression::Constant(ConstantExpression::Number(literal.0.span)))
            }
            _ => unimplemented!(),
        },
        Ast::Identifier(identifier) => {
            Ok(Expression::Column(ColumnExpression(identifier.value().to_string())))
        }
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
            Ok(Expression::Add(AddExpression { left: Box::new(left), right: Box::new(right) }))
        }
        InfixOperator::Divide(_) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Divide(DivideExpression {
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        InfixOperator::Subtract(_) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Subtract(SubstractExpression {
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        InfixOperator::Modulo(_) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Modulo(ModuloExpression {
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        InfixOperator::Multiply(_) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Multiply(MultiplyExpression {
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        InfixOperator::Call(_) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            let Expression::Column(ColumnExpression(name)) = left else { panic!() };
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
