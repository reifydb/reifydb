// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{
    Ast, AstCreate, AstDescribe, AstFilter, AstFrom, AstGroupBy, AstInfix, AstInsert, AstLiteral,
    AstPolicy, AstPolicyKind, AstPrefix, AstSelect, AstStatement, InfixOperator,
};
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;

use crate::ast;
use crate::expression::{
    AddExpression, AliasExpression, CallExpression, CastExpression, ColumnExpression,
    ConstantExpression, DivideExpression, EqualExpression, Expression, GreaterThanEqualExpression,
    GreaterThanExpression, IdentExpression, KindExpression, LessThanEqualExpression,
    LessThanExpression, ModuloExpression, MultiplyExpression, NotEqualExpression, PrefixExpression,
    PrefixOperator, SubtractExpression, TupleExpression,
};
pub use error::Error;
use reifydb_catalog::Catalog;
use reifydb_catalog::column::Column;
use reifydb_catalog::column_policy::{ColumnPolicyKind, ColumnSaturationPolicy};
use reifydb_catalog::table::ColumnToCreate;
use reifydb_core::{SortDirection, SortKey, ValueKind};
use reifydb_diagnostic::{Diagnostic, Span};
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Rx;

mod error;
pub mod node;
mod planner;

pub type RowToInsert = Vec<Expression>;

#[derive(Debug)]
pub enum PlanRx {
    /// A Query plan. Recursively executes the query plan tree and returns the resulting rows.
    Query(QueryPlan),
}

#[derive(Debug)]
pub enum PlanTx {
    /// An ADD COLUMN TO plan. Creates and adds a new column
    AddColumnToTable(AddColumnToTablePlan),
    /// A CREATE DEFERRED VIEW plan. Creates a new deferred view.
    CreateDeferredView(CreateDeferredViewPlan),
    /// A CREATE SCHEMA plan. Creates a new schema.
    CreateSchema(CreateSchemaPlan),
    /// A CREATE SEQUENCE plan. Creates a new sequence
    CreateSequence(CreateSequencePlan),
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
pub struct AddColumnToTablePlan {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub if_not_exists: bool,
    pub value: ValueKind,
}

#[derive(Debug)]
pub struct CreateDeferredViewPlan {
    pub schema: String,
    pub view: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct CreateSchemaPlan {
    pub schema: String,
    pub if_not_exists: bool,
    pub span: Span,
}

#[derive(Debug)]
pub struct CreateSequencePlan {
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

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub schema: String,
    pub table: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
    pub span: Span,
}

#[derive(Debug)]
pub enum InsertIntoTablePlan {
    Values { schema: String, table: Span, columns: Vec<Column>, rows_to_insert: Vec<RowToInsert> },
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
    Describe {
        plan: Box<QueryPlan>,
    },
    ScanTable {
        schema: String,
        table: String,
        next: Option<Box<QueryPlan>>,
    },
    Filter {
        expression: Expression,
        next: Option<Box<QueryPlan>>,
    },
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

pub fn plan_tx<VS: VersionedStorage, US: UnversionedStorage>(
    rx: &mut impl Rx,
    statement: AstStatement,
) -> Result<PlanTx> {
    for ast in statement.into_iter().rev() {
        match ast {
            Ast::Create(create) => {
                return match create {
                    AstCreate::DeferredView { schema, name, columns, .. } => {
                        let mut result_columns: Vec<ColumnToCreate> = vec![];

                        for col in columns.iter() {
                            let column_name = col.name.value().to_string();
                            let column_type = col.ty.kind();

                            let policies = if let Some(policy_block) = &col.policies {
                                policy_block
                                    .policies
                                    .iter()
                                    .map(convert_policy)
                                    .collect::<Vec<ColumnPolicyKind>>()
                            } else {
                                vec![]
                            };

                            result_columns.push(ColumnToCreate {
                                name: column_name,
                                value: column_type,
                                policies,
                            });
                        }

                        Ok(PlanTx::CreateDeferredView(CreateDeferredViewPlan {
                            schema: schema.value().to_string(),
                            view: name.value().to_string(),
                            if_not_exists: false,
                            columns: result_columns,
                        }))
                    }
                    AstCreate::Schema { name, .. } => Ok(PlanTx::CreateSchema(CreateSchemaPlan {
                        schema: name.value().to_string(),
                        if_not_exists: false,
                        span: name.0.span,
                    })),
                    AstCreate::Series { schema, name, columns: definitions, .. } => {
                        // let mut columns: Vec<ColumnToCreate> = vec![];
                        //
                        // for definition in &definitions.nodes {
                        //     match definition {
                        //         Ast::Infix(ast) => {
                        //             let name = ast.left.as_identifier();
                        //             let ty = ast.right.as_type();
                        //
                        //             columns.push(ColumnToCreate {
                        //                 name: name.value().to_string(),
                        //                 value: ty.kind(),
                        //             })
                        //         }
                        //         _ => unimplemented!(),
                        //     }
                        // }
                        //
                        // Ok(Plan::CreateSeries(CreateSeriesPlan {
                        //     schema: schema.value().to_string(),
                        //     series: name.value().to_string(),
                        //     if_not_exists: false,
                        //     columns,
                        // }))
                        unimplemented!()
                    }
                    AstCreate::Table { schema, name, columns, .. } => {
                        let mut result_columns: Vec<ColumnToCreate> = vec![];

                        for col in columns.iter() {
                            let column_name = col.name.value().to_string();
                            let column_type = col.ty.kind();

                            let policies = if let Some(policy_block) = &col.policies {
                                policy_block
                                    .policies
                                    .iter()
                                    .map(convert_policy)
                                    .collect::<Vec<ColumnPolicyKind>>()
                            } else {
                                vec![]
                            };

                            result_columns.push(ColumnToCreate {
                                name: column_name,
                                value: column_type,
                                policies,
                            });
                        }

                        Ok(PlanTx::CreateTable(CreateTablePlan {
                            schema: schema.value().to_string(),
                            table: name.value().to_string(),
                            if_not_exists: false,
                            columns: result_columns,
                            span: schema.0.span,
                        }))
                    }
                };
            }
            Ast::Insert(insert) => {
                return match insert {
                    AstInsert { schema, store, columns, rows, .. } => {
                        let schema = schema.value().to_string();
                        let store = store.0.span;

                        let schema = Catalog::get_schema_by_name(rx, &schema).unwrap().unwrap();
                        let Some(table) =
                            Catalog::get_table_by_name(rx, schema.id, &store.fragment).unwrap()
                        else {
                            return Err(Error(Diagnostic::table_not_found(
                                store.clone(),
                                &schema.name,
                                &store.fragment,
                            )));
                        };

                        // Get the store schema from the catalog once
                        // let store_schema =
                        //     catalog.get(&schema).unwrap().get(store.deref()).unwrap();

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
                        let columns_to_insert: Vec<_> = insert_column_names
                            .iter()
                            .map(|name| {
                                Catalog::get_column_by_name(rx, table.id, name.deref())
                                    .unwrap()
                                    .unwrap()
                            })
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
                            .map(|mut row| {
                                let mut values = vec![None; columns_to_insert.len()];

                                for (col_idx, col) in table.columns.iter().enumerate() {
                                    if let Some(&input_idx) = insert_index_map.get(&col.name) {
                                        let expr =
                                            mem::replace(&mut row.nodes[input_idx], Ast::Nop);

                                        let expr = match expr {
                                            Ast::Literal(AstLiteral::Boolean(ast)) => {
                                                Expression::Constant(ConstantExpression::Bool {
                                                    span: ast.0.span,
                                                })
                                            }
                                            Ast::Literal(AstLiteral::Number(ast)) => {
                                                Expression::Constant(ConstantExpression::Number {
                                                    span: ast.0.span,
                                                })
                                            }
                                            Ast::Literal(AstLiteral::Text(ast)) => {
                                                Expression::Constant(ConstantExpression::Text {
                                                    span: ast.0.span,
                                                })
                                            }
                                            Ast::Prefix(AstPrefix { operator, node }) => {
                                                let a = node.deref();

                                                let (span, operator) = match operator {
                                                    ast::AstPrefixOperator::Plus(token) => (
                                                        token.span.clone(),
                                                        PrefixOperator::Plus(token.span),
                                                    ),
                                                    ast::AstPrefixOperator::Negate(token) => (
                                                        token.span.clone(),
                                                        PrefixOperator::Minus(token.span),
                                                    ),
                                                    ast::AstPrefixOperator::Not(token) => {
                                                        unimplemented!()
                                                    }
                                                };

                                                Expression::Prefix(PrefixExpression {
                                                    operator,
                                                    expression: Box::new(match a {
                                                        Ast::Literal(lit) => match lit {
                                                            AstLiteral::Boolean(n) => {
                                                                Expression::Constant(
                                                                    ConstantExpression::Bool {
                                                                        span: n.0.span.clone(),
                                                                    },
                                                                )
                                                            }
                                                            AstLiteral::Number(n) => {
                                                                Expression::Constant(
                                                                    ConstantExpression::Number {
                                                                        span: n.0.span.clone(),
                                                                    },
                                                                )
                                                            }
                                                            AstLiteral::Text(t) => {
                                                                Expression::Constant(
                                                                    ConstantExpression::Text {
                                                                        span: t.0.span.clone(),
                                                                    },
                                                                )
                                                            }
                                                            AstLiteral::Undefined(t) => {
                                                                Expression::Constant(
                                                                    ConstantExpression::Undefined {
                                                                        span: t.0.span.clone(),
                                                                    },
                                                                )
                                                            }
                                                        },
                                                        _ => unimplemented!(),
                                                    }),
                                                    span,
                                                })
                                            }
                                            Ast::Infix(infix) => expression_infix(infix).unwrap(),
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

                        // let s = catalog.get(&schema).unwrap().get(&store).unwrap();

                        let columns = table.columns;

                        // match s.kind().unwrap() {
                        //     StoreKind::Series => {
                        //         Ok(PlanTx::InsertIntoSeries(InsertIntoSeriesPlan::Values {
                        //             schema: schema.name,
                        //             series: store,
                        //             columns,
                        //             rows_to_insert,
                        //         }))
                        //     }
                        Ok(PlanTx::InsertIntoTable(InsertIntoTablePlan::Values {
                            schema: schema.name,
                            table: store,
                            columns,
                            rows_to_insert,
                        }))
                        // }
                        // StoreKind::DeferredView => unreachable!(),
                        // }

                        // FIXME validate
                    }
                };
            }
            Ast::From(from) => return Ok(PlanTx::Query(plan_from(from, None)?)),
            Ast::Select(select) => return Ok(PlanTx::Query(plan_select(select, None)?)),
            node => unreachable!("{node:?}"),
        };
    }

    unreachable!()
}

pub fn convert_policy(ast: &AstPolicy) -> ColumnPolicyKind {
    use ColumnPolicyKind::*;

    match ast.policy {
        AstPolicyKind::Saturation => {
            if ast.value.is_literal_undefined() {
                return Saturation(ColumnSaturationPolicy::Undefined);
            }
            let ident = ast.value.as_identifier().value();
            match ident {
                "error" => Saturation(ColumnSaturationPolicy::Error),
                // "saturate" => Some(Saturation(Saturate)),
                // "wrap" => Some(Saturation(Wrap)),
                // "zero" => Some(Saturation(Zero)),
                _ => unimplemented!(),
            }
        }
        AstPolicyKind::Default => unimplemented!(),
        AstPolicyKind::NotUndefined => unimplemented!(),
    }
}

pub fn plan_rx(statement: AstStatement) -> Result<PlanRx> {
    let mut head: Option<Box<QueryPlan>> = None;

    for ast in statement.into_iter().rev() {
        let plan = plan_ast_node(ast, head)?;
        head = Some(Box::new(plan));
    }

    Ok(head.map(|boxed| PlanRx::Query(*boxed)).unwrap())
}

fn plan_ast_node(ast: Ast, next: Option<Box<QueryPlan>>) -> Result<QueryPlan> {
    match ast {
        Ast::Describe(describe) => match describe {
            AstDescribe::Query { node, .. } => {
                Ok(QueryPlan::Describe { plan: Box::new(plan_ast_node(*node, next)?) })
            }
        },

        Ast::From(from) => plan_from(from, next),
        Ast::GroupBy(group) => plan_group_by(group, next),
        Ast::Filter(filter) => plan_filter(filter, next),
        Ast::Select(select) => plan_select(select, next),
        Ast::OrderBy(order) => Ok(QueryPlan::Sort {
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
            next,
        }),
        Ast::Limit(limit) => Ok(QueryPlan::Limit { limit: limit.limit, next }),

        _ => unimplemented!("Unsupported AST node"),
    }
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
                            alias: Some(IdentExpression(node.0.span.clone())),
                            expression: Box::new(Expression::Column(ColumnExpression(node.0.span))),
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
                        alias: Some(IdentExpression(node.0.span.clone())),
                        expression: Box::new(Expression::Column(ColumnExpression(node.0.span))),
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
        AstFrom::Store { schema, store, .. } => Ok(QueryPlan::ScanTable {
            schema: schema.value().to_string(),
            next: head,
            table: store.value().to_string(),
        }),
        AstFrom::Query { .. } => unimplemented!(),
    }
}

fn plan_filter(filter: AstFilter, head: Option<Box<QueryPlan>>) -> Result<QueryPlan> {
    Ok(QueryPlan::Filter {
        expression: match *filter.node {
            Ast::Infix(node) => expression_infix(node)?,
            node => unimplemented!("{node:?}"),
        },
        next: head,
    })
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
                    alias: Some(IdentExpression(node.0.span.clone())),
                    expression: Box::new(Expression::Column(ColumnExpression(node.0.span))),
                },
                Ast::Infix(node) => AliasExpression {
                    alias: None,
                    expression: Box::new(expression_infix(node).unwrap()),
                },
                Ast::Cast(node) => AliasExpression {
                    alias: None,
                    expression: Box::new(Expression::Cast(CastExpression {
                        span: node.token.span,
                        expression: Box::new(expression(*node.node).unwrap()),
                        to: KindExpression {
                            span: node.to.token().span.clone(),
                            kind: node.to.kind(),
                        },
                    })),
                },
                Ast::Literal(node) => match node {
                    AstLiteral::Boolean(node) => AliasExpression {
                        alias: None,
                        expression: Box::new(Expression::Constant(ConstantExpression::Bool {
                            span: node.0.span,
                        })),
                    },
                    AstLiteral::Number(node) => AliasExpression {
                        alias: None,
                        expression: Box::new(Expression::Constant(ConstantExpression::Number {
                            span: node.0.span,
                        })),
                    },
                    AstLiteral::Text(node) => AliasExpression {
                        alias: None,
                        expression: Box::new(Expression::Constant(ConstantExpression::Text {
                            span: node.0.span,
                        })),
                    },
                    AstLiteral::Undefined(node) => AliasExpression {
                        alias: None,
                        expression: Box::new(Expression::Constant(ConstantExpression::Undefined {
                            span: node.0.span,
                        })),
                    },
                },
                Ast::Prefix(node) => {
                    let (span, operator) = match node.operator {
                        ast::AstPrefixOperator::Plus(token) => {
                            (token.span.clone(), PrefixOperator::Plus(token.span))
                        }
                        ast::AstPrefixOperator::Negate(token) => {
                            (token.span.clone(), PrefixOperator::Minus(token.span))
                        }
                        ast::AstPrefixOperator::Not(token) => unimplemented!(),
                    };

                    AliasExpression {
                        alias: None,
                        expression: Box::new(Expression::Prefix(PrefixExpression {
                            operator,
                            expression: Box::new(expression(*node.node).unwrap()), //FIXME
                            span,
                        })),
                    }
                }
                ast => unimplemented!("{:?}", ast),
            })
            .collect(),
        next: head,
    })
}

fn expression(ast: Ast) -> Result<Expression> {
    match ast {
        Ast::Literal(literal) => match literal {
            AstLiteral::Boolean(literal) => {
                Ok(Expression::Constant(ConstantExpression::Bool { span: literal.0.span }))
            }
            AstLiteral::Number(literal) => {
                Ok(Expression::Constant(ConstantExpression::Number { span: literal.0.span }))
            }
            _ => unimplemented!(),
        },
        Ast::Identifier(identifier) => Ok(Expression::Column(ColumnExpression(identifier.0.span))),
        Ast::Infix(infix) => expression_infix(infix),
        Ast::Tuple(tuple) => {
            let mut expressions = Vec::with_capacity(tuple.len());

            for ast in tuple.nodes {
                expressions.push(expression(ast)?);
            }

            Ok(Expression::Tuple(TupleExpression { expressions, span: tuple.token.span }))
        }
        Ast::Prefix(prefix) => {
            let (span, operator) = match prefix.operator {
                ast::AstPrefixOperator::Plus(token) => {
                    (token.span.clone(), PrefixOperator::Plus(token.span))
                }
                ast::AstPrefixOperator::Negate(token) => {
                    (token.span.clone(), PrefixOperator::Minus(token.span))
                }
                ast::AstPrefixOperator::Not(token) => unimplemented!(),
            };

            Ok(Expression::Prefix(PrefixExpression {
                operator,
                expression: Box::new(expression(*prefix.node)?),
                span,
            }))
        }
        Ast::Cast(node) => Ok(Expression::Cast(CastExpression {
            span: node.token.span,
            expression: Box::new(expression(*node.node).unwrap()),
            to: KindExpression { span: node.to.token().span.clone(), kind: node.to.kind() },
        })),
        Ast::Kind(node) => Ok(Expression::Kind(KindExpression {
            span: node.token().span.clone(),
            kind: node.kind(),
        })),

        _ => unimplemented!("{ast:#?}"),
    }
}

fn expression_infix(infix: AstInfix) -> Result<Expression> {
    match infix.operator {
        InfixOperator::Add(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Add(AddExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::Divide(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Divide(DivideExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::Subtract(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Subtract(SubtractExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::Modulo(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Modulo(ModuloExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::Multiply(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;
            Ok(Expression::Multiply(MultiplyExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::Call(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            let Expression::Column(ColumnExpression(span)) = left else { panic!() };
            let Expression::Tuple(tuple) = right else { panic!() };

            Ok(Expression::Call(CallExpression {
                func: IdentExpression(span),
                args: tuple.expressions,
                span: token.span,
            }))
        }
        InfixOperator::GreaterThan(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            Ok(Expression::GreaterThan(GreaterThanExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::GreaterThanEqual(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            Ok(Expression::GreaterThanEqual(GreaterThanEqualExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::LessThan(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            Ok(Expression::LessThan(LessThanExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::LessThanEqual(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            Ok(Expression::LessThanEqual(LessThanEqualExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::Equal(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            Ok(Expression::Equal(EqualExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
            }))
        }
        InfixOperator::NotEqual(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            Ok(Expression::NotEqual(NotEqualExpression {
                left: Box::new(left),
                right: Box::new(right),
                span: token.span,
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
        // InfixOperator::TypeAscription(_) => {}
    }
}
