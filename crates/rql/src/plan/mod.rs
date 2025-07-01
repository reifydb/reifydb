// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast;
use crate::ast::{
    Ast, AstAggregate, AstCreate, AstDescribe, AstFilter, AstFrom, AstInfix, AstInsert, AstJoin,
    AstLiteral, AstOrder, AstPolicy, AstPolicyKind, AstPrefix, AstSelect, AstStatement,
    InfixOperator,
};
use crate::expression::{
    AccessTableExpression, AddExpression, AliasExpression, CallExpression, CastExpression,
    ColumnExpression, ConstantExpression, DivideExpression, EqualExpression, Expression,
    GreaterThanEqualExpression, GreaterThanExpression, IdentExpression, KindExpression,
    LessThanEqualExpression, LessThanExpression, ModuloExpression, MultiplyExpression,
    NotEqualExpression, PrefixExpression, PrefixOperator, SubtractExpression, TupleExpression,
};
use reifydb_catalog::Catalog;
use reifydb_catalog::column::Column;
use reifydb_catalog::column_policy::{ColumnPolicyKind, ColumnSaturationPolicy};
use reifydb_catalog::table::ColumnToCreate;
use reifydb_core::interface::{Rx, UnversionedStorage, VersionedStorage};
use reifydb_core::{Error, Kind, OrderDirection, OrderKey, Span};
use reifydb_diagnostic::catalog::table_not_found;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;

pub(crate) mod logical;
mod physical;
mod planner;

pub type RowToInsert = Vec<Expression>;

#[derive(Debug)]
pub enum PlanRx {
    /// A Query plan. Recursively executes the query plan tree and returns the resulting rows.
    Query(PhysicalQueryPlan),
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
    Query(PhysicalQueryPlan),
}

#[derive(Debug)]
pub struct AddColumnToTablePlan {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub if_not_exists: bool,
    pub value: Kind,
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
pub enum PhysicalQueryPlan {
    Aggregate {
        by: Vec<Expression>,
        project: Vec<Expression>,
        next: Option<Box<PhysicalQueryPlan>>,
    },
    Describe {
        plan: Box<PhysicalQueryPlan>,
    },
    ScanTable {
        schema: String,
        table: String,
        next: Option<Box<PhysicalQueryPlan>>,
    },
    Filter {
        expression: Expression,
        next: Option<Box<PhysicalQueryPlan>>,
    },
    Project {
        expressions: Vec<Expression>,
        next: Option<Box<PhysicalQueryPlan>>,
    },
    Order {
        order_by: Vec<OrderKey>,
        next: Option<Box<PhysicalQueryPlan>>,
    },
    Limit {
        limit: usize,
        next: Option<Box<PhysicalQueryPlan>>,
    },
    LeftJoin {
        left: Box<PhysicalQueryPlan>,
        right: Box<PhysicalQueryPlan>,
        on: Vec<Expression>,
        next: Option<Box<PhysicalQueryPlan>>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn plan_tx<VS: VersionedStorage, US: UnversionedStorage>(
    rx: &mut impl Rx,
    statement: AstStatement,
) -> Result<Option<PlanTx>> {
    if statement.is_empty() {
        return Ok(None);
    }

    match &statement[0] {
        Ast::From(_) | Ast::Select(_) => {
            return if let Some(plan) = plan_rx(statement)? {
                match plan {
                    PlanRx::Query(query_plan) => Ok(Some(PlanTx::Query(query_plan))),
                }
            } else {
                Ok(None)
            };
        }
        _ => {}
    }

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

                        Ok(Some(PlanTx::CreateDeferredView(CreateDeferredViewPlan {
                            schema: schema.value().to_string(),
                            view: name.value().to_string(),
                            if_not_exists: false,
                            columns: result_columns,
                        })))
                    }
                    AstCreate::Schema { name, .. } => {
                        Ok(Some(PlanTx::CreateSchema(CreateSchemaPlan {
                            schema: name.value().to_string(),
                            if_not_exists: false,
                            span: name.0.span,
                        })))
                    }
                    AstCreate::Series { .. } => {
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

                        Ok(Some(PlanTx::CreateTable(CreateTablePlan {
                            schema: schema.value().to_string(),
                            table: name.value().to_string(),
                            if_not_exists: false,
                            columns: result_columns,
                            span: schema.0.span,
                        })))
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
                            return Err(Error(table_not_found(
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
                                                    ast::AstPrefixOperator::Not(_token) => {
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
                        Ok(Some(PlanTx::InsertIntoTable(InsertIntoTablePlan::Values {
                            schema: schema.name,
                            table: store,
                            columns,
                            rows_to_insert,
                        })))
                        // }
                        // StoreKind::DeferredView => unreachable!(),
                        // }

                        // FIXME validate
                    }
                };
            }
            Ast::From(from) => return Ok(Some(PlanTx::Query(plan_from(from, None)?))),
            Ast::Select(select) => return Ok(Some(PlanTx::Query(plan_select(select, None)?))),
            node => unimplemented!("{node:?}"),
        };
    }

    Ok(None)
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

pub fn plan_rx(ast: AstStatement) -> Result<Option<PlanRx>> {
    let mut head: Option<Box<PhysicalQueryPlan>> = None;

    for ast in ast.into_iter().rev() {
        let plan = plan_ast_node(ast, head)?;
        head = Some(Box::new(plan));
    }

    Ok(head.map(|boxed| PlanRx::Query(*boxed)))

    // let nodes = compile_logical(ast);
    // dbg!(&nodes);
    //
    // Ok(None)
}

fn plan_ast_node(ast: Ast, next: Option<Box<PhysicalQueryPlan>>) -> Result<PhysicalQueryPlan> {
    match ast {
        Ast::Describe(describe) => match describe {
            AstDescribe::Query { node, .. } => {
                Ok(PhysicalQueryPlan::Describe { plan: Box::new(plan_ast_node(*node, next)?) })
            }
        },

        Ast::From(from) => plan_from(from, next),
        Ast::Aggregate(group) => plan_aggregate(group, next),
        Ast::Filter(filter) => plan_filter(filter, next),
        Ast::Select(select) => plan_select(select, next),
        Ast::Order(order) => plan_order_by(order, next),
        Ast::Limit(limit) => Ok(PhysicalQueryPlan::Limit { limit: limit.limit, next }),
        Ast::Join(join) => plan_join(join, next),

        _ => unimplemented!("Unsupported AST node"),
    }
}

fn plan_order_by(
    order: AstOrder,
    head: Option<Box<PhysicalQueryPlan>>,
) -> Result<PhysicalQueryPlan> {
    let order_by: Vec<_> = order
        .columns
        .into_iter()
        .zip(order.directions)
        .map(|(column, direction)| {
            let direction = direction
                .map(|direction| match direction.value().to_lowercase().as_str() {
                    "asc" => OrderDirection::Asc,
                    "desc" => OrderDirection::Desc,
                    _ => unimplemented!(),
                })
                .unwrap_or(OrderDirection::Desc);

            OrderKey { column: column.0.span, direction }
        })
        .collect();

    Ok(PhysicalQueryPlan::Order { order_by, next: head })
}

fn plan_aggregate(
    aggregate: AstAggregate,
    head: Option<Box<PhysicalQueryPlan>>,
) -> Result<PhysicalQueryPlan> {
    let by = aggregate
        .by
        .into_iter()
        .map(|ast| match ast {
            Ast::Identifier(node) => Expression::Column(ColumnExpression(node.0.span)),
            ast => unimplemented!("{ast:?}"),
        })
        .collect::<Vec<_>>();

    // FIXME this is duplicated code from plan_select
    let project = aggregate
        .select
        .into_iter()
        .map(|ast| match ast {
            // Ast::Block(_) => {}
            // Ast::Create(_) => {}
            // Ast::From(_) => {}
            Ast::Identifier(node) => Expression::Column(ColumnExpression(node.0.span)),
            Ast::Infix(node) => expression_infix(node).unwrap(),
            ast => unimplemented!("{:?}", ast),
        })
        .collect();

    Ok(PhysicalQueryPlan::Aggregate { by, project, next: head })
}

fn plan_from(from: AstFrom, head: Option<Box<PhysicalQueryPlan>>) -> Result<PhysicalQueryPlan> {
    match from {
        AstFrom::Table { schema, table, .. } => Ok(PhysicalQueryPlan::ScanTable {
            schema: schema.unwrap().value().to_string(),
            next: head,
            table: table.value().to_string(),
        }),
        AstFrom::Query { .. } => unimplemented!(),
    }
}

fn plan_filter(
    filter: AstFilter,
    head: Option<Box<PhysicalQueryPlan>>,
) -> Result<PhysicalQueryPlan> {
    Ok(PhysicalQueryPlan::Filter {
        expression: match *filter.node {
            Ast::Infix(node) => expression_infix(node)?,
            node => unimplemented!("{node:?}"),
        },
        next: head,
    })
}

fn plan_join(join: AstJoin, head: Option<Box<PhysicalQueryPlan>>) -> Result<PhysicalQueryPlan> {
    match join {
        AstJoin::LeftJoin { with, on, .. } => {
            // let left = head.ok_or_else(|| panic!("left side of join is missing"))?;
            // let right = Box::new(plan_ast_node(*with, head)?);

            let on = on.into_iter().map(expression).collect::<Result<Vec<_>>>()?;
            // dbg!(&conditions);

            dbg!(&with);

            Ok(PhysicalQueryPlan::LeftJoin {
                left: Box::new(PhysicalQueryPlan::ScanTable {
                    schema: "test".to_string(),
                    table: "one".to_string(),
                    next: None,
                }),
                right: Box::new(PhysicalQueryPlan::ScanTable {
                    schema: "test".to_string(),
                    table: "two".to_string(),
                    next: None,
                }),
                on,
                next: head,
            })
        }
    }
}

fn plan_select(
    select: AstSelect,
    head: Option<Box<PhysicalQueryPlan>>,
) -> Result<PhysicalQueryPlan> {
    Ok(PhysicalQueryPlan::Project {
        expressions: select
            .select
            .into_iter()
            .map(|ast| match ast {
                // Ast::Block(_) => {}
                // Ast::Create(_) => {}
                // Ast::From(_) => {}
                Ast::Identifier(node) => Expression::Column(ColumnExpression(node.0.span)),
                Ast::Infix(node) => expression_infix(node).unwrap(),
                Ast::Cast(node) => {
                    let mut tuple = node.tuple;
                    let expr = tuple.nodes.remove(0);
                    let ast_kind = tuple.nodes.pop().unwrap();
                    let kind = ast_kind.as_kind().kind();
                    let span = ast_kind.as_kind().token().span.clone();

                    Expression::Cast(CastExpression {
                        span: node.token.span,
                        expression: Box::new(expression(expr).unwrap()),
                        to: KindExpression { span, kind },
                    })
                }
                Ast::Literal(node) => match node {
                    AstLiteral::Boolean(node) => {
                        Expression::Constant(ConstantExpression::Bool { span: node.0.span })
                    }
                    AstLiteral::Number(node) => {
                        Expression::Constant(ConstantExpression::Number { span: node.0.span })
                    }
                    AstLiteral::Text(node) => {
                        Expression::Constant(ConstantExpression::Text { span: node.0.span })
                    }
                    AstLiteral::Undefined(node) => {
                        Expression::Constant(ConstantExpression::Undefined { span: node.0.span })
                    }
                },
                Ast::Prefix(node) => {
                    let (span, operator) = match node.operator {
                        ast::AstPrefixOperator::Plus(token) => {
                            (token.span.clone(), PrefixOperator::Plus(token.span))
                        }
                        ast::AstPrefixOperator::Negate(token) => {
                            (token.span.clone(), PrefixOperator::Minus(token.span))
                        }
                        ast::AstPrefixOperator::Not(_token) => unimplemented!(),
                    };

                    Expression::Prefix(PrefixExpression {
                        operator,
                        expression: Box::new(expression(*node.node).unwrap()), //FIXME
                        span,
                    })
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
                ast::AstPrefixOperator::Not(_token) => unimplemented!(),
            };

            Ok(Expression::Prefix(PrefixExpression {
                operator,
                expression: Box::new(expression(*prefix.node)?),
                span,
            }))
        }
        Ast::Cast(node) => {
            let mut tuple = node.tuple;
            let ast_kind = tuple.nodes.pop().unwrap();
            let expr = tuple.nodes.pop().unwrap();
            let kind = ast_kind.as_kind().kind();
            let span = ast_kind.as_kind().token().span.clone();

            Ok(Expression::Cast(CastExpression {
                span: node.token.span,
                expression: Box::new(expression(expr).unwrap()),
                to: KindExpression { span, kind },
            }))
        }
        Ast::Kind(node) => Ok(Expression::Kind(KindExpression {
            span: node.token().span.clone(),
            kind: node.kind(),
        })),

        _ => unimplemented!("{ast:#?}"),
    }
}

fn expression_infix(infix: AstInfix) -> Result<Expression> {
    match infix.operator {
        InfixOperator::AccessTable(token) => {
            let Ast::Identifier(left) = infix.left.deref() else { unimplemented!() };
            let Ast::Identifier(right) = infix.right.deref() else { unimplemented!() };

            Ok(Expression::AccessTable(AccessTableExpression {
                table: left.0.span.clone(),
                column: right.0.span.clone(),
            }))
        }

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
        InfixOperator::As(token) => {
            let left = expression(*infix.left)?;
            let right = expression(*infix.right)?;

            Ok(Expression::Alias(AliasExpression {
                alias: IdentExpression(right.span()),
                expression: Box::new(left),
                span: token.span,
            }))
        }

        operator => unimplemented!("not implemented: {operator:?}"),
        // InfixOperator::Arrow(_) => {}
        // InfixOperator::AccessPackage(_) => {}
        // InfixOperator::Assign(_) => {}
        // InfixOperator::Subtract(_) => {}
        // InfixOperator::Multiply(_) => {}
        // InfixOperator::Divide(_) => {}
        // InfixOperator::Modulo(_) => {}
        // InfixOperator::TypeAscription(_) => {}
    }
}
