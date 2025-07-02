// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod create;
pub(crate) mod explain;
mod expression;
mod insert;
mod query;

use crate::ast::{Ast, AstStatement};
use crate::expression::Expression;
use reifydb_catalog::table::ColumnToCreate;
use reifydb_core::{OrderKey, Span};

struct Compiler {}

pub fn compile_logical(ast: AstStatement) -> crate::Result<Vec<LogicalPlan>> {
    Compiler::compile(ast)
}

impl Compiler {
    fn compile(ast: AstStatement) -> crate::Result<Vec<LogicalPlan>> {
        if ast.is_empty() {
            return Ok(vec![]);
        }

        let mut result = Vec::with_capacity(ast.len());
        for node in ast {
            match node {
                Ast::Create(node) => result.push(Self::compile_create(node)?),
                Ast::InsertIntoTable(node) => result.push(Self::compile_insert_into_table(node)?),

                Ast::Aggregate(node) => result.push(Self::compile_aggregate(node)?),
                Ast::Filter(node) => result.push(Self::compile_filter(node)?),
                Ast::From(node) => result.push(Self::compile_from(node)?),
                Ast::Join(node) => result.push(Self::compile_join(node)?),
                Ast::Limit(node) => result.push(Self::compile_limit(node)?),
                Ast::Order(node) => result.push(Self::compile_order(node)?),
                Ast::Select(node) => result.push(Self::compile_select(node)?),
                node => unimplemented!("{:?}", node),
            }
        }
        Ok(result)
    }
}

#[derive(Debug)]
pub enum LogicalPlan {
    CreateDeferredView(CreateDeferredViewNode),
    CreateSchema(CreateSchemaNode),
    CreateSequence(CreateSequenceNode),
    CreateTable(CreateTableNode),
    InsertIntoTable(InsertIntoTableNode),
    // Query
    Aggregate(AggregateNode),
    Filter(FilterNode),
    JoinLeft(JoinLeftNode),
    Limit(LimitNode),
    Order(OrderNode),
    Select(SelectNode),
    TableScan(TableScanNode),
}

#[derive(Debug)]
pub struct CreateDeferredViewNode {
    pub schema: Span,
    pub view: Span,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug)]
pub struct CreateSchemaNode {
    pub schema: Span,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSequenceNode {
    pub schema: Span,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableNode {
    pub schema: Span,
    pub table: Span,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug)]
pub enum InsertIntoTableNode {
    Values { schema: Span, table: Span, columns: Vec<Span>, rows_to_insert: Vec<Vec<Expression>> },
}

#[derive(Debug)]
pub struct AggregateNode {
    pub by: Vec<Expression>,
    pub select: Vec<Expression>,
}

#[derive(Debug)]
pub struct FilterNode {
    pub condition: Expression,
}

#[derive(Debug)]
pub struct JoinLeftNode {
    pub with: Vec<LogicalPlan>,
    pub on: Vec<Expression>,
}
#[derive(Debug)]
pub struct LimitNode {
    pub limit: usize,
}

#[derive(Debug)]
pub struct OrderNode {
    pub by: Vec<OrderKey>,
}

#[derive(Debug)]
pub struct SelectNode {
    pub select: Vec<Expression>,
}

#[derive(Debug)]
pub struct TableScanNode {
    pub schema: Option<Span>,
    pub table: Span,
}
