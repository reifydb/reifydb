// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod query;

use crate::ast::{Ast, AstStatement};
use crate::expression::Expression;
use reifydb_core::{OrderKey, Span};

pub struct Compiler {}

pub fn compile_logical(ast: AstStatement) -> crate::Result<Vec<LogicalQueryPlan>> {
    let compiler = Compiler {};
    compiler.compile(ast)
}

impl Compiler {
    pub(crate) fn compile(&self, ast: AstStatement) -> crate::Result<Vec<LogicalQueryPlan>> {
        let mut result = Vec::with_capacity(ast.len());
        for node in ast {
            match node {
                Ast::From(node) => result.extend(self.compile_from(node)?),
                node => unimplemented!("{:?}", node),
            }
        }
        Ok(result)
    }
}

#[derive(Debug)]
pub enum LogicalQueryPlan {
    Aggregate(AggregateNode),
    Filter(FilterNode),
    JoinLeft(JoinLeftNode),
    Limit(LimitNode),
    Order(OrderNode),
    Project(ProjectNode),
    TableScan(TableScanNode),
}

#[derive(Debug)]
pub struct AggregateNode {
    pub by: Vec<Expression>,
    pub project: Vec<Expression>,
}

#[derive(Debug)]
pub struct FilterNode {
    pub filter: Vec<Expression>,
}

#[derive(Debug)]
pub struct JoinLeftNode {
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
pub struct ProjectNode {
    pub project: Vec<Expression>,
}

#[derive(Debug)]
pub struct TableScanNode {
    pub schema: Option<Span>,
    pub table: Span,
}
