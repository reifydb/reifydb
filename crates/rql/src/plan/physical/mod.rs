// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub(crate) mod explain;

use crate::expression::Expression;
use crate::plan::logical::LogicalQueryPlan;
use crate::plan::physical::PhysicalQueryPlan::TableScan;
use reifydb_core::{OrderKey, Span};

struct Compiler {}

pub fn compile_physical(logical: Vec<LogicalQueryPlan>) -> crate::Result<PhysicalQueryPlan> {
    let compiler = Compiler {};
    compiler.compile(logical)
}

impl Compiler {
    fn compile(&self, logical: Vec<LogicalQueryPlan>) -> crate::Result<PhysicalQueryPlan> {
        let mut stack: Vec<PhysicalQueryPlan> = Vec::new();

        for node in logical.into_iter() {
            match node {
                LogicalQueryPlan::Aggregate(aggregate) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Aggregate(AggregateNode {
                        by: aggregate.by,
                        select: aggregate.select,
                        next: Some(Box::new(input)),
                    }));
                }

                LogicalQueryPlan::Filter(filter) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Filter(FilterNode {
                        condition: filter.condition,
                        next: Some(Box::new(input)),
                    }));
                }

                LogicalQueryPlan::Limit(limit) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Limit(LimitNode {
                        limit: limit.limit,
                        next: Some(Box::new(input)),
                    }));
                }

                LogicalQueryPlan::JoinLeft(join) => {
                    let left = stack.pop().unwrap(); // FIXME;
                    let right = compile_physical(join.with)?;
                    stack.push(PhysicalQueryPlan::JoinLeft(JoinLeftNode {
                        left: Box::new(left),
                        right: Box::new(right),
                        on: join.on,
                        next: None,
                    }));
                }

                LogicalQueryPlan::Order(order) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Order(OrderNode {
                        by: order.by,
                        next: Some(Box::new(input)),
                    }));
                }

                LogicalQueryPlan::Select(select) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Select(SelectNode {
                        select: select.select,
                        next: Some(Box::new(input)),
                    }));
                }

                LogicalQueryPlan::TableScan(scan) => {
                    stack.push(TableScan(TableScanNode {
                        schema: scan.schema,
                        table: scan.table,
                        next: None,
                    }));
                }
            }
        }

        if stack.len() != 1 {
            // return Err("Logical plan did not reduce to a single physical plan".into());
            panic!("logical plan did not reduce to a single physical plan"); // FIXME
        }

        Ok(stack.pop().unwrap())
    }
}

#[derive(Debug)]
pub enum PhysicalQueryPlan {
    Aggregate(AggregateNode),
    Filter(FilterNode),
    JoinLeft(JoinLeftNode),
    Limit(LimitNode),
    Order(OrderNode),
    Select(SelectNode),
    TableScan(TableScanNode),
}

#[derive(Debug)]
pub struct AggregateNode {
    pub by: Vec<Expression>,
    pub select: Vec<Expression>,
    pub next: Option<Box<PhysicalQueryPlan>>,
}

#[derive(Debug)]
pub struct FilterNode {
    pub condition: Expression,
    pub next: Option<Box<PhysicalQueryPlan>>,
}

#[derive(Debug)]
pub struct JoinLeftNode {
    pub left: Box<PhysicalQueryPlan>,
    pub right: Box<PhysicalQueryPlan>,
    pub on: Vec<Expression>,
    pub next: Option<Box<PhysicalQueryPlan>>,
}

#[derive(Debug)]
pub struct LimitNode {
    pub limit: usize,
    pub next: Option<Box<PhysicalQueryPlan>>,
}

#[derive(Debug)]
pub struct OrderNode {
    pub by: Vec<OrderKey>,
    pub next: Option<Box<PhysicalQueryPlan>>,
}

#[derive(Debug)]
pub struct SelectNode {
    pub select: Vec<Expression>,
    pub next: Option<Box<PhysicalQueryPlan>>,
}

#[derive(Debug)]
pub struct TableScanNode {
    pub schema: Option<Span>,
    pub table: Span,
    pub next: Option<Box<PhysicalQueryPlan>>,
}
