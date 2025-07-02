// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub(crate) mod explain;

use crate::expression::Expression;
use crate::plan::logical::LogicalQueryPlan;
use crate::plan::physical::PhysicalQueryPlan::TableScan;
use reifydb_core::{OrderKey, Span};

struct Compiler {}

pub fn compile_physical(
    logical: Vec<LogicalQueryPlan>,
) -> crate::Result<Option<PhysicalQueryPlan>> {
    let compiler = Compiler {};
    compiler.compile(logical)
}

impl Compiler {
    fn compile(&self, logical: Vec<LogicalQueryPlan>) -> crate::Result<Option<PhysicalQueryPlan>> {
        if logical.is_empty() {
            return Ok(None);
        }

        let mut stack: Vec<PhysicalQueryPlan> = Vec::new();

        for node in logical {
            match node {
                LogicalQueryPlan::Aggregate(aggregate) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Aggregate(AggregateNode {
                        by: aggregate.by,
                        select: aggregate.select,
                        input: Box::new(input),
                    }));
                }

                LogicalQueryPlan::Filter(filter) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Filter(FilterNode {
                        conditions: vec![filter.condition],
                        input: Box::new(input),
                    }));
                }

                LogicalQueryPlan::Limit(limit) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Limit(LimitNode {
                        limit: limit.limit,
                        input: Box::new(input),
                    }));
                }

                LogicalQueryPlan::JoinLeft(join) => {
                    let left = stack.pop().unwrap(); // FIXME;
                    let right = compile_physical(join.with)?.unwrap();
                    stack.push(PhysicalQueryPlan::JoinLeft(JoinLeftNode {
                        left: Box::new(left),
                        right: Box::new(right),
                        on: join.on,
                    }));
                }

                LogicalQueryPlan::Order(order) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalQueryPlan::Order(OrderNode {
                        by: order.by,
                        input: Box::new(input),
                    }));
                }

                LogicalQueryPlan::Select(select) => {
                    let input = stack.pop().map(Box::new);
                    stack.push(PhysicalQueryPlan::Select(SelectNode {
                        select: select.select,
                        input,
                    }));
                }

                LogicalQueryPlan::TableScan(scan) => {
                    stack.push(TableScan(TableScanNode { schema: scan.schema, table: scan.table }));
                }
            }
        }

        if stack.len() != 1 {
            // return Err("Logical plan did not reduce to a single physical plan".into());
            panic!("logical plan did not reduce to a single physical plan"); // FIXME
        }

        Ok(Some(stack.pop().unwrap()))
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
    pub input: Box<PhysicalQueryPlan>,
    pub by: Vec<Expression>,
    pub select: Vec<Expression>,
}

#[derive(Debug)]
pub struct FilterNode {
    pub input: Box<PhysicalQueryPlan>,
    pub conditions: Vec<Expression>,
}

#[derive(Debug)]
pub struct JoinLeftNode {
    pub left: Box<PhysicalQueryPlan>,
    pub right: Box<PhysicalQueryPlan>,
    pub on: Vec<Expression>,
}

#[derive(Debug)]
pub struct LimitNode {
    pub input: Box<PhysicalQueryPlan>,
    pub limit: usize,
}

#[derive(Debug)]
pub struct OrderNode {
    pub input: Box<PhysicalQueryPlan>,
    pub by: Vec<OrderKey>,
}

#[derive(Debug)]
pub struct SelectNode {
    pub input: Option<Box<PhysicalQueryPlan>>,
    pub select: Vec<Expression>,
}

#[derive(Debug)]
pub struct TableScanNode {
    pub schema: Option<Span>,
    pub table: Span,
}
