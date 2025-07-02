// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod create;
mod insert;

use crate::expression::Expression;
use crate::plan::logical::LogicalPlan;
use crate::plan::physical::PhysicalPlan::TableScan;
use reifydb_catalog::column::Column;
use reifydb_catalog::table::ColumnToCreate;
use reifydb_core::interface::Rx;
use reifydb_core::{OrderKey, Span};

struct Compiler {}

pub fn compile_physical(
    rx: &mut impl Rx,
    logical: Vec<LogicalPlan>,
) -> crate::Result<Option<PhysicalPlan>> {
    Compiler::compile(rx, logical)
}

impl Compiler {
    fn compile(rx: &mut impl Rx, logical: Vec<LogicalPlan>) -> crate::Result<Option<PhysicalPlan>> {
        if logical.is_empty() {
            return Ok(None);
        }

        let mut stack: Vec<PhysicalPlan> = Vec::new();
        for plan in logical {
            match plan {
                LogicalPlan::CreateSchema(create) => {
                    stack.push(Self::compile_create_schema(rx, create)?);
                }
                LogicalPlan::CreateTable(create) => {
                    stack.push(Self::compile_create_table(rx, create)?);
                }
                LogicalPlan::InsertIntoTable(insert) => {
                    stack.push(Self::compile_insert_into_table(rx, insert)?)
                }
                LogicalPlan::Aggregate(aggregate) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalPlan::Aggregate(AggregateNode {
                        by: aggregate.by,
                        select: aggregate.select,
                        input: Box::new(input),
                    }));
                }

                LogicalPlan::Filter(filter) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalPlan::Filter(FilterNode {
                        conditions: vec![filter.condition],
                        input: Box::new(input),
                    }));
                }

                LogicalPlan::Limit(limit) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalPlan::Limit(LimitNode {
                        limit: limit.limit,
                        input: Box::new(input),
                    }));
                }

                LogicalPlan::JoinLeft(join) => {
                    let left = stack.pop().unwrap(); // FIXME;
                    let right = Self::compile(rx, join.with)?.unwrap();
                    stack.push(PhysicalPlan::JoinLeft(JoinLeftNode {
                        left: Box::new(left),
                        right: Box::new(right),
                        on: join.on,
                    }));
                }

                LogicalPlan::Order(order) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalPlan::Order(OrderNode {
                        by: order.by,
                        input: Box::new(input),
                    }));
                }

                LogicalPlan::Select(select) => {
                    let input = stack.pop().map(Box::new);
                    stack.push(PhysicalPlan::Select(SelectNode { select: select.select, input }));
                }
                LogicalPlan::TableScan(scan) => {
                    stack.push(TableScan(TableScanNode { schema: scan.schema, table: scan.table }));
                }
                _ => unimplemented!(),
            }
        }

        if stack.len() != 1 {
            // return Err("Logical plan did not reduce to a single physical plan".into());
            dbg!(&stack);
            panic!("logical plan did not reduce to a single physical plan"); // FIXME
        }

        Ok(Some(stack.pop().unwrap()))
    }
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    CreateDeferredView(CreateDeferredViewPlan),
    CreateSchema(CreateSchemaPlan),
    CreateTable(CreateTablePlan),
    InsertIntoTable(InsertIntoTablePlan),

    // Query
    Aggregate(AggregateNode),
    Filter(FilterNode),
    JoinLeft(JoinLeftNode),
    Limit(LimitNode),
    Order(OrderNode),
    Select(SelectNode),
    TableScan(TableScanNode),
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewPlan {
    pub schema: Span,
    pub view: Span,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct CreateSchemaPlan {
    pub schema: Span,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub schema: Span,
    pub table: Span,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug, Clone)]
pub enum InsertIntoTablePlan {
    Values { schema: Span, table: Span, columns: Vec<Column>, rows_to_insert: Vec<Vec<Expression>> },
}

#[derive(Debug, Clone)]
pub struct AggregateNode {
    pub input: Box<PhysicalPlan>,
    pub by: Vec<Expression>,
    pub select: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct FilterNode {
    pub input: Box<PhysicalPlan>,
    pub conditions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct JoinLeftNode {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub on: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct LimitNode {
    pub input: Box<PhysicalPlan>,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub struct OrderNode {
    pub input: Box<PhysicalPlan>,
    pub by: Vec<OrderKey>,
}

#[derive(Debug, Clone)]
pub struct SelectNode {
    pub input: Option<Box<PhysicalPlan>>,
    pub select: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct TableScanNode {
    pub schema: Option<Span>,
    pub table: Span,
}
