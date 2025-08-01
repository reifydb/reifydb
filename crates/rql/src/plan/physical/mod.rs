// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod create;

use crate::expression::{Expression, AliasExpression};
use crate::plan::logical::LogicalPlan;
use crate::plan::physical::PhysicalPlan::TableScan;
use reifydb_catalog::table::ColumnToCreate;
use reifydb_core::interface::VersionedReadTransaction;
use reifydb_core::{JoinType, OwnedSpan, SortKey};

struct Compiler {}

pub fn compile_physical(
	rx: &mut impl VersionedReadTransaction,
	logical: Vec<LogicalPlan>,
) -> crate::Result<Option<PhysicalPlan>> {
    Compiler::compile(rx, logical)
}

impl Compiler {
    fn compile(rx: &mut impl VersionedReadTransaction, logical: Vec<LogicalPlan>) -> crate::Result<Option<PhysicalPlan>> {
        if logical.is_empty() {
            return Ok(None);
        }

        let mut stack: Vec<PhysicalPlan> = Vec::new();
        for plan in logical {
            match plan {
                LogicalPlan::Aggregate(aggregate) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalPlan::Aggregate(AggregateNode {
                        by: aggregate.by,
                        map: aggregate.map,
                        input: Box::new(input),
                    }));
                }

                LogicalPlan::CreateSchema(create) => {
                    stack.push(Self::compile_create_schema(rx, create)?);
                }

                LogicalPlan::CreateTable(create) => {
                    stack.push(Self::compile_create_table(rx, create)?);
                }

                LogicalPlan::Filter(filter) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalPlan::Filter(FilterNode {
                        conditions: vec![filter.condition],
                        input: Box::new(input),
                    }));
                }

                LogicalPlan::InlineData(inline) => {
                    stack.push(PhysicalPlan::InlineData(InlineDataNode { rows: inline.rows }));
                }

                LogicalPlan::Delete(delete) => {
                    let input = stack.pop().map(|i| Box::new(i));
                    stack.push(PhysicalPlan::Delete(DeletePlan {
                        input,
                        schema: delete.schema,
                        table: delete.table,
                    }))
                }

                LogicalPlan::Insert(insert) => {
                    let input = stack.pop().unwrap();
                    stack.push(PhysicalPlan::Insert(InsertPlan {
                        input: Box::new(input),
                        schema: insert.schema,
                        table: insert.table,
                    }))
                }

                LogicalPlan::Update(update) => {
                    let input = stack.pop().unwrap();
                    stack.push(PhysicalPlan::Update(UpdatePlan {
                        input: Box::new(input),
                        schema: update.schema,
                        table: update.table,
                    }))
                }

                LogicalPlan::JoinInner(join) => {
                    let left = stack.pop().unwrap(); // FIXME;
                    let right = Self::compile(rx, join.with)?.unwrap();
                    stack.push(PhysicalPlan::JoinInner(JoinInnerNode {
                        left: Box::new(left),
                        right: Box::new(right),
                        on: join.on,
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

                LogicalPlan::JoinNatural(join) => {
                    let left = stack.pop().unwrap(); // FIXME;
                    let right = Self::compile(rx, join.with)?.unwrap();
                    stack.push(PhysicalPlan::JoinNatural(JoinNaturalNode {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type: join.join_type,
                    }));
                }

                LogicalPlan::Order(order) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalPlan::Sort(SortNode {
                        by: order.by,
                        input: Box::new(input),
                    }));
                }

                LogicalPlan::Map(map) => {
                    let input = stack.pop().map(Box::new);
                    stack.push(PhysicalPlan::Map(MapNode { map: map.map, input }));
                }

                LogicalPlan::TableScan(scan) => {
                    stack.push(TableScan(TableScanNode { schema: scan.schema, table: scan.table }));
                }

                LogicalPlan::Take(take) => {
                    let input = stack.pop().unwrap(); // FIXME
                    stack.push(PhysicalPlan::Take(TakeNode {
                        take: take.take,
                        input: Box::new(input),
                    }));
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
    CreateComputedView(CreateComputedViewPlan),
    CreateSchema(CreateSchemaPlan),
    CreateTable(CreateTablePlan),
    // Mutate
    Delete(DeletePlan),
    Insert(InsertPlan),
    Update(UpdatePlan),

    // Query
    Aggregate(AggregateNode),
    Filter(FilterNode),
    JoinInner(JoinInnerNode),
    JoinLeft(JoinLeftNode),
    JoinNatural(JoinNaturalNode),
    Take(TakeNode),
    Sort(SortNode),
    Map(MapNode),
    InlineData(InlineDataNode),
    TableScan(TableScanNode),
}

#[derive(Debug, Clone)]
pub struct CreateComputedViewPlan {
    pub schema: OwnedSpan,
    pub view: OwnedSpan,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct CreateSchemaPlan {
    pub schema: OwnedSpan,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub schema: OwnedSpan,
    pub table: OwnedSpan,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct AggregateNode {
    pub input: Box<PhysicalPlan>,
    pub by: Vec<Expression>,
    pub map: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct FilterNode {
    pub input: Box<PhysicalPlan>,
    pub conditions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub input: Option<Box<PhysicalPlan>>,
    pub schema: Option<OwnedSpan>,
    pub table: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub input: Box<PhysicalPlan>,
    pub schema: Option<OwnedSpan>,
    pub table: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub input: Box<PhysicalPlan>,
    pub schema: Option<OwnedSpan>,
    pub table: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct JoinInnerNode {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub on: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct JoinLeftNode {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub on: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct JoinNaturalNode {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub join_type: JoinType,
}

#[derive(Debug, Clone)]
pub struct SortNode {
    pub input: Box<PhysicalPlan>,
    pub by: Vec<SortKey>,
}

#[derive(Debug, Clone)]
pub struct MapNode {
    pub input: Option<Box<PhysicalPlan>>,
    pub map: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct InlineDataNode {
    pub rows: Vec<Vec<AliasExpression>>,
}

#[derive(Debug, Clone)]
pub struct TableScanNode {
    pub schema: Option<OwnedSpan>,
    pub table: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct TakeNode {
    pub input: Box<PhysicalPlan>,
    pub take: usize,
}
