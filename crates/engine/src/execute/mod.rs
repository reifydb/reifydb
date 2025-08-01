// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::columns::Columns;
use crate::columnar::layout::ColumnsLayout;
use crate::columnar::{Column, ColumnData, ColumnQualified, TableQualified};
use crate::function::{Functions, math};
use query::compile::compile;
use reifydb_core::interface::{
    ActiveWriteTransaction, Table, UnversionedTransaction,
    VersionedReadTransaction, VersionedTransaction,
};
use reifydb_rql::plan::physical::PhysicalPlan;
use std::marker::PhantomData;
use std::sync::Arc;

mod catalog;
mod mutate;
mod query;

pub struct ExecutionContext {
    pub functions: Functions,
    pub table: Option<Table>,
    pub batch_size: usize,
    pub preserve_row_ids: bool,
}

#[derive(Debug)]
pub(crate) struct Batch {
    pub columns: Columns,
}

pub(crate) trait ExecutionPlan {
    fn next(
        &mut self,
        ctx: &ExecutionContext,
        rx: &mut dyn VersionedReadTransaction,
    ) -> crate::Result<Option<Batch>>;
    fn layout(&self) -> Option<ColumnsLayout>;
}

pub(crate) struct Executor<VT: VersionedTransaction, UT: UnversionedTransaction> {
    functions: Functions,
    _phantom: PhantomData<(VT, UT)>,
}

pub fn execute_rx<VT: VersionedTransaction, UT: UnversionedTransaction>(
    rx: &mut impl VersionedReadTransaction,
    plan: PhysicalPlan,
) -> crate::Result<Columns> {
    let executor: Executor<VT, UT> = Executor {
        // FIXME receive functions from RX
        functions: Functions::builder()
            .register_aggregate("sum", math::aggregate::Sum::new)
            .register_aggregate("min", math::aggregate::Min::new)
            .register_aggregate("max", math::aggregate::Max::new)
            .register_aggregate("avg", math::aggregate::Avg::new)
            .register_scalar("abs", math::scalar::Abs::new)
            .register_scalar("avg", math::scalar::Avg::new)
            .build(),
        _phantom: PhantomData,
    };

    executor.execute_rx(rx, plan)
}

pub fn execute_tx<VT: VersionedTransaction, UT: UnversionedTransaction>(
    atx: &mut ActiveWriteTransaction<VT, UT>,
    plan: PhysicalPlan,
) -> crate::Result<Columns> {
    // FIXME receive functions from atx
    let executor: Executor<VT, UT> = Executor {
        functions: Functions::builder()
            .register_aggregate("sum", math::aggregate::Sum::new)
            .register_aggregate("min", math::aggregate::Min::new)
            .register_aggregate("max", math::aggregate::Max::new)
            .register_aggregate("avg", math::aggregate::Avg::new)
            .register_scalar("abs", math::scalar::Abs::new)
            .register_scalar("avg", math::scalar::Avg::new)
            .build(),
        _phantom: PhantomData,
    };

    executor.execute_tx(atx, plan)
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction> Executor<VT, UT> {
    pub(crate) fn execute_rx(
        self,
        rx: &mut impl VersionedReadTransaction,
        plan: PhysicalPlan,
    ) -> crate::Result<Columns> {
        match plan {
            // Query
            PhysicalPlan::Aggregate(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::JoinInner(_)
            | PhysicalPlan::JoinLeft(_)
            | PhysicalPlan::JoinNatural(_)
            | PhysicalPlan::Take(_)
            | PhysicalPlan::Sort(_)
            | PhysicalPlan::Map(_)
            | PhysicalPlan::InlineData(_)
            | PhysicalPlan::Delete(_)
            | PhysicalPlan::Insert(_)
            | PhysicalPlan::Update(_)
            | PhysicalPlan::TableScan(_) => self.execute_query_plan(rx, plan),

            PhysicalPlan::CreateComputedView(_)
            | PhysicalPlan::CreateSchema(_)
            | PhysicalPlan::CreateTable(_) => unreachable!(), // FIXME return explanatory diagnostic
        }
    }

    pub(crate) fn execute_tx(
        mut self,
        atx: &mut ActiveWriteTransaction<VT, UT>,
        plan: PhysicalPlan,
    ) -> crate::Result<Columns> {
        match plan {
            PhysicalPlan::CreateComputedView(plan) => self.create_computed_view(atx, plan),
            PhysicalPlan::CreateSchema(plan) => self.create_schema(atx, plan),
            PhysicalPlan::CreateTable(plan) => self.create_table(atx, plan),
            PhysicalPlan::Delete(plan) => self.delete(atx, plan),
            PhysicalPlan::Insert(plan) => self.insert(atx, plan),
            PhysicalPlan::Update(plan) => self.update(atx, plan),

            PhysicalPlan::Aggregate(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::JoinInner(_)
            | PhysicalPlan::JoinLeft(_)
            | PhysicalPlan::JoinNatural(_)
            | PhysicalPlan::Take(_)
            | PhysicalPlan::Sort(_)
            | PhysicalPlan::Map(_)
            | PhysicalPlan::InlineData(_)
            | PhysicalPlan::TableScan(_) => self.execute_query_plan(atx, plan),
        }
    }

    fn execute_query_plan(
        self,
        rx: &mut impl VersionedReadTransaction,
        plan: PhysicalPlan,
    ) -> crate::Result<Columns> {
        match plan {
            // PhysicalPlan::Describe { plan } => {
            //     // FIXME evaluating the entire columns is quite wasteful but good enough to write some tests
            //     let result = self.execute_query_plan(rx, *plan)?;
            //     let ExecutionResult::Query { columns, .. } = result else { panic!() };
            //     Ok(ExecutionResult::DescribeQuery { columns })
            // }
            _ => {
                let context = Arc::new(ExecutionContext {
                    functions: self.functions,
                    table: None,
                    batch_size: 1024,
                    preserve_row_ids: false,
                });
                let mut node = compile(plan, rx, context.clone());
                let mut result: Option<Columns> = None;

                while let Some(Batch { columns }) = node.next(&context, rx)? {
                    if let Some(mut result_columns) = result.take() {
                        result_columns.append_columns(columns)?;
                        result = Some(result_columns);
                    } else {
                        result = Some(columns);
                    }
                }

                let layout = node.layout();

                if let Some(mut columns) = result {
                    if let Some(layout) = layout {
                        columns.apply_layout(&layout);
                    }

                    Ok(columns.into())
                } else {
                    // empty columns - reconstruct table, for better UX
                    let columns: Vec<Column> = node
                        .layout()
                        .unwrap_or(ColumnsLayout { columns: vec![] })
                        .columns
                        .into_iter()
                        .map(|layout| match layout.table {
                            Some(table) => Column::TableQualified(TableQualified {
                                table,
                                name: layout.name,
                                data: ColumnData::undefined(0),
                            }),
                            None => Column::ColumnQualified(ColumnQualified {
                                name: layout.name,
                                data: ColumnData::undefined(0),
                            }),
                        })
                        .collect();

                    Ok(Columns::new(columns))
                }
            }
        }
    }
}
