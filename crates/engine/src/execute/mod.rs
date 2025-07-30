// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::frame::Frame;
use crate::column::layout::FrameLayout;
use crate::column::{ColumnQualified, EngineColumn, EngineColumnData, TableQualified};
use crate::function::{Functions, math};
use query::compile::compile;
use reifydb_core::interface::{Rx, Table, Tx, UnversionedStorage, VersionedStorage};
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
    pub frame: Frame,
}

pub(crate) trait ExecutionPlan {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>>;
    fn layout(&self) -> Option<FrameLayout>;
}

pub(crate) struct Executor<VS: VersionedStorage, US: UnversionedStorage> {
    functions: Functions,
    _phantom: PhantomData<(VS, US)>,
}

pub fn execute_rx<VS: VersionedStorage, US: UnversionedStorage>(
    rx: &mut impl Rx,
    plan: PhysicalPlan,
) -> crate::Result<Frame> {
    let executor: Executor<VS, US> = Executor {
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

pub fn execute_tx<VS: VersionedStorage, US: UnversionedStorage>(
    tx: &mut impl Tx<VS, US>,
    plan: PhysicalPlan,
) -> crate::Result<Frame> {
    // FIXME receive functions from TX
    let executor: Executor<VS, US> = Executor {
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

    executor.execute_tx(tx, plan)
}

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn execute_rx(self, rx: &mut impl Rx, plan: PhysicalPlan) -> crate::Result<Frame> {
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
        tx: &mut impl Tx<VS, US>,
        plan: PhysicalPlan,
    ) -> crate::Result<Frame> {
        match plan {
            PhysicalPlan::CreateComputedView(plan) => self.create_computed_view(tx, plan),
            PhysicalPlan::CreateSchema(plan) => self.create_schema(tx, plan),
            PhysicalPlan::CreateTable(plan) => self.create_table(tx, plan),
            PhysicalPlan::Delete(plan) => self.delete(tx, plan),
            PhysicalPlan::Insert(plan) => self.insert(tx, plan),
            PhysicalPlan::Update(plan) => self.update(tx, plan),

            PhysicalPlan::Aggregate(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::JoinInner(_)
            | PhysicalPlan::JoinLeft(_)
            | PhysicalPlan::JoinNatural(_)
            | PhysicalPlan::Take(_)
            | PhysicalPlan::Sort(_)
            | PhysicalPlan::Map(_)
            | PhysicalPlan::InlineData(_)
            | PhysicalPlan::TableScan(_) => self.execute_query_plan(tx, plan),
        }
    }

    fn execute_query_plan(self, rx: &mut impl Rx, plan: PhysicalPlan) -> crate::Result<Frame> {
        match plan {
            // PhysicalPlan::Describe { plan } => {
            //     // FIXME evaluating the entire frame is quite wasteful but good enough to write some tests
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
                let mut result: Option<Frame> = None;

                while let Some(Batch { frame }) = node.next(&context, rx)? {
                    if let Some(mut result_frame) = result.take() {
                        result_frame.append_frame(frame)?;
                        result = Some(result_frame);
                    } else {
                        result = Some(frame);
                    }
                }

                let layout = node.layout();

                if let Some(mut frame) = result {
                    if let Some(layout) = layout {
                        frame.apply_layout(&layout);
                    }

                    Ok(frame.into())
                } else {
                    // empty frame - reconstruct table, for better UX
                    let columns: Vec<EngineColumn> = node
                        .layout()
                        .unwrap_or(FrameLayout { columns: vec![] })
                        .columns
                        .into_iter()
                        .map(|layout| match layout.table {
                            Some(table) => EngineColumn::TableQualified(TableQualified {
                                table,
                                name: layout.name,
                                data: EngineColumnData::undefined(0),
                            }),
                            None => EngineColumn::ColumnQualified(ColumnQualified {
                                name: layout.name,
                                data: EngineColumnData::undefined(0),
                            }),
                        })
                        .collect();

                    let index = columns
                        .iter()
                        .enumerate()
                        .map(|(i, col)| (col.qualified_name(), i))
                        .collect();

                    let frame_index = columns
                        .iter()
                        .enumerate()
                        .filter_map(|(i, col)| {
                            col.table().map(|sf| ((sf.to_string(), col.name().to_string()), i))
                        })
                        .collect();

                    Ok(Frame { name: "".to_string(), columns, index, frame_index })
                }
            }
        }
    }
}
