// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod catalog;
mod context;
mod error;
mod mutate;
mod query;

#[derive(Debug)]
pub(crate) struct Batch {
    pub frame: Frame,
    pub mask: BitVec,
}

pub(crate) trait ExecutionPlan {
    fn next(&mut self, rx: &mut dyn Rx) -> crate::Result<Option<Batch>>;
    fn layout(&self) -> Option<FrameLayout>;
}

use crate::frame::{FrameColumn, ColumnValues, Frame, FrameLayout};
use crate::function::{Functions, math};
pub use context::ExecutionContext;
pub use error::Error;
use query::compile::compile;
use reifydb_core::BitVec;
use reifydb_core::interface::{Rx, Tx, UnversionedStorage, VersionedStorage};
use reifydb_rql::plan::physical::PhysicalPlan;
use std::marker::PhantomData;
use std::sync::Arc;

pub(crate) struct Executor<VS: VersionedStorage, US: UnversionedStorage> {
    functions: Functions,
    _marker: PhantomData<(VS, US)>,
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
        _marker: PhantomData,
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
        _marker: PhantomData,
    };

    executor.execute_tx(tx, plan)
}

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn execute_rx(self, rx: &mut impl Rx, plan: PhysicalPlan) -> crate::Result<Frame> {
        match plan {
            // Query
            PhysicalPlan::Aggregate(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::JoinLeft(_)
            | PhysicalPlan::Take(_)
            | PhysicalPlan::Sort(_)
            | PhysicalPlan::Map(_)
            | PhysicalPlan::InlineData(_)
            | PhysicalPlan::Insert(_)
            | PhysicalPlan::TableScan(_) => self.execute_query_plan(rx, plan),

            PhysicalPlan::CreateDeferredView(_)
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
            PhysicalPlan::CreateDeferredView(plan) => self.create_deferred_view(tx, plan),
            PhysicalPlan::CreateSchema(plan) => self.create_schema(tx, plan),
            PhysicalPlan::CreateTable(plan) => self.create_table(tx, plan),
            PhysicalPlan::Insert(plan) => self.insert(tx, plan),

            PhysicalPlan::Aggregate(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::JoinLeft(_)
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
                let context = ExecutionContext::new(self.functions);
                let mut node = compile(plan, rx, Arc::new(context));
                let mut result: Option<Frame> = None;

                while let Some(Batch { mut frame, mask }) = node.next(rx)? {
                    frame.filter(&mask)?;
                    if let Some(mut result_frame) = result.take() {
                        result_frame.append_frame(frame)?;
                        result = Some(result_frame);
                    } else {
                        result = Some(frame);
                    }
                }

                if let Some(frame) = result {
                    Ok(frame.into())
                } else {
                    Ok(Frame {
                        name: "frame".to_string(),
                        columns: node
                            .layout()
                            .unwrap_or(FrameLayout { columns: vec![] })
                            .columns
                            .into_iter()
                            .map(|cl| FrameColumn {
                                name: cl.name,
                                values: ColumnValues::with_capacity(cl.data_type, 0),
                            })
                            .collect(),
                        index: Default::default(),
                    })
                }
            }
        }
    }
}
