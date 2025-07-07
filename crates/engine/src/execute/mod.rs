// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod catalog;
mod error;
mod query;
mod write;

use crate::execute::query::Batch;
use crate::frame::{Column, ColumnValues, Frame, FrameLayout};
use crate::function::{Functions, math};
pub use error::Error;
use reifydb_core::interface::{Rx, Tx, UnversionedStorage, VersionedStorage};
use reifydb_rql::plan::physical::PhysicalPlan;
use std::marker::PhantomData;

pub(crate) struct Executor<VS: VersionedStorage, US: UnversionedStorage> {
    functions: Functions,
    _marker: PhantomData<(VS, US)>,
}

pub fn execute_query<VS: VersionedStorage, US: UnversionedStorage>(
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

pub fn execute<VS: VersionedStorage, US: UnversionedStorage>(
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
    pub(crate) fn execute_query_plan(
        self,
        rx: &mut impl Rx,
        plan: PhysicalPlan,
    ) -> crate::Result<Frame> {
        match plan {
            // PhysicalPlan::Describe { plan } => {
            //     // FIXME evaluating the entire frame is quite wasteful but good enough to write some tests
            //     let result = self.execute_query_plan(rx, *plan)?;
            //     let ExecutionResult::Query { columns, .. } = result else { panic!() };
            //     Ok(ExecutionResult::DescribeQuery { columns })
            // }
            _ => {
                let mut node = query::compile(plan, rx, self.functions);
                let mut result: Option<Frame> = None;

                while let Some(Batch { mut frame, mask }) = node.next()? {
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
                            .map(|cl| Column {
                                name: cl.name,
                                values: ColumnValues::with_capacity(cl.kind, 0),
                            })
                            .collect(),
                        index: Default::default(),
                    })
                }
            }
        }
    }

    pub(crate) fn execute_rx(self, rx: &mut impl Rx, plan: PhysicalPlan) -> crate::Result<Frame> {
        match plan {
            // Query
            PhysicalPlan::Aggregate(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::JoinLeft(_)
            | PhysicalPlan::Limit(_)
            | PhysicalPlan::Order(_)
            | PhysicalPlan::Select(_)
            | PhysicalPlan::TableScan(_) => self.execute_query_plan(rx, plan),

            PhysicalPlan::CreateDeferredView(_)
            | PhysicalPlan::CreateSchema(_)
            | PhysicalPlan::CreateTable(_)
            | PhysicalPlan::InsertIntoTable(_) => unreachable!(), // FIXME return explanatory diagnostic
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
            PhysicalPlan::InsertIntoTable(plan) => self.insert_into_table(tx, plan),
            // Query
            PhysicalPlan::Aggregate(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::JoinLeft(_)
            | PhysicalPlan::Limit(_)
            | PhysicalPlan::Order(_)
            | PhysicalPlan::Select(_)
            | PhysicalPlan::TableScan(_) => self.execute_query_plan(tx, plan),
        }
    }
}
