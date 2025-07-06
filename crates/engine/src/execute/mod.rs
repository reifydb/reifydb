// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod catalog;
mod display;
mod error;
mod query;
mod write;

use crate::execute::query::Batch;
use crate::frame::{ColumnValues, Frame, FrameLayout};
use crate::function::{Functions, math};
pub use error::Error;
use reifydb_catalog::schema::SchemaId;
use reifydb_catalog::table::TableId;
use reifydb_core::interface::{Rx, Tx, UnversionedStorage, VersionedStorage};
use reifydb_core::{Kind, Value};
use reifydb_rql::plan::physical::PhysicalPlan;
use std::marker::PhantomData;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    pub name: String,
    pub kind: Kind,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExecutionResult {
    CreateDeferredView { schema: String, view: String },
    CreateSchema(CreateSchemaResult),
    CreateSeries { schema: String, series: String },
    CreateTable(CreateTableResult),
    InsertIntoTable { schema: String, table: String, inserted: usize },
    InsertIntoSeries { schema: String, series: String, inserted: usize },
    OldQuery { columns: Vec<Column>, rows: Vec<Vec<Value>> },
    DescribeQuery { columns: Vec<Column> },
}

#[derive(Debug, Eq, PartialEq)]
pub struct CreateSchemaResult {
    pub id: SchemaId,
    pub schema: String,
    pub created: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CreateTableResult {
    pub id: TableId,
    pub schema: String,
    pub table: String,
    pub created: bool,
}

impl From<Frame> for ExecutionResult {
    fn from(value: Frame) -> Self {
        let columns: Vec<Column> = value
            .columns
            .iter()
            .map(|c| Column { name: c.name.clone(), kind: c.data.kind() })
            .collect();

        let row_count = value.columns.first().map_or(0, |col| col.data.len());
        let mut rows = Vec::with_capacity(row_count);

        for row_idx in 0..row_count {
            let mut row = Vec::with_capacity(value.columns.len());

            for col in &value.columns {
                let value = match &col.data {
                    ColumnValues::Bool(vals, valid) => {
                        if valid[row_idx] {
                            Value::Bool(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Float4(vals, valid) => {
                        if valid[row_idx] {
                            Value::float4(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Float8(vals, valid) => {
                        if valid[row_idx] {
                            Value::float8(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int1(vals, valid) => {
                        if valid[row_idx] {
                            Value::Int1(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int2(vals, valid) => {
                        if valid[row_idx] {
                            Value::Int2(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int4(vals, valid) => {
                        if valid[row_idx] {
                            Value::Int4(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int8(vals, valid) => {
                        if valid[row_idx] {
                            Value::Int8(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Int16(vals, valid) => {
                        if valid[row_idx] {
                            Value::Int16(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint1(vals, valid) => {
                        if valid[row_idx] {
                            Value::Uint1(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint2(vals, valid) => {
                        if valid[row_idx] {
                            Value::Uint2(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint4(vals, valid) => {
                        if valid[row_idx] {
                            Value::Uint4(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint8(vals, valid) => {
                        if valid[row_idx] {
                            Value::Uint8(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Uint16(vals, valid) => {
                        if valid[row_idx] {
                            Value::Uint16(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::String(vals, valid) => {
                        if valid[row_idx] {
                            Value::String(vals[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Undefined(_) => Value::Undefined,
                };
                row.push(value);
            }

            rows.push(row);
        }

        ExecutionResult::OldQuery { columns, rows }
    }
}

pub(crate) struct Executor<VS: VersionedStorage, US: UnversionedStorage> {
    functions: Functions,
    _marker: PhantomData<(VS, US)>,
}

pub fn execute_query<VS: VersionedStorage, US: UnversionedStorage>(
    rx: &mut impl Rx,
    plan: PhysicalPlan,
) -> crate::Result<ExecutionResult> {
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
) -> crate::Result<ExecutionResult> {
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
    ) -> crate::Result<ExecutionResult> {
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
                    Ok(ExecutionResult::OldQuery {
                        columns: node
                            .layout()
                            .unwrap_or(FrameLayout { columns: vec![] })
                            .columns
                            .into_iter()
                            .map(|cl| Column { name: cl.name, kind: cl.kind })
                            .collect(),
                        rows: vec![],
                    })
                }
            }
        }
    }

    pub(crate) fn execute_rx(
        self,
        rx: &mut impl Rx,
        plan: PhysicalPlan,
    ) -> crate::Result<ExecutionResult> {
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
    ) -> crate::Result<ExecutionResult> {
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
