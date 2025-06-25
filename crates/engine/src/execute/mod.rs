// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod catalog;
mod display;
mod error;
mod query;
mod write;

use crate::execute::query::Batch;
use crate::frame::{ColumnValues, Frame, FrameLayout};
use crate::function::{FunctionRegistry, math};
pub use error::Error;
use reifydb_catalog::schema::SchemaId;
use reifydb_catalog::table::TableId;
use reifydb_core::{Kind, Value};
use reifydb_rql::plan::{PlanRx, PlanTx, QueryPlan};
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::{Rx, Tx};
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
    Query { columns: Vec<Column>, rows: Vec<Vec<Value>> },
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
            .map(|c| {
                let value = match &c.data {
                    ColumnValues::Bool(_, _) => Kind::Bool,
                    ColumnValues::Float4(_, _) => Kind::Float4,
                    ColumnValues::Float8(_, _) => Kind::Float8,
                    ColumnValues::Int1(_, _) => Kind::Int1,
                    ColumnValues::Int2(_, _) => Kind::Int2,
                    ColumnValues::Int4(_, _) => Kind::Int4,
                    ColumnValues::Int8(_, _) => Kind::Int8,
                    ColumnValues::Int16(_, _) => Kind::Int16,
                    ColumnValues::String(_, _) => Kind::Text,
                    ColumnValues::Uint1(_, _) => Kind::Uint1,
                    ColumnValues::Uint2(_, _) => Kind::Uint2,
                    ColumnValues::Uint4(_, _) => Kind::Uint4,
                    ColumnValues::Uint8(_, _) => Kind::Uint8,
                    ColumnValues::Uint16(_, _) => Kind::Uint16,
                    ColumnValues::Undefined(_) => Kind::Undefined,
                };

                Column { name: c.name.clone(), kind: value }
            })
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

        ExecutionResult::Query { columns, rows }
    }
}

pub(crate) struct Executor<VS: VersionedStorage, US: UnversionedStorage> {
    functions: FunctionRegistry,
    _marker: PhantomData<(VS, US)>,
}

pub fn execute_rx<VS: VersionedStorage, US: UnversionedStorage>(
    rx: &mut impl Rx,
    plan: PlanRx,
) -> crate::Result<ExecutionResult> {
    let mut executor: Executor<VS, US> = Executor {
        functions: FunctionRegistry::new(), // FIXME receive functions from RX
        _marker: PhantomData,
    };

    executor.functions.register(math::AbsFunction {});
    executor.functions.register(math::AvgFunction {});

    executor.execute_rx(rx, plan)
}

pub fn execute_tx<VS: VersionedStorage, US: UnversionedStorage>(
    tx: &mut impl Tx<VS, US>,
    plan: PlanTx,
) -> crate::Result<ExecutionResult> {
    let mut executor: Executor<VS, US> = Executor {
        functions: FunctionRegistry::new(), // FIXME receive functions from TX
        _marker: PhantomData,
    };

    executor.functions.register(math::AbsFunction {});
    executor.functions.register(math::AvgFunction {});

    executor.execute_tx(tx, plan)
}

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn execute_query_plan(
        self,
        rx: &mut impl Rx,
        plan: QueryPlan,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            QueryPlan::Describe { plan } => {
                // FIXME evaluating the entire frame is quite wasteful but good enough to write some tests
                let result = self.execute_query_plan(rx, *plan)?;
                let ExecutionResult::Query { columns, .. } = result else { panic!() };
                Ok(ExecutionResult::DescribeQuery { columns })
            }
            _ => {
                let mut node = query::compile(plan, rx);
                let mut result: Option<Frame> = None;
                // let mut fallback: Option<FrameLayout> = None;

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
                    Ok(ExecutionResult::Query {
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
        plan: PlanRx,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            PlanRx::Query(plan) => self.execute_query_plan(rx, plan),
        }
    }

    pub(crate) fn execute_tx(
        mut self,
        tx: &mut impl Tx<VS, US>,
        plan: PlanTx,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            PlanTx::AddColumnToTable(_) => unimplemented!(),
            PlanTx::CreateDeferredView(plan) => self.create_deferred_view(tx, plan),
            PlanTx::CreateSchema(plan) => self.create_schema(tx, plan),
            PlanTx::CreateSequence(_) => unimplemented!(),
            PlanTx::CreateSeries(plan) => self.create_series(tx, plan),
            PlanTx::CreateTable(plan) => self.create_table(tx, plan),
            PlanTx::InsertIntoSeries(plan) => self.insert_into_series(tx, plan),
            PlanTx::InsertIntoTable(plan) => self.insert_into_table(tx, plan),
            PlanTx::Query(plan) => self.execute_query_plan(tx, plan),
        }
    }
}
