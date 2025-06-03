// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::grpc_db::{
    Int128, QueryResult, Row, RxRequest, RxResult, TxRequest, TxResult, UInt128,
};
use crate::server::grpc::{AuthenticatedUser, grpc_db};
use reifydb_core::Value;
use reifydb_transaction::{Rx, Transaction};
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::spawn_blocking;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::server::grpc::grpc_db::tx_result::Result::{
    CreateSchema, CreateTable, InsertIntoSeries, InsertIntoTable,
};
use reifydb_auth::Principal;
use reifydb_engine::{Engine, ExecutionResult};
use reifydb_storage::Storage;
use tokio_stream::once;

pub struct DbService<S: Storage + 'static, T: Transaction<S> + 'static> {
    pub(crate) engine: Arc<Engine<S, T>>,
}

impl<S: Storage + 'static, T: Transaction<S> + 'static> DbService<S, T> {
    pub fn new(engine: Engine<S, T>) -> Self {
        Self { engine: Arc::new(engine) }
    }
}

pub type TxResultStream = Pin<Box<dyn Stream<Item = Result<grpc_db::TxResult, Status>> + Send>>;
pub type RxResultStream = Pin<Box<dyn Stream<Item = Result<grpc_db::RxResult, Status>> + Send>>;

#[tonic::async_trait]
impl<S: Storage + 'static, T: Transaction<S> + 'static> grpc_db::db_server::Db for DbService<S, T> {
    type TxStream = TxResultStream;

    async fn tx(&self, request: Request<TxRequest>) -> Result<Response<TxResultStream>, Status> {
        let user = request
            .extensions()
            .get::<AuthenticatedUser>()
            .ok_or_else(|| tonic::Status::unauthenticated("No authenticated user found"))?;

        println!("Authenticated as: {:?}", user);

        let query = request.into_inner().query;
        println!("Received query: {}", query);

        let engine = self.engine.clone();
        let result = spawn_blocking(move || {
            let result = engine
                .tx_as(&Principal::System { id: 1, name: "root".to_string() }, &query)
                .unwrap();

            result
        })
        .await
        .unwrap();

        let mut columns: Vec<grpc_db::Column> = vec![];
        let mut rows: Vec<grpc_db::Row> = vec![];

        match &result[0] {
            ExecutionResult::Query { columns: ls, rows: rs } => {
                columns = ls
                    .iter()
                    .map(|c| grpc_db::Column {
                        name: c.name.clone(),
                        value: 0, // or some ID if relevant
                    })
                    .collect();

                rows = rs
                    .iter()
                    .map(|r| Row { values: r.iter().map(value_to_query_value).collect() })
                    .collect();
            }
            ExecutionResult::CreateSchema { schema } => {
                let msg = TxResult {
                    result: Some(CreateSchema(grpc_db::CreateSchema { schema: schema.clone() })),
                };
                return Ok(Response::new(Box::pin(once(Ok(msg))) as TxResultStream));
            }
            ExecutionResult::CreateSeries { schema, series } => {
                // let msg = TxResult {
                //     result: Some(CreateSeries(grpc_db::CreateSeries {
                //         schema: schema.clone(),
                //         series: series.clone(),
                //     })),
                // };
                // return Ok(Response::new(Box::pin(once(Ok(msg))) as TxResultStream));
                unimplemented!()
            }
            ExecutionResult::CreateTable { schema, table } => {
                let msg = TxResult {
                    result: Some(CreateTable(grpc_db::CreateTable {
                        schema: schema.clone(),
                        table: table.clone(),
                    })),
                };
                return Ok(Response::new(Box::pin(once(Ok(msg))) as TxResultStream));
            }
            ExecutionResult::InsertIntoSeries { schema, series, inserted } => {
                let msg = TxResult {
                    result: Some(InsertIntoSeries(grpc_db::InsertIntoSeries {
                        schema: schema.clone(),
                        series: series.clone(),
                        inserted: *inserted as u32,
                    })),
                };
                return Ok(Response::new(Box::pin(once(Ok(msg))) as TxResultStream));
            }
            ExecutionResult::InsertIntoTable { schema, table, inserted } => {
                let msg = TxResult {
                    result: Some(InsertIntoTable(grpc_db::InsertIntoTable {
                        schema: schema.clone(),
                        table: table.clone(),
                        inserted: *inserted as u32,
                    })),
                };
                return Ok(Response::new(Box::pin(once(Ok(msg))) as TxResultStream));
            }
        }

        let result = TxResult {
            result: Some(grpc_db::tx_result::Result::Query(QueryResult { columns, rows })),
        };

        Ok(Response::new(Box::pin(once(Ok(result))) as TxResultStream))
    }

    type RxStream = RxResultStream;

    async fn rx(&self, request: Request<RxRequest>) -> Result<Response<Self::RxStream>, Status> {
        let user = request
            .extensions()
            .get::<AuthenticatedUser>()
            .ok_or_else(|| tonic::Status::unauthenticated("No authenticated user found"))?;

        println!("Authenticated as: {:?}", user);

        let query = request.into_inner().query;
        println!("Received query: {}", query);

        let engine = self.engine.clone();
        let result = spawn_blocking(move || {
            let result = engine
                .rx_as(&Principal::System { id: 1, name: "root".to_string() }, &query)
                .unwrap();
            result
        })
        .await
        .unwrap();

        let mut columns: Vec<grpc_db::Column> = vec![];
        let mut rows: Vec<grpc_db::Row> = vec![];

        match &result[0] {
            ExecutionResult::Query { columns: ls, rows: rs } => {
                columns = ls
                    .iter()
                    .map(|c| grpc_db::Column {
                        name: c.name.clone(),
                        value: 0, // or some ID if relevant
                    })
                    .collect();

                rows = rs
                    .iter()
                    .map(|r| Row { values: r.iter().map(value_to_query_value).collect() })
                    .collect();
            }
            _ => {}
        }

        let result = RxResult {
            result: Some(grpc_db::rx_result::Result::Query(QueryResult { columns, rows })),
        };

        Ok(Response::new(Box::pin(once(Ok(result))) as RxResultStream))
    }
}

fn value_to_query_value(value: &Value) -> grpc_db::Value {
    use grpc_db::value::Kind;

    grpc_db::Value {
        kind: Some(match value {
            Value::Bool(v) => Kind::BoolValue(*v),
            Value::Float4(v) => Kind::Float32Value(v.value()),
            Value::Float8(v) => Kind::Float64Value(v.value()),
            Value::Int1(v) => Kind::Int1Value(*v as i32),
            Value::Int2(v) => Kind::Int2Value(*v as i32),
            Value::Int4(v) => Kind::Int4Value(*v),
            Value::Int8(v) => Kind::Int8Value(*v),
            Value::Int16(v) => {
                Kind::Int16Value(Int128 { high: ((*v) >> 64) as u64, low: *v as u64 })
            }
            Value::String(s) => Kind::StringValue(s.clone()),
            Value::Uint1(v) => Kind::Uint1Value(*v as u32),
            Value::Uint2(v) => Kind::Uint2Value(*v as u32),
            Value::Uint4(v) => Kind::Uint4Value(*v),
            Value::Uint8(v) => Kind::Uint8Value(*v),
            Value::Uint16(v) => {
                Kind::Uint16Value(UInt128 { high: (v >> 64) as u64, low: *v as u64 })
            }
            Value::Undefined => Kind::UndefinedValue(false),
        }),
    }
}
