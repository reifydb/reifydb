// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::grpc_db::{
    Int128, QueryResult, Row, RxRequest, RxResult, TxRequest, TxResult, UInt128,
};
use crate::server::grpc::{AuthenticatedUser, grpc_db};
use reifydb_core::Value;
use reifydb_transaction::Transaction;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::spawn_blocking;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::server::grpc::grpc_db::tx_result::Result::{
    CreateSchema, CreateTable, InsertIntoSeries, InsertIntoTable,
};
use reifydb_auth::Principal;
use reifydb_engine::{CreateSchemaResult, CreateTableResult, Engine, ExecutionResult};
use reifydb_storage::Storage;
use tokio_stream::once;

pub struct DbService<S: Storage + 'static, T: Transaction<S, S> + 'static> {
    pub(crate) engine: Arc<Engine<S, S, T>>,
}

impl<S: Storage + 'static, T: Transaction<S, S> + 'static> DbService<S, T> {
    pub fn new(engine: Engine<S, S, T>) -> Self {
        Self { engine: Arc::new(engine) }
    }
}

pub type TxResultStream = Pin<Box<dyn Stream<Item = Result<grpc_db::TxResult, Status>> + Send>>;
pub type RxResultStream = Pin<Box<dyn Stream<Item = Result<grpc_db::RxResult, Status>> + Send>>;

#[tonic::async_trait]
impl<S: Storage + 'static, T: Transaction<S, S> + 'static> grpc_db::db_server::Db
    for DbService<S, T>
{
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

        spawn_blocking(move || {
            match engine.tx_as(&Principal::System { id: 1, name: "root".to_string() }, &query) {
                Ok(results) => {
                    let mut responses: Vec<Result<TxResult, Status>> = vec![];

                    for res in results {
                        let tx_result =
                            match res {
                                ExecutionResult::Query { columns: ls, rows: rs } => {
                                    let columns = ls
                                        .iter()
                                        .map(|c| grpc_db::Column { name: c.name.clone(), value: 0 })
                                        .collect();

                                    let rows = rs
                                        .iter()
                                        .map(|r| Row {
                                            values: r.iter().map(value_to_query_value).collect(),
                                        })
                                        .collect();

                                    TxResult {
                                        result: Some(grpc_db::tx_result::Result::Query(
                                            QueryResult { columns, rows },
                                        )),
                                    }
                                }
                                ExecutionResult::DescribeQuery { columns: ls } => {
                                    let columns = ls
                                        .iter()
                                        .map(|c| grpc_db::Column { name: c.name.clone(), value: 0 })
                                        .collect();

                                    TxResult {
                                        result: Some(grpc_db::tx_result::Result::Query(
                                            QueryResult { columns, rows: vec![] },
                                        )),
                                    }
                                }
                                ExecutionResult::CreateSchema(CreateSchemaResult {
                                    id,
                                    schema,
                                    created,
                                }) => TxResult {
                                    result: Some(CreateSchema(grpc_db::CreateSchema {
                                        id: id.0,
                                        schema,
                                        created,
                                    })),
                                },
                                ExecutionResult::CreateSeries { .. } => {
                                    unimplemented!()
                                }
                                ExecutionResult::CreateTable(CreateTableResult {
                                    id,
                                    schema,
                                    table,
                                    created,
                                }) => TxResult {
                                    result: Some(CreateTable(grpc_db::CreateTable {
                                        id: id.0,
                                        schema,
                                        table,
                                        created,
                                    })),
                                },
                                ExecutionResult::InsertIntoSeries { schema, series, inserted } => {
                                    TxResult {
                                        result: Some(InsertIntoSeries(grpc_db::InsertIntoSeries {
                                            schema,
                                            series,
                                            inserted: inserted as u32,
                                        })),
                                    }
                                }
                                ExecutionResult::InsertIntoTable { schema, table, inserted } => {
                                    TxResult {
                                        result: Some(InsertIntoTable(grpc_db::InsertIntoTable {
                                            schema,
                                            table,
                                            inserted: inserted as u32,
                                        })),
                                    }
                                }
                                ExecutionResult::CreateDeferredView { .. } => {
                                    unimplemented!()
                                }
                            };

                        responses.push(Ok(tx_result));
                    }

                    Ok(Response::new(Box::pin(tokio_stream::iter(responses)) as TxResultStream))
                }
                Err(err) => {
                    let diagnostic = err.diagnostic();
                    let result = TxResult {
                        result: Some(grpc_db::tx_result::Result::Error(map_diagnostic(diagnostic))),
                    };

                    Ok(Response::new(Box::pin(once(Ok(result))) as TxResultStream))
                }
            }
        })
        .await
        .unwrap()
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

        let results = spawn_blocking(move || {
            engine.rx_as(&Principal::System { id: 1, name: "root".to_string() }, &query).unwrap()
        })
        .await
        .unwrap();

        let stream = tokio_stream::iter(results.into_iter().filter_map(|res| match res {
            ExecutionResult::Query { columns: ls, rows: rs } => {
                let columns =
                    ls.iter().map(|c| grpc_db::Column { name: c.name.clone(), value: 0 }).collect();

                let rows = rs
                    .iter()
                    .map(|r| grpc_db::Row { values: r.iter().map(value_to_query_value).collect() })
                    .collect();

                Some(Ok(RxResult {
                    result: Some(grpc_db::rx_result::Result::Query(QueryResult { columns, rows })),
                }))
            }
            _ => None,
        }));

        Ok(Response::new(Box::pin(stream) as RxResultStream))
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

fn map_diagnostic(diagnostic: reifydb_diagnostic::Diagnostic) -> grpc_db::Diagnostic {
    grpc_db::Diagnostic {
        code: diagnostic.code.to_string(),
        message: diagnostic.message,
        span: diagnostic.span.map(|s| grpc_db::Span {
            offset: s.offset.0,
            line: s.line.0,
            fragment: s.fragment,
        }),
        label: diagnostic.label.unwrap_or_default(),
        help: diagnostic.help.unwrap_or_default(),
        notes: diagnostic.notes,
        column: diagnostic
            .column
            .map(|c| grpc_db::DiagnosticColumn { name: c.name, value: c.value as i32 }),
    }
}
