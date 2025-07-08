// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::pin::Pin;
use std::sync::Arc;
use tokio::task::spawn_blocking;
use tokio_stream::{Stream, once};
use tonic::{Request, Response, Status};

use crate::grpc::server::grpc::RxResult;
use crate::grpc::server::grpc::{RxRequest, TxRequest, TxResult};
use crate::grpc::server::{AuthenticatedUser, grpc};
use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_core::{Diagnostic, Kind, Value};
use reifydb_engine::Engine;
use reifydb_engine::frame::Frame;

pub struct DbService<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub(crate) engine: Arc<Engine<VS, US, T>>,
}

impl<VS, US, T> DbService<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(engine: Engine<VS, US, T>) -> Self {
        Self { engine: Arc::new(engine) }
    }
}

pub type TxResultStream = Pin<Box<dyn Stream<Item = Result<grpc::TxResult, Status>> + Send>>;
pub type RxResultStream = Pin<Box<dyn Stream<Item = Result<grpc::RxResult, Status>> + Send>>;

#[tonic::async_trait]
impl<VS, US, T> grpc::db_server::Db for DbService<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
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
            match engine.tx_as(&Principal::System { id: 1, name: "root".to_string() }, &query)
            {
                Ok(frames) => {
                    let mut responses: Vec<Result<TxResult, Status>> = vec![];

                    for frame in frames {
                        responses.push(Ok(TxResult {
                            result: Some(grpc::tx_result::Result::Frame(map_frame(frame))),
                        }))
                    }

                    Ok(Response::new(Box::pin(tokio_stream::iter(responses)) as TxResultStream))
                }
                Err(err) => {
                    let diagnostic = err.diagnostic();
                    let result = TxResult {
                        result: Some(grpc::tx_result::Result::Error(map_diagnostic(diagnostic))),
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

        spawn_blocking(move || {
            match engine.tx_as(&Principal::System { id: 1, name: "root".to_string() }, &query)
            {
                Ok(frames) => {
                    let mut responses: Vec<Result<RxResult, Status>> = vec![];

                    for frame in frames {
                        responses.push(Ok(RxResult {
                            result: Some(grpc::rx_result::Result::Frame(map_frame(frame))),
                        }))
                    }

                    Ok(Response::new(Box::pin(tokio_stream::iter(responses)) as RxResultStream))
                }
                Err(err) => {
                    let diagnostic = err.diagnostic();
                    let result = RxResult {
                        result: Some(grpc::rx_result::Result::Error(map_diagnostic(diagnostic))),
                    };

                    Ok(Response::new(Box::pin(once(Ok(result))) as RxResultStream))
                }
            }
        })
        .await
        .unwrap()
    }
}

fn map_diagnostic(diagnostic: Diagnostic) -> grpc::Diagnostic {
    grpc::Diagnostic {
        code: diagnostic.code.to_string(),
        statement: diagnostic.statement,
        message: diagnostic.message,
        span: diagnostic.span.map(|s| grpc::Span {
            offset: s.offset.0,
            line: s.line.0,
            fragment: s.fragment,
        }),
        label: diagnostic.label,
        help: diagnostic.help,
        notes: diagnostic.notes,
        column: diagnostic
            .column
            .map(|c| grpc::DiagnosticColumn { name: c.name, kind: c.value as i32 }),
    }
}

fn map_frame(frame: Frame) -> grpc::Frame {
    use grpc::{Column, Frame, Int128, UInt128, Value as GrpcValue, value::Kind as GrpcKind};

    Frame {
        name: frame.name,
        columns: frame
            .columns
            .into_iter()
            .map(|col| {
                let kind = col.values.kind();

                let values = col
                    .values
                    .iter()
                    .map(|v| {
                        let kind = match v {
                            Value::Bool(b) => GrpcKind::BoolValue(b),
                            Value::Float4(f) => GrpcKind::Float32Value(f.value()),
                            Value::Float8(f) => GrpcKind::Float64Value(f.value()),
                            Value::Int1(i) => GrpcKind::Int1Value(i as i32),
                            Value::Int2(i) => GrpcKind::Int2Value(i as i32),
                            Value::Int4(i) => GrpcKind::Int4Value(i),
                            Value::Int8(i) => GrpcKind::Int8Value(i),
                            Value::Int16(i) => GrpcKind::Int16Value(Int128 {
                                high: (i >> 64) as u64,
                                low: i as u64,
                            }),
                            Value::Uint1(i) => GrpcKind::Uint1Value(i as u32),
                            Value::Uint2(i) => GrpcKind::Uint2Value(i as u32),
                            Value::Uint4(i) => GrpcKind::Uint4Value(i),
                            Value::Uint8(i) => GrpcKind::Uint8Value(i),
                            Value::Uint16(i) => GrpcKind::Uint16Value(UInt128 {
                                high: (i >> 64) as u64,
                                low: i as u64,
                            }),
                            Value::String(s) => GrpcKind::StringValue(s.clone()),
                            Value::Undefined => GrpcKind::UndefinedValue(false),
                        };
                        GrpcValue { kind: Some(kind) }
                    })
                    .collect();

                Column { name: col.name, kind: Kind::to_u8(&kind) as i32, values }
            })
            .collect(),
    }
}
