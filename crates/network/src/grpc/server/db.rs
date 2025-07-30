// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::pin::Pin;
use std::sync::Arc;
use tokio::task::spawn_blocking;
use tokio_stream::{Stream, once};
use tonic::{Request, Response, Status};

use crate::grpc::server::grpc::RxResult;
use crate::grpc::server::grpc::{RxRequest, TxRequest, TxResult};
use crate::grpc::server::{AuthenticatedUser, grpc};
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, Transaction, UnversionedStorage, VersionedStorage,
};
use reifydb_core::result::Frame;
use reifydb_core::result::error::diagnostic::Diagnostic;
use reifydb_core::{Type, Value};
use reifydb_engine::Engine;

pub struct DbService<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub(crate) engine: Arc<Engine<VS, US, T>>,
    _phantom: std::marker::PhantomData<(VS, US, T)>,
}

impl<VS, US, T> DbService<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(engine: Engine<VS, US, T>) -> Self {
        Self { engine: Arc::new(engine), _phantom: std::marker::PhantomData }
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
            match engine.tx_as(&Principal::System { id: 1, name: "root".to_string() }, &query) {
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
            match engine.tx_as(&Principal::System { id: 1, name: "root".to_string() }, &query) {
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
            offset: s.column.0,
            line: s.line.0,
            fragment: s.fragment,
        }),
        label: diagnostic.label,
        help: diagnostic.help,
        notes: diagnostic.notes,
        column: diagnostic.column.map(|c| grpc::DiagnosticColumn { name: c.name, ty: c.ty as i32 }),
        cause: diagnostic.cause.map(|cb| Box::from(map_diagnostic(*cb))),
    }
}

fn map_frame(frame: Frame) -> grpc::Frame {
    use grpc::{
        Date, DateTime, Frame, FrameColumn, Int128, Interval, Time, UInt128, Value as GrpcValue,
        value::Type as GrpcType,
    };

    Frame {
        columns: frame
            .into_iter()
            .map(|col| {
                let data_type = col.get_type();

                let data = col
                    .iter()
                    .map(|v| {
                        let data_type = match v {
                            Value::Bool(b) => GrpcType::BoolValue(b),
                            Value::Float4(f) => GrpcType::Float32Value(f.value()),
                            Value::Float8(f) => GrpcType::Float64Value(f.value()),
                            Value::Int1(i) => GrpcType::Int1Value(i as i32),
                            Value::Int2(i) => GrpcType::Int2Value(i as i32),
                            Value::Int4(i) => GrpcType::Int4Value(i),
                            Value::Int8(i) => GrpcType::Int8Value(i),
                            Value::Int16(i) => GrpcType::Int16Value(Int128 {
                                high: (i >> 64) as u64,
                                low: i as u64,
                            }),
                            Value::Uint1(i) => GrpcType::Uint1Value(i as u32),
                            Value::Uint2(i) => GrpcType::Uint2Value(i as u32),
                            Value::Uint4(i) => GrpcType::Uint4Value(i),
                            Value::Uint8(i) => GrpcType::Uint8Value(i),
                            Value::Uint16(i) => GrpcType::Uint16Value(UInt128 {
                                high: (i >> 64) as u64,
                                low: i as u64,
                            }),
                            Value::Utf8(s) => GrpcType::StringValue(s.clone()),
                            Value::Date(d) => GrpcType::DateValue(Date {
                                days_since_epoch: d.to_days_since_epoch(),
                            }),
                            Value::DateTime(dt) => {
                                let (seconds, nanos) = dt.to_parts();
                                GrpcType::DatetimeValue(DateTime { seconds, nanos })
                            }
                            Value::Time(t) => GrpcType::TimeValue(Time {
                                nanos_since_midnight: t.to_nanos_since_midnight(),
                            }),
                            Value::Interval(i) => GrpcType::IntervalValue(Interval {
                                months: i.get_months(),
                                days: i.get_days(),
                                nanos: i.get_nanos(),
                            }),
                            Value::Undefined => GrpcType::UndefinedValue(false),
                            Value::RowId(row_id) => GrpcType::RowIdValue(row_id.value()),
                            Value::Uuid4(uuid) => GrpcType::Uuid4Value(uuid.as_bytes().to_vec()),
                            Value::Uuid7(uuid) => GrpcType::Uuid7Value(uuid.as_bytes().to_vec()),
                            Value::Blob(blob) => GrpcType::BlobValue(blob.as_bytes().to_vec()),
                        };
                        GrpcValue { r#type: Some(data_type) }
                    })
                    .collect();

                FrameColumn {
                    name: col.name.to_string(),
                    ty: Type::to_u8(&data_type) as i32,
                    frame: col.table.as_ref().map(|s| s.to_string()),
                    data,
                }
            })
            .collect(),
    }
}
