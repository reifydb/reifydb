// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, pin::Pin, sync::Arc};

use reifydb_core::{
	Type, Value,
	interface::{
		Engine as EngineInterface, Identity, Params as CoreParams,
		Transaction,
	},
	result::{Frame, error::diagnostic::Diagnostic},
};
use reifydb_engine::StandardEngine;
use tokio::task::spawn_blocking;
use tokio_stream::{Stream, once};
use tonic::{Request, Response, Status};

use crate::grpc::server::{
	AuthenticatedUser, grpc,
	grpc::{CommandRequest, CommandResult, QueryRequest, QueryResult},
};

pub struct DbService<T: Transaction> {
	pub(crate) engine: Arc<StandardEngine<T>>,
	_phantom: std::marker::PhantomData<T>,
}

fn grpc_value_to_core_value(grpc_val: grpc::Value) -> Option<Value> {
	use grpc::value::Type as GrpcType;

	match grpc_val.r#type? {
		GrpcType::BoolValue(b) => Some(Value::Bool(b)),
		GrpcType::Float32Value(f) => Some(Value::float4(f)),
		GrpcType::Float64Value(f) => Some(Value::float8(f)),
		GrpcType::Int1Value(i) => Some(Value::Int1(i as i8)),
		GrpcType::Int2Value(i) => Some(Value::Int2(i as i16)),
		GrpcType::Int4Value(i) => Some(Value::Int4(i)),
		GrpcType::Int8Value(i) => Some(Value::Int8(i)),
		GrpcType::Int16Value(i128) => Some(Value::Int16(
			((i128.high as i128) << 64) | (i128.low as i128),
		)),
		GrpcType::Uint1Value(u) => Some(Value::Uint1(u as u8)),
		GrpcType::Uint2Value(u) => Some(Value::Uint2(u as u16)),
		GrpcType::Uint4Value(u) => Some(Value::Uint4(u)),
		GrpcType::Uint8Value(u) => Some(Value::Uint8(u)),
		GrpcType::Uint16Value(u128) => Some(Value::Uint16(
			((u128.high as u128) << 64) | (u128.low as u128),
		)),
		GrpcType::StringValue(s) => Some(Value::Utf8(s)),
		GrpcType::DateValue(d) => {
			reifydb_core::Date::from_days_since_epoch(
				d.days_since_epoch,
			)
			.map(Value::Date)
		}
		GrpcType::DatetimeValue(dt) => {
			reifydb_core::DateTime::from_parts(dt.seconds, dt.nanos)
				.ok()
				.map(Value::DateTime)
		}
		GrpcType::TimeValue(t) => {
			reifydb_core::Time::from_nanos_since_midnight(
				t.nanos_since_midnight,
			)
			.map(Value::Time)
		}
		GrpcType::IntervalValue(i) => Some(Value::Interval(
			reifydb_core::Interval::new(i.months, i.days, i.nanos),
		)),
		GrpcType::UndefinedValue(_) => Some(Value::Undefined),
		GrpcType::RowIdValue(id) => {
			Some(Value::RowId(reifydb_core::RowId::new(id)))
		}
		GrpcType::Uuid4Value(bytes) => uuid::Uuid::from_slice(&bytes)
			.ok()
			.filter(|u| u.get_version_num() == 4)
			.map(|u| Value::Uuid4(reifydb_core::Uuid4::from(u))),
		GrpcType::Uuid7Value(bytes) => uuid::Uuid::from_slice(&bytes)
			.ok()
			.filter(|u| u.get_version_num() == 7)
			.map(|u| Value::Uuid7(reifydb_core::Uuid7::from(u))),
		GrpcType::BlobValue(bytes) => {
			Some(Value::Blob(reifydb_core::Blob::new(bytes)))
		}
	}
}

fn grpc_params_to_core_params(grpc_params: Option<grpc::Params>) -> CoreParams {
	use grpc::params::Params as GrpcParamsType;

	match grpc_params.and_then(|p| p.params) {
		Some(GrpcParamsType::Positional(pos)) => {
			let values: Vec<Value> = pos
				.values
				.into_iter()
				.filter_map(grpc_value_to_core_value)
				.collect();
			CoreParams::Positional(values)
		}
		Some(GrpcParamsType::Named(named)) => {
			let mut map = HashMap::new();
			for (key, value) in named.values {
				if let Some(v) = grpc_value_to_core_value(value)
				{
					map.insert(key, v);
				}
			}
			CoreParams::Named(map)
		}
		None => CoreParams::None,
	}
}

impl<T: Transaction> DbService<T> {
	pub fn new(engine: StandardEngine<T>) -> Self {
		Self {
			engine: Arc::new(engine),
			_phantom: std::marker::PhantomData,
		}
	}
}

pub type CommandResultStream = Pin<
	Box<dyn Stream<Item = Result<grpc::CommandResult, Status>> + Send>,
>;
pub type QueryResultStream = Pin<
	Box<dyn Stream<Item = Result<grpc::QueryResult, Status>> + Send>,
>;

#[tonic::async_trait]
impl<T: Transaction> grpc::db_server::Db for DbService<T> {
	type CommandStream = CommandResultStream;

	async fn command(
		&self,
		request: Request<CommandRequest>,
	) -> Result<Response<CommandResultStream>, Status> {
		let user = request
			.extensions()
			.get::<AuthenticatedUser>()
			.ok_or_else(|| {
				tonic::Status::unauthenticated(
					"No authenticated user found",
				)
			})?;

		println!("Authenticated as: {:?}", user);

		let req = request.into_inner();
		let rql = req.statements;
		let params = grpc_params_to_core_params(req.params);
		println!("Received query: {}", rql);

		let engine = self.engine.clone();

		spawn_blocking(move || {
            match engine.command_as(&Identity::System { id: 1, name: "root".to_string() }, &rql, params)
            {
                Ok(frames) => {
                    let mut responses: Vec<Result<CommandResult, Status>> = vec![];

                    for frame in frames {
                        responses.push(Ok(CommandResult {
                            result: Some(grpc::command_result::Result::Frame(map_frame(frame))),
                        }))
                    }

                    Ok(
                        Response::new(
                            Box::pin(tokio_stream::iter(responses)) as CommandResultStream
                        ),
                    )
                }
                Err(err) => {
                    let diagnostic = err.diagnostic();
                    let result = CommandResult {
                        result: Some(grpc::command_result::Result::Error(map_diagnostic(
                            diagnostic,
                        ))),
                    };

                    Ok(Response::new(Box::pin(once(Ok(result))) as CommandResultStream))
                }
            }
        })
        .await
        .unwrap()
	}

	type QueryStream = QueryResultStream;

	async fn query(
		&self,
		request: Request<QueryRequest>,
	) -> Result<Response<Self::QueryStream>, Status> {
		let user = request
			.extensions()
			.get::<AuthenticatedUser>()
			.ok_or_else(|| {
				tonic::Status::unauthenticated(
					"No authenticated user found",
				)
			})?;

		println!("Authenticated as: {:?}", user);

		let req = request.into_inner();
		let rql = req.statements;
		let params = grpc_params_to_core_params(req.params);
		println!("Received query: {}", rql);

		let engine = self.engine.clone();

		spawn_blocking(move || {
            match engine.command_as(&Identity::System { id: 1, name: "root".to_string() }, &rql, params)
            {
                Ok(frames) => {
                    let mut responses: Vec<Result<QueryResult, Status>> = vec![];

                    for frame in frames {
                        responses.push(Ok(QueryResult {
                            result: Some(grpc::query_result::Result::Frame(map_frame(frame))),
                        }))
                    }

                    Ok(Response::new(Box::pin(tokio_stream::iter(responses)) as QueryResultStream))
                }
                Err(err) => {
                    let diagnostic = err.diagnostic();
                    let result = QueryResult {
                        result: Some(grpc::query_result::Result::Error(map_diagnostic(diagnostic))),
                    };

                    Ok(Response::new(Box::pin(once(Ok(result))) as QueryResultStream))
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
		column: diagnostic.column.map(|c| grpc::DiagnosticColumn {
			name: c.name,
			ty: c.ty as i32,
		}),
		cause: diagnostic
			.cause
			.map(|cb| Box::from(map_diagnostic(*cb))),
	}
}

fn map_frame(frame: Frame) -> grpc::Frame {
	use grpc::{
		Date, DateTime, Frame, FrameColumn, Int128, Interval, Time,
		UInt128, Value as GrpcValue, value::Type as GrpcType,
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
