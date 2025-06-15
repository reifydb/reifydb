// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::ordered_float::{OrderedF32, OrderedF64};
use reifydb_core::{Value, ValueKind};
use reifydb_engine::{Column, ExecutionResult};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};
use tonic::Streaming;
use tonic::metadata::MetadataValue;

pub(crate) mod grpc_db {
    tonic::include_proto!("grpc_db");
}

// FIXME 1ms is a little bit little for production - only for testing for now
async fn wait_for_socket(addr: &SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match TcpStream::connect(addr).await {
            // connection succeeded, server is ready
            Ok(_) => return,
            Err(_) => sleep(Duration::from_millis(1)).await,
        }
    }
    panic!("Timed out waiting for server to start at {}", addr);
}

pub struct Client {
    pub socket_addr: SocketAddr,
}

pub async fn parse_rx_query_result(
    mut stream: Streaming<grpc_db::RxResult>,
) -> Result<ExecutionResult, tonic::Status> {
    while let Some(msg) = stream.message().await? {
        match msg.result {
            Some(grpc_db::rx_result::Result::Query(query)) => {
                let labels = query
                    .columns
                    .into_iter()
                    .map(|c| Column { value: ValueKind::Bool, name: c.name })
                    .collect();

                let rows = query
                    .rows
                    .into_iter()
                    .map(|r| {
                        r.values
                            .into_iter()
                            .map(|v| match v.kind.unwrap_or_else(|| panic!("Missing value kind")) {
                                grpc_db::value::Kind::BoolValue(b) => Value::Bool(b),
                                grpc_db::value::Kind::Float32Value(f) => OrderedF32::try_from(f)
                                    .ok()
                                    .map(Value::Float4)
                                    .unwrap_or(Value::Undefined),
                                grpc_db::value::Kind::Float64Value(f) => OrderedF64::try_from(f)
                                    .ok()
                                    .map(Value::Float8)
                                    .unwrap_or(Value::Undefined),
                                grpc_db::value::Kind::Int1Value(i) => Value::Int1(i as i8),
                                grpc_db::value::Kind::Int2Value(i) => Value::Int2(i as i16),
                                grpc_db::value::Kind::Int4Value(i) => Value::Int4(i),
                                grpc_db::value::Kind::Int8Value(i) => Value::Int8(i),
                                grpc_db::value::Kind::Int16Value(i) => {
                                    Value::Int16(((i.high as i128) << 64) | i.low as i128)
                                }

                                grpc_db::value::Kind::Uint1Value(u) => Value::Uint1(u as u8),
                                grpc_db::value::Kind::Uint2Value(u) => Value::Uint2(u as u16),
                                grpc_db::value::Kind::Uint4Value(u) => Value::Uint4(u),
                                grpc_db::value::Kind::Uint8Value(u) => Value::Uint8(u),
                                grpc_db::value::Kind::Uint16Value(u) => {
                                    Value::Uint16(((u.high as u128) << 64) | u.low as u128)
                                }

                                grpc_db::value::Kind::StringValue(s) => Value::String(s),
                                grpc_db::value::Kind::UndefinedValue(_) => Value::Undefined,

                                kind => unimplemented!("Value kind {:?} not yet supported", kind),
                            })
                            .collect()
                    })
                    .collect();

                return Ok(ExecutionResult::Query { columns: labels, rows });
            }
            Some(grpc_db::rx_result::Result::Error(e)) => {
                // return Err(tonic::Status::internal(e));
                unimplemented!()
            }
            None => {
                return Err(tonic::Status::internal("empty rx_result"));
            }
        }
    }

    Err(tonic::Status::internal("no rx_result received"))
}

impl Client {
    pub async fn rx(&self, query: &str) -> crate::Result<Vec<ExecutionResult>> {
        // FIXME this is quite expensive and should only used in tests
        // add a server.on_ready(||{ signal_server_read() } and use it for tests instead

        wait_for_socket(&self.socket_addr, Duration::from_millis(500)).await;
        let uri = format!("http://{}", self.socket_addr);
        let mut client = grpc_db::db_client::DbClient::connect(uri).await.unwrap();

        let mut request = tonic::Request::new(grpc_db::RxRequest { query: query.into() });

        request
            .metadata_mut()
            .insert("authorization", MetadataValue::from_str("Bearer mysecrettoken").unwrap());

        let stream = client.rx(request).await.unwrap().into_inner();
        let result = parse_rx_query_result(stream).await.unwrap();

        Ok(vec![result])
    }

    pub async fn tx(&self, query: &str) -> crate::Result<Vec<ExecutionResult>> {
        // FIXME this is quite expensive and should only used in tests
        // add a server.on_ready(||{ signal_server_read() } and use it for tests instead
        wait_for_socket(&self.socket_addr, Duration::from_millis(500)).await;
        let uri = format!("http://{}", self.socket_addr);
        let mut client = grpc_db::db_client::DbClient::connect(uri).await.unwrap();

        let mut request = tonic::Request::new(grpc_db::TxRequest { query: query.into() });

        request
            .metadata_mut()
            .insert("authorization", MetadataValue::from_str("Bearer mysecrettoken").unwrap());

        let mut stream = client.tx(request).await.unwrap().into_inner();

        let mut results = vec![];

        while let Some(msg) = stream.message().await.unwrap() {
            use grpc_db::tx_result::Result::*;

            let result = match msg.result {
                Some(Error(diagnostic)) => {
                    return Err(crate::Error {
                        diagnostic: unmap_diagnostic(diagnostic),
                        source: query.to_string(),
                    });
                }
                Some(CreateSchema(cs)) => {
                    ExecutionResult::CreateSchema { schema: cs.schema, created: cs.created }
                }
                Some(CreateTable(ct)) => {
                    ExecutionResult::CreateTable { schema: ct.schema, table: ct.table }
                }
                Some(InsertIntoTable(insert)) => ExecutionResult::InsertIntoTable {
                    schema: insert.schema,
                    table: insert.table,
                    inserted: insert.inserted as usize,
                },
                Some(Query(query)) => {
                    let labels = query
                        .columns
                        .into_iter()
                        .map(|c| Column { name: c.name, value: ValueKind::Bool })
                        .collect();

                    let rows = query
                        .rows
                        .into_iter()
                        .map(|r| {
                            r.values
                                .into_iter()
                                .map(|v| {
                                    match v.kind.unwrap_or_else(|| panic!("Missing value kind")) {
                                        grpc_db::value::Kind::BoolValue(b) => Value::Bool(b),
                                        grpc_db::value::Kind::Float32Value(f) => {
                                            OrderedF32::try_from(f)
                                                .ok()
                                                .map(Value::Float4)
                                                .unwrap_or(Value::Undefined)
                                        }
                                        grpc_db::value::Kind::Float64Value(f) => {
                                            OrderedF64::try_from(f)
                                                .ok()
                                                .map(Value::Float8)
                                                .unwrap_or(Value::Undefined)
                                        }
                                        grpc_db::value::Kind::Int1Value(i) => Value::Int1(i as i8),
                                        grpc_db::value::Kind::Int2Value(i) => Value::Int2(i as i16),
                                        grpc_db::value::Kind::Int4Value(i) => Value::Int4(i),
                                        grpc_db::value::Kind::Int8Value(i) => Value::Int8(i),
                                        grpc_db::value::Kind::Int16Value(i) => {
                                            Value::Int16(((i.high as i128) << 64) | i.low as i128)
                                        }

                                        grpc_db::value::Kind::Uint1Value(u) => {
                                            Value::Uint1(u as u8)
                                        }
                                        grpc_db::value::Kind::Uint2Value(u) => {
                                            Value::Uint2(u as u16)
                                        }
                                        grpc_db::value::Kind::Uint4Value(u) => Value::Uint4(u),
                                        grpc_db::value::Kind::Uint8Value(u) => Value::Uint8(u),
                                        grpc_db::value::Kind::Uint16Value(u) => {
                                            Value::Uint16(((u.high as u128) << 64) | u.low as u128)
                                        }

                                        grpc_db::value::Kind::StringValue(s) => Value::String(s),
                                        grpc_db::value::Kind::UndefinedValue(_) => Value::Undefined,

                                        kind => unimplemented!(
                                            "Value kind {:?} not yet supported",
                                            kind
                                        ),
                                    }
                                })
                                .collect()
                        })
                        .collect();

                    ExecutionResult::Query { columns: labels, rows }
                }

                // Some(Error(e)) => return Err(tonic::Status::internal(e)),
                // None => return Err(tonic::Status::internal("empty tx_result")),
                _ => unimplemented!("Unhandled value kind"),
            };

            results.push(result);
        }

        Ok(results)
    }
}

fn unmap_diagnostic(grpc: grpc_db::Diagnostic) -> reifydb_diagnostic::Diagnostic {
    reifydb_diagnostic::Diagnostic {
        code: grpc.code,
        message: grpc.message,
        span: grpc.span.map(|s| reifydb_diagnostic::Span {
            offset: reifydb_diagnostic::Offset(s.offset),
            line: reifydb_diagnostic::Line(s.line),
            fragment: s.fragment,
        }),
        label: if grpc.label.is_empty() { None } else { Some(grpc.label) },
        help: if grpc.help.is_empty() { None } else { Some(grpc.help) },
        notes: grpc.notes,
        column: grpc.column.map(|c| reifydb_diagnostic::DiagnosticColumn {
            name: c.name,
            value: match c.value {
                0 => ValueKind::Bool,
                1 => ValueKind::Float4,
                2 => ValueKind::Float8,
                3 => ValueKind::Int1,
                4 => ValueKind::Int2,
                5 => ValueKind::Int4,
                6 => ValueKind::Int8,
                7 => ValueKind::Int16,
                8 => ValueKind::String,
                9 => ValueKind::Uint1,
                10 => ValueKind::Uint2,
                11 => ValueKind::Uint4,
                12 => ValueKind::Uint8,
                13 => ValueKind::Uint16,
                14 => ValueKind::Undefined,
                _ => ValueKind::Undefined,
            },
        }),
    }
}
