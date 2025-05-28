// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::ordered_float::OrderedF64;
use reifydb_core::{Value, ValueKind};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};
use tonic::Streaming;
use tonic::metadata::MetadataValue;
use reifydb_engine::{Column, ExecutionResult};

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
                            .map(|v| match v.kind.unwrap() {
                                grpc_db::value::Kind::BoolValue(b) => Value::Bool(b),
                                grpc_db::value::Kind::Float64Value(f) => OrderedF64::try_from(f)
                                    .ok()
                                    .map(|v| Value::Float8(v))
                                    .unwrap_or(Value::Undefined),
                                grpc_db::value::Kind::Int2Value(i) => Value::Int2(i as i16),
                                grpc_db::value::Kind::Uint2Value(u) => Value::Uint2(u as u16),
                                grpc_db::value::Kind::TextValue(t) => Value::Text(t),
                                _ => unimplemented!("Value kind not yet supported"),
                            })
                            .collect()
                    })
                    .collect();

                return Ok(ExecutionResult::Query { columns: labels, rows });
            }
            Some(grpc_db::rx_result::Result::Error(e)) => {
                return Err(tonic::Status::internal(e));
            }
            None => {
                return Err(tonic::Status::internal("empty rx_result"));
            }
        }
    }

    Err(tonic::Status::internal("no rx_result received"))
}

impl Client {
    pub async fn rx(&self, query: &str) -> Vec<ExecutionResult> {
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
        vec![result]
    }

    pub async fn tx(&self, query: &str) -> Vec<ExecutionResult> {
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
                Some(CreateSchema(cs)) => ExecutionResult::CreateSchema { schema: cs.schema },
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
                                .map(|v| match v.kind.unwrap() {
                                    grpc_db::value::Kind::BoolValue(b) => Value::Bool(b),
                                    grpc_db::value::Kind::Float64Value(f) => {
                                        OrderedF64::try_from(f)
                                            .ok()
                                            .map(|v| Value::Float8(v))
                                            .unwrap_or(Value::Undefined)
                                    }
                                    grpc_db::value::Kind::Int2Value(i) => Value::Int2(i as i16),
                                    grpc_db::value::Kind::Uint2Value(u) => Value::Uint2(u as u16),
                                    grpc_db::value::Kind::TextValue(t) => Value::Text(t),
                                    _ => unimplemented!("Unhandled value kind"),
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

        results
    }
}
