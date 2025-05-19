// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub(crate) mod grpc_query {
    tonic::include_proto!("grpc_query");
}

use crate::client::grpc_query::QueryRequest;
use crate::client::grpc_query::query_client::QueryClient;
use crate::client::grpc_query::value::Kind;
use base::Value;
use engine::execute::ExecutionResult;
use grpc_query::{Column, QueryResult, Row};
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use tonic::codegen::tokio_stream::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::{Status, Streaming};

pub struct Table {
    pub columns: Vec<Column>,
    pub rows: RowStream,
}

pub struct RowStream {
    stream: InternalRowStream,
}

impl RowStream {
    pub fn new(stream: InternalRowStream) -> Self {
        Self { stream }
    }

    /// Reads the next row from the stream asynchronously.
    pub async fn next(&mut self) -> Result<Option<Vec<Value>>, Status> {
        while let Some(msg) = self.stream.next().await {
            match msg? {
                values => return Ok(Some(values)),
            }
        }
        Ok(None)
    }
}

pub struct InternalRowStream {
    inner: Streaming<QueryResult>,
}

impl Stream for InternalRowStream {
    type Item = Result<Vec<Value>, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = Pin::new(&mut self.as_mut().get_mut().inner);

        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => match msg.result {
                Some(grpc_query::query_result::Result::Row(Row { values })) => {
                    let values = values
                        .into_iter()
                        .map(|v| match v.kind.unwrap() {
                            Kind::BoolValue(v) => Value::Bool(v),
                            Kind::Int2Value(v) => Value::Int2(v as i16),
                            Kind::Uint2Value(v) => Value::Uint2(v as u16),
                            Kind::TextValue(v) => Value::Text(v),
                            _ => unimplemented!(),
                        })
                        .collect();

                    Poll::Ready(Some(Ok(values)))
                }
                Some(grpc_query::query_result::Result::Error(e)) => {
                    Poll::Ready(Some(Err(tonic::Status::internal(e))))
                }
                Some(grpc_query::query_result::Result::Header(_)) => {
                    // headers should already be consumed
                    self.poll_next(cx)
                }
                None => Poll::Ready(None),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub async fn parse_table(mut stream: Streaming<QueryResult>) -> Result<Table, tonic::Status> {
    while let Some(msg) = stream.message().await? {
        if let Some(grpc_query::query_result::Result::Header(header)) = msg.result {
            return Ok(Table {
                columns: header.columns,
                rows: RowStream::new(InternalRowStream { inner: stream }),
            });
        }
    }

    Err(tonic::Status::invalid_argument("No header received"))
}

pub struct Client {}

impl Client {
    pub async fn internal_query(&self, query: &str) -> Table {
        let mut client = QueryClient::connect("http://[::1]:4321").await.unwrap();

        let mut request = tonic::Request::new(QueryRequest { query: query.into() });
        let token = MetadataValue::from_str("Bearer mysecrettoken").unwrap();
        request.metadata_mut().insert("authorization", token);

        let stream = client.query(request).await.unwrap().into_inner();
        let table = parse_table(stream).await.unwrap();
        table
    }

    pub async fn rx_execute(&self, query: &str) -> Vec<ExecutionResult> {
        vec![]
    }

    pub async fn tx_execute(&self, query: &str) -> Vec<ExecutionResult> {
        vec![]
    }
}
