// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::service::grpc_query;
use crate::server::service::grpc_query::query_server::Query;
use crate::server::service::grpc_query::{Column, ColumnHeader, QueryRequest, QueryResult, Row};
use base::Value;
use std::pin::Pin;
use tokio_stream::Stream;
use tokio_stream::{self as stream, StreamExt};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct QueryService {}

type QueryResultStream = Pin<Box<dyn Stream<Item = Result<QueryResult, Status>> + Send>>;

#[tonic::async_trait]
impl Query for QueryService {
    type QueryStream = QueryResultStream;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let query = request.into_inner().query;

        println!("Received query: {}", query);

        let columns = vec![
            Column { name: "id".into(), value: 1u32 },
            Column { name: "name".into(), value: 2u32 },
        ];

        let rows: Vec<Vec<grpc_query::Value>> = vec![
            vec![
                value_to_query_value(&Value::Uint2(1)),
                value_to_query_value(&Value::Text("Alice".to_string())),
            ],
            vec![
                value_to_query_value(&Value::Uint2(2)),
                value_to_query_value(&Value::Text("Bob".to_string())),
            ],
            vec![
                value_to_query_value(&Value::Uint2(3)),
                value_to_query_value(&Value::Text("Eve".to_string())),
            ],
        ];

        let header = QueryResult {
            result: Some(grpc_query::query_result::Result::Header(ColumnHeader { columns })),
        };

        let row_messages = rows.into_iter().map(|values| {
            Ok(QueryResult { result: Some(grpc_query::query_result::Result::Row(Row { values })) })
        });

        let output = stream::iter(vec![Ok(header)]).chain(stream::iter(row_messages));

        Ok(Response::new(Box::pin(output) as Self::QueryStream))
    }
}

fn value_to_query_value(value: &Value) -> grpc_query::Value {
    use grpc_query::value::Kind;

    grpc_query::Value {
        kind: Some(match value {
            Value::Bool(v) => Kind::BoolValue(*v),
            // Int1(v) => Kind::Int1Value(*v as i32),
            Value::Int2(v) => Kind::Int2Value(*v as i32),
            // Int4(v) => Kind::Int4Value(*v),
            // Int8(v) => Kind::Int8Value(*v),
            // Int16(v) => Kind::Int16Value(v.to_string()),
            // Uint1(v) => Kind::Uint1Value(*v as u32),
            Value::Uint2(v) => Kind::Uint2Value(*v as u32),
            // Uint4(v) => Kind::Uint4Value(*v),
            // Uint8(v) => Kind::Uint8Value(*v),
            // Uint16(v) => Kind::Uint16Value(v.to_string()),
            Value::Text(s) => Kind::TextValue(s.clone()),
            Value::Undefined => unimplemented!(),
        }),
    }
}
