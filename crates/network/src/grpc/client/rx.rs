// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use crate::grpc::client::convert::{convert_diagnostic, convert_value};
use crate::grpc::client::grpc_db::rx_result;
use crate::grpc::client::{Client, grpc_db, wait_for_socket};
use reifydb_core::{Error, Kind};
use reifydb_engine::{Column, ExecutionResult};
use std::str::FromStr;
use std::time::Duration;
use tonic::metadata::MetadataValue;

impl Client {
    pub async fn rx(&self, query: &str) -> Result<Vec<ExecutionResult>, Error> {
        // FIXME this is quite expensive and should only used in tests
        // add a server.on_ready(||{ signal_server_read() } and use it for tests instead

        wait_for_socket(&self.socket_addr, Duration::from_millis(500)).await?;
        let uri = format!("http://{}", self.socket_addr);
        let mut client = grpc_db::db_client::DbClient::connect(uri).await.unwrap();

        let mut request = tonic::Request::new(grpc_db::RxRequest { query: query.into() });

        request
            .metadata_mut()
            .insert("authorization", MetadataValue::from_str("Bearer mysecrettoken").unwrap());

        let mut results = Vec::new();

        let mut stream = client.rx(request).await.unwrap().into_inner();
        while let Some(msg) = stream.message().await.unwrap() {
            if let Some(result) = msg.result {
                results.push(convert_result(result, query)?);
            }
        }
        Ok(results)
    }
}

fn convert_result(result: rx_result::Result, query: &str) -> Result<ExecutionResult, Error> {
    Ok(match result {
        rx_result::Result::Error(diagnostic) => {
            // return Err(crate::Error::execution_error(query, convert_diagnostic(diagnostic)));
            return Err(Error(convert_diagnostic(diagnostic)));
        }
        rx_result::Result::Query(query) => {
            let labels = query
                .columns
                .into_iter()
                .map(|c| Column { name: c.name, kind: Kind::Bool })
                .collect();

            let rows = query
                .rows
                .into_iter()
                .map(|r| r.values.into_iter().map(convert_value).collect())
                .collect();

            ExecutionResult::Query { columns: labels, rows }
        }
    })
}
