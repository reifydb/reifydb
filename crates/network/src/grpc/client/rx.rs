// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use crate::error::NetworkError;
use crate::grpc::client::convert::{convert_diagnostic, convert_frame};
use crate::grpc::client::{Client, grpc, wait_for_socket};
use grpc::rx_result::Result as RxResultEnum;
use reifydb_engine::frame::Frame;
use std::str::FromStr;
use std::time::Duration;
use tonic::metadata::MetadataValue;

impl Client {
    pub async fn rx(&self, query: &str) -> Result<Vec<Frame>, NetworkError> {
        // FIXME this is quite expensive and should only used in tests
        // add a server.on_ready(||{ signal_server_read() } and use it for tests instead

        wait_for_socket(&self.socket_addr, Duration::from_millis(500)).await?;
        let uri = format!("http://{}", self.socket_addr);
        let mut client = grpc::db_client::DbClient::connect(uri).await?;

        let mut request = tonic::Request::new(grpc::RxRequest { query: query.into() });

        request
            .metadata_mut()
            .insert("authorization", MetadataValue::from_str("Bearer mysecrettoken").unwrap());

        let mut results = Vec::new();

        let mut stream = client.rx(request).await?.into_inner();
        while let Some(msg) = stream.message().await.unwrap() {
            if let Some(result) = msg.result {
                results.push(convert_result(result, query)?);
            }
        }
        Ok(results)
    }
}

pub fn convert_result(result: RxResultEnum, query: &str) -> Result<Frame, NetworkError> {
    match result {
        RxResultEnum::Error(diagnostic) => {
            let mut diag = convert_diagnostic(diagnostic);
            diag.statement = Some(query.to_string());
            Err(NetworkError::execution_error(diag))
        }
        RxResultEnum::Frame(grpc_frame) => Ok(convert_frame(grpc_frame)),
    }
}
