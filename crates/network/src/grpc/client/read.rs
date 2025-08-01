// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use crate::grpc::client::convert::{convert_diagnostic, convert_frame};
use crate::grpc::client::{GrpcClient, grpc};
use grpc::read_result::Result as ReadResultEnum;
use reifydb_core::Error;
use reifydb_core::error;
use reifydb_core::result::Frame;
use reifydb_core::result::error::diagnostic::network;
use std::str::FromStr;
use tonic::metadata::MetadataValue;

impl GrpcClient {
    pub async fn read(&self, query: &str) -> Result<Vec<Frame>, Error> {
        let uri = format!("http://{}", self.socket_addr);
        let mut client = grpc::db_client::DbClient::connect(uri)
            .await
            .map_err(|e| error!(network::transport_error(e)))?;

        let mut request = tonic::Request::new(grpc::ReadRequest { query: query.into() });

        request
            .metadata_mut()
            .insert("authorization", MetadataValue::from_str("Bearer mysecrettoken").unwrap());

        let mut results = Vec::new();

        let mut stream =
            client.read(request).await.map_err(|e| error!(network::status_error(e)))?.into_inner();
        while let Some(msg) = stream.message().await.unwrap() {
            if let Some(result) = msg.result {
                results.push(convert_result(result, query)?);
            }
        }
        Ok(results)
    }
}

pub fn convert_result(result: ReadResultEnum, query: &str) -> Result<Frame, Error> {
    match result {
        ReadResultEnum::Error(diagnostic) => {
            let mut diag = convert_diagnostic(diagnostic);
            diag.set_statement(query.to_string());
            Err(error!(diag))
        }
        ReadResultEnum::Frame(grpc_frame) => Ok(convert_frame(grpc_frame)),
    }
}
