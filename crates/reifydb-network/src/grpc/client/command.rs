// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::str::FromStr;

use reifydb_core::{
	Error, error,
	interface::Params,
	result::{Frame, error::diagnostic::network},
};
use tonic::metadata::MetadataValue;

use crate::grpc::client::{
	GrpcClient,
	convert::{convert_diagnostic, convert_frame},
	grpc,
	grpc::command_result,
};

impl GrpcClient {
	pub async fn command(
		&self,
		statements: &str,
		params: Params,
	) -> Result<Vec<Frame>, Error> {
		let uri = format!("http://{}", self.socket_addr);
		let mut client = grpc::db_client::DbClient::connect(uri)
			.await
			.map_err(|e| error!(network::transport_error(e)))?;
		let mut request = tonic::Request::new(grpc::CommandRequest {
            statements: statements.into(),
            params: crate::grpc::client::convert::core_params_to_grpc_params(params),
        });

		request.metadata_mut().insert(
			"authorization",
			MetadataValue::from_str("Bearer mysecrettoken")
				.unwrap(),
		);

		let mut stream = client
			.command(request)
			.await
			.map_err(|e| error!(network::status_error(e)))?
			.into_inner();

		let mut results = Vec::new();
		while let Some(msg) = stream.message().await.unwrap() {
			if let Some(result) = msg.result {
				results.push(convert_result(
					result, statements,
				)?);
			}
		}
		Ok(results)
	}
}

pub fn convert_result(
	result: command_result::Result,
	query: &str,
) -> Result<Frame, Error> {
	match result {
		command_result::Result::Error(diagnostic) => {
			let mut diag = convert_diagnostic(diagnostic);
			diag.set_statement(query.to_string());
			Err(error!(diag))
		}
		command_result::Result::Frame(grpc_frame) => {
			Ok(convert_frame(grpc_frame))
		}
	}
}
