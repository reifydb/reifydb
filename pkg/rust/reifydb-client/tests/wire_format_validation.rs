// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(reifydb_single_threaded))]
mod tests {
	use reifydb_client::{WireFormat, grpc::GrpcClient};

	#[tokio::test]
	#[cfg(feature = "grpc")]
	async fn test_grpc_rejects_json() {
		let result = GrpcClient::connect("http://localhost:8091", WireFormat::Frames).await;
		assert!(result.is_err());
		if let Err(err) = result {
			assert_eq!(err.0.code, "INVALID_FORMAT");
			assert!(err.0.message.contains("WireFormat::Frames is not supported"));
		}
	}
}
