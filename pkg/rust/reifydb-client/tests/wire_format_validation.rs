// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(reifydb_single_threaded))]
mod tests {
	use reifydb_client::{HttpClient, WireFormat, WsClient, grpc::GrpcClient};

	#[tokio::test]
	#[cfg(feature = "http")]
	async fn test_http_rejects_proto() {
		let result = HttpClient::connect("http://localhost:8080", WireFormat::Proto).await;
		assert!(result.is_err());
		if let Err(err) = result {
			assert_eq!(err.0.code, "INVALID_FORMAT");
			assert!(err.0.message.contains("WireFormat::Proto is not supported"));
		}
	}

	#[tokio::test]
	#[cfg(feature = "ws")]
	async fn test_ws_rejects_proto() {
		let result = WsClient::connect("ws://localhost:8090", WireFormat::Proto).await;
		assert!(result.is_err());
		if let Err(err) = result {
			assert_eq!(err.0.code, "INVALID_FORMAT");
			assert!(err.0.message.contains("WireFormat::Proto is not supported"));
		}
	}

	#[tokio::test]
	#[cfg(feature = "grpc")]
	async fn test_grpc_rejects_json() {
		let result = GrpcClient::connect("http://localhost:8091", WireFormat::Json).await;
		assert!(result.is_err());
		if let Err(err) = result {
			assert_eq!(err.0.code, "INVALID_FORMAT");
			assert!(err.0.message.contains("WireFormat::Json is not supported"));
		}
	}
}
