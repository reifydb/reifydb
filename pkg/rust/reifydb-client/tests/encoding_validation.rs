// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(reifydb_single_threaded))]
mod tests {
	use reifydb_client::{Encoding, HttpClient, WsClient, grpc::GrpcClient};

	#[tokio::test]
	#[cfg(feature = "http")]
	async fn test_http_rejects_proto() {
		let result = HttpClient::connect("http://localhost:8080", Encoding::Proto).await;
		assert!(result.is_err());
		if let Err(err) = result {
			assert_eq!(err.0.code, "INVALID_ENCODING");
			assert!(err.0.message.contains("Encoding::Proto is not supported"));
		}
	}

	#[tokio::test]
	#[cfg(feature = "ws")]
	async fn test_ws_rejects_proto() {
		let result = WsClient::connect("ws://localhost:8090", Encoding::Proto).await;
		assert!(result.is_err());
		if let Err(err) = result {
			assert_eq!(err.0.code, "INVALID_ENCODING");
			assert!(err.0.message.contains("Encoding::Proto is not supported"));
		}
	}

	#[tokio::test]
	#[cfg(feature = "grpc")]
	async fn test_grpc_rejects_json() {
		let result = GrpcClient::connect("http://localhost:8091", Encoding::Json).await;
		assert!(result.is_err());
		if let Err(err) = result {
			assert_eq!(err.0.code, "INVALID_ENCODING");
			assert!(err.0.message.contains("Encoding::Json is not supported"));
		}
	}
}
