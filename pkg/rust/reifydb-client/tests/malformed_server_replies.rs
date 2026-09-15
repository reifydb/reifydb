// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{HttpClient, WireFormat};
use rustls::crypto::ring::default_provider;
use tokio::{net::TcpListener, spawn};

#[tokio::test]
async fn an_http_request_to_a_closed_port_is_an_error_not_a_panic() {
	// A transport failure is the caller's to handle, so it must come back as an error, not abort the task.
	let _ = default_provider().install_default();
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	drop(listener);
	let client = HttpClient::connect(&format!("http://{address}"), WireFormat::Frames).await.unwrap();

	let result = spawn(async move { client.query("from test::t", None).await.map(|frames| frames.len()) }).await;

	assert!(matches!(result, Ok(Err(_))), "expected the request to fail, got {result:?}");
}
