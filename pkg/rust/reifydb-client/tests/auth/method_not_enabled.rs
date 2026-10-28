// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_client::{GrpcClient, WireFormat, WsClient};
use serde_json::{Value as JsonValue, json};
use tokio::runtime::Runtime;

use crate::{
	auth::start_server_with_auth_users,
	common::{cleanup_server, create_server_instance},
};

fn solana_credentials() -> HashMap<String, String> {
	HashMap::from([("identifier".to_string(), "key".to_string()), ("public_key".to_string(), "key".to_string())])
}

#[test]
fn an_undeclared_method_is_refused_distinctly_on_every_transport() {
	// Apps must tell "this login is switched off" from "wrong credentials" on whichever transport they use.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (ws_port, grpc_port, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let response = reqwest::Client::new()
			.post(format!("http://[::1]:{}/v1/authenticate", http_port))
			.json(&json!({ "method": "solana", "credentials": solana_credentials() }))
			.send()
			.await
			.unwrap();
		assert_eq!(response.status().as_u16(), 403);
		let body: JsonValue = response.json().await.unwrap();
		assert_eq!(body["status"], "method_not_enabled", "{body}");
		assert_eq!(body["reason"], "authentication method 'solana' is not enabled", "{body}");

		let mut ws = WsClient::connect(&format!("ws://[::1]:{}", ws_port), WireFormat::Frames).await.unwrap();
		let ws_err = ws.login("solana", solana_credentials()).await.unwrap_err().to_string();
		assert!(ws_err.contains("AUTH_METHOD_NOT_ENABLED"), "{ws_err}");

		let mut grpc =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), WireFormat::Rbcf).await.unwrap();
		let grpc_err = grpc.login("solana", solana_credentials()).await.unwrap_err().to_string();
		assert!(grpc_err.contains("authentication method 'solana' is not enabled"), "{grpc_err}");
	});

	cleanup_server(Some(server));
}
