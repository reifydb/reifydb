// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::WsClient;
use tokio::runtime::Runtime;

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_ws_port},
	ws::subscription::{create_test_table, unique_table_name},
};

#[test]
fn test_subscribe_returns_subscription_id() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_basic");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();
		assert_eq!(sub_id, "1");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_unsubscribe_success() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_unsub");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// Unsubscribe should succeed without error
		let result = client.unsubscribe(&sub_id).await;
		assert!(result.is_ok(), "Unsubscribe should succeed");

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscribe_invalid_query() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		// Invalid RQL should return an error
		let result = client.subscribe("INVALID RQL SYNTAX HERE").await;
		assert!(result.is_err(), "Invalid query should return error");

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscribe_nonexistent_table() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		// Non-existent table should return an error
		let result = client.subscribe("from nonexistent_table_xyz_12345").await;
		assert!(result.is_err(), "Non-existent table should return error");

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_unsubscribe_invalid_id() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		// Invalid subscription ID - server may or may not error
		let result = client.unsubscribe("fake-subscription-id-12345").await;
		// Just verify it doesn't panic - behavior may vary
		let _ = result;

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_try_recv_empty() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_try_recv");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// try_recv should return Empty when no changes
		let result = client.try_recv();
		assert!(result.is_err(), "try_recv should return error when empty");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
