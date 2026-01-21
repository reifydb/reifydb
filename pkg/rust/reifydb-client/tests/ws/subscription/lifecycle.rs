// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::common::{cleanup_server, create_server_instance, start_server_and_get_ws_port};
use crate::ws::subscription::{create_test_table, recv_with_timeout, unique_table_name};
use reifydb_client::WsClient;
use tokio::runtime::Runtime;

#[test]
fn test_no_changes_after_unsubscribe() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_after_unsub");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();

		// Unsubscribe
		client.unsubscribe(&sub_id).await.unwrap();

		// Insert data after unsubscribe
		client.command(&format!("from [{{ id: 1 }}] insert test.{}", table), None).await.unwrap();

		// Should NOT receive any change
		let change = recv_with_timeout(&mut client, 500).await;
		assert!(change.is_none(), "Should NOT receive changes after unsubscribe");

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_close_cleans_up_subscriptions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_close");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let _sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();

		// Close without explicit unsubscribe - should not panic
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_rapid_subscribe_unsubscribe() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_rapid");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		// Rapid subscribe/unsubscribe cycles
		for _ in 0..10 {
			let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
			client.unsubscribe(&sub_id).await.unwrap();
		}

		// Should still work after rapid cycles
		let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
		assert!(!sub_id.is_empty());

		client.command(&format!("from [{{ id: 999 }}] insert test.{}", table), None).await.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some(), "Should still receive changes after rapid cycles");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
