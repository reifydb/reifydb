// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::WsClient;
use tokio::runtime::Runtime;

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_ws_port},
	ws::subscription::{create_test_table, find_column, recv_with_timeout, unique_table_name},
};

#[test]
fn test_filtered_subscription() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_filter");
		create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		// Subscribe with filter: only id > 10
		let sub_id = client.subscribe(&format!("from test.{} filter {{ id > 10 }}", table)).await.unwrap();

		// Insert matching data
		client.command(&format!("INSERT test.{} [{{ id: 15, value: 150 }}]", table), None).await.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some(), "Should receive matching insert");

		let change = change.unwrap();
		let id_col = find_column(&change.frame, "id").unwrap();
		assert_eq!(id_col.data[0], "15");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_no_callback_for_non_matching() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_no_match");
		create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		// Subscribe with filter: only id > 100
		let sub_id = client.subscribe(&format!("from test.{} filter {{ id > 100 }}", table)).await.unwrap();

		// Insert non-matching data (id = 5, which is < 100)
		client.command(&format!("INSERT test.{} [{{ id: 5, value: 50 }}]", table), None).await.unwrap();

		// Should NOT receive any change (use short timeout)
		let change = recv_with_timeout(&mut client, 500).await;
		assert!(change.is_none(), "Should NOT receive notification for non-matching insert");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
