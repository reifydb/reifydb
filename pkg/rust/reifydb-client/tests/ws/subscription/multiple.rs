// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::WsClient;
use tokio::runtime::Runtime;

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_ws_port},
	ws::subscription::{
		create_test_table, find_column, recv_multiple_with_timeout, recv_with_timeout, unique_table_name,
	},
};

#[test]
fn test_multiple_subscriptions_different_tables() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table1 = unique_table_name("sub_multi_t1");
		let table2 = unique_table_name("sub_multi_t2");
		create_test_table(&client, &table1, &[("id", "int4"), ("name", "utf8")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		let sub_id1 = client.subscribe(&format!("from test::{}", table1)).await.unwrap();
		let sub_id2 = client.subscribe(&format!("from test::{}", table2)).await.unwrap();

		assert_ne!(sub_id1, sub_id2, "Subscription IDs should be different");

		// Insert into both tables
		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'alice' }}]", table1), None).await.unwrap();
		client.command(&format!("INSERT test::{} [{{ id: 2, value: 200 }}]", table2), None).await.unwrap();

		// Receive both changes
		let changes = recv_multiple_with_timeout(&mut client, 2, 5000).await;
		assert_eq!(changes.len(), 2, "Should receive 2 changes");

		// Verify changes have different subscription IDs
		let subs: Vec<_> = changes.iter().map(|c| c.subscription_id.as_str()).collect();
		assert!(subs.contains(&sub_id1.as_str()));
		assert!(subs.contains(&sub_id2.as_str()));

		client.unsubscribe(&sub_id1).await.unwrap();
		client.unsubscribe(&sub_id2).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_multiple_subscriptions_same_table() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_same_table");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		// Subscribe twice to the same table
		let sub_id1 = client.subscribe(&format!("from test::{}", table)).await.unwrap();
		let sub_id2 = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		assert_ne!(sub_id1, sub_id2, "Different subscriptions should have different IDs");

		// Insert data
		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'test' }}]", table), None).await.unwrap();

		// Should receive change for both subscriptions
		let changes = recv_multiple_with_timeout(&mut client, 2, 5000).await;
		assert_eq!(changes.len(), 2, "Should receive 2 changes (one per subscription)");

		client.unsubscribe(&sub_id1).await.unwrap();
		client.unsubscribe(&sub_id2).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_changes_routed_to_correct_subscription() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table1 = unique_table_name("sub_route_t1");
		let table2 = unique_table_name("sub_route_t2");
		create_test_table(&client, &table1, &[("id", "int4")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4")]).await.unwrap();

		let sub_id1 = client.subscribe(&format!("from test::{}", table1)).await.unwrap();
		let sub_id2 = client.subscribe(&format!("from test::{}", table2)).await.unwrap();

		// Insert only into table1
		client.command(&format!("INSERT test::{} [{{ id: 100 }}]", table1), None).await.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some());

		let change = change.unwrap();
		// Change should be for sub_id1 only
		assert_eq!(change.subscription_id, sub_id1, "Change should be routed to correct subscription");

		// Verify the data
		let id_col = find_column(&change.frame, "id").unwrap();
		assert_eq!(id_col.data[0], "100");

		client.unsubscribe(&sub_id1).await.unwrap();
		client.unsubscribe(&sub_id2).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
