// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::{GrpcClient, Value};
use tokio::runtime::Runtime;

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port},
	grpc::subscription::{create_test_table, find_column, recv_with_timeout, unique_table_name},
};

#[test]
fn test_multiple_subscriptions_different_tables() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken");

		let table1 = unique_table_name("sub_multi_t1");
		let table2 = unique_table_name("sub_multi_t2");
		create_test_table(&client, &table1, &[("id", "int4"), ("name", "utf8")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		let mut sub1 = client.subscribe(&format!("from test::{}", table1)).await.unwrap();
		let mut sub2 = client.subscribe(&format!("from test::{}", table2)).await.unwrap();

		assert_ne!(sub1.subscription_id(), sub2.subscription_id(), "Subscription IDs should be different");

		// Insert into both tables
		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'alice' }}]", table1), None).await.unwrap();
		client.command(&format!("INSERT test::{} [{{ id: 2, value: 200 }}]", table2), None).await.unwrap();

		// Receive changes on each subscription independently
		let frames1 = recv_with_timeout(&mut sub1, 5000).await;
		assert!(frames1.is_some(), "Should receive change on sub1");

		let frames2 = recv_with_timeout(&mut sub2, 5000).await;
		assert!(frames2.is_some(), "Should receive change on sub2");

		drop(sub1);
		drop(sub2);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_multiple_subscriptions_same_table() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_same_table");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		// Subscribe twice to the same table
		let mut sub1 = client.subscribe(&format!("from test::{}", table)).await.unwrap();
		let mut sub2 = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		assert_ne!(
			sub1.subscription_id(),
			sub2.subscription_id(),
			"Different subscriptions should have different IDs"
		);

		// Insert data
		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'test' }}]", table), None).await.unwrap();

		// Should receive change for both subscriptions
		let frames1 = recv_with_timeout(&mut sub1, 5000).await;
		assert!(frames1.is_some(), "Sub1 should receive change");

		let frames2 = recv_with_timeout(&mut sub2, 5000).await;
		assert!(frames2.is_some(), "Sub2 should receive change");

		drop(sub1);
		drop(sub2);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_changes_routed_to_correct_subscription() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken");

		let table1 = unique_table_name("sub_route_t1");
		let table2 = unique_table_name("sub_route_t2");
		create_test_table(&client, &table1, &[("id", "int4")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4")]).await.unwrap();

		let mut sub1 = client.subscribe(&format!("from test::{}", table1)).await.unwrap();
		let mut sub2 = client.subscribe(&format!("from test::{}", table2)).await.unwrap();

		// Insert only into table1
		client.command(&format!("INSERT test::{} [{{ id: 100 }}]", table1), None).await.unwrap();

		// Sub1 should receive the change
		let frames1 = recv_with_timeout(&mut sub1, 5000).await;
		assert!(frames1.is_some(), "Sub1 should receive change");

		let frame = &frames1.unwrap()[0];
		let id_col = find_column(frame, "id").unwrap();
		assert_eq!(id_col.data.get_value(0), Value::Int4(100));

		// Sub2 should NOT receive any change (short timeout)
		let frames2 = recv_with_timeout(&mut sub2, 500).await;
		assert!(frames2.is_none(), "Sub2 should NOT receive change for table1 insert");

		drop(sub1);
		drop(sub2);
	});

	cleanup_server(Some(server));
}
