// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_client::{GrpcClient, SubscribeItem, SubscriptionConfig, Value, WireFormat};
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
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");

		let table1 = unique_table_name("sub_multi_t1");
		let table2 = unique_table_name("sub_multi_t2");
		create_test_table(&client, &table1, &[("id", "int4"), ("name", "utf8")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		let mut sub1 = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table1),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);
		let mut sub2 = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table2),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);

		assert_ne!(sub1.subscription_id(), sub2.subscription_id(), "Subscription IDs should be different");

		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'alice' }}]", table1), None).await.unwrap();
		client.command(&format!("INSERT test::{} [{{ id: 2, value: 200 }}]", table2), None).await.unwrap();

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
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_same_table");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		let mut sub1 = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);
		let mut sub2 = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);

		assert_ne!(
			sub1.subscription_id(),
			sub2.subscription_id(),
			"Different subscriptions should have different IDs"
		);

		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'test' }}]", table), None).await.unwrap();

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
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");

		let table1 = unique_table_name("sub_route_t1");
		let table2 = unique_table_name("sub_route_t2");
		create_test_table(&client, &table1, &[("id", "int4")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4")]).await.unwrap();

		let mut sub1 = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table1),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);
		let mut sub2 = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table2),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);

		client.command(&format!("INSERT test::{} [{{ id: 100 }}]", table1), None).await.unwrap();

		let change1 = recv_with_timeout(&mut sub1, 5000).await;
		assert!(change1.is_some(), "Sub1 should receive change");

		let frame = &change1.unwrap().changes[0].frame;
		let id_col = find_column(frame, "id").unwrap();
		assert_eq!(id_col.get_value(0), Value::Int4(100));

		let change2 = recv_with_timeout(&mut sub2, 500).await;
		assert!(change2.is_none(), "Sub2 should NOT receive change for table1 insert");

		drop(sub1);
		drop(sub2);
	});

	cleanup_server(Some(server));
}

#[test]
fn one_stream_routes_each_entry_to_its_own_handle() {
	// Catches entries of one shared stream being routed by position instead of by subscription id.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");

		let table_a = unique_table_name("sub_one_stream_a");
		let table_b = unique_table_name("sub_one_stream_b");
		create_test_table(&client, &table_a, &[("id", "int4")]).await.unwrap();
		create_test_table(&client, &table_b, &[("id", "int4")]).await.unwrap();

		let query_a = format!("from test::{}", table_a);
		let query_b = format!("from test::{}", table_b);
		let mut handles = client
			.subscribe(&[
				SubscribeItem::new(&query_a, SubscriptionConfig::default()),
				SubscribeItem::new(&query_b, SubscriptionConfig::default()),
			])
			.await
			.unwrap();
		assert_eq!(handles.len(), 2, "one handle per item");
		assert_ne!(handles[0].subscription_id(), handles[1].subscription_id());

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table_a), None).await.unwrap();
		client.command(&format!("INSERT test::{} [{{ id: 2 }}]", table_b), None).await.unwrap();

		let change_a = recv_with_timeout(&mut handles[0], 5000).await.expect("handle 0 should receive A's row");
		let id_a = find_column(&change_a.changes[0].frame, "id").unwrap();
		assert_eq!(id_a.get_value(0), Value::Int4(1), "handle 0 must see only table A's row");

		let change_b = recv_with_timeout(&mut handles[1], 5000).await.expect("handle 1 should receive B's row");
		let id_b = find_column(&change_b.changes[0].frame, "id").unwrap();
		assert_eq!(id_b.get_value(0), Value::Int4(2), "handle 1 must see only table B's row");

		assert!(
			recv_with_timeout(&mut handles[0], 500).await.is_none(),
			"handle 0 must not receive table B's row"
		);
		assert!(
			recv_with_timeout(&mut handles[1], 500).await.is_none(),
			"handle 1 must not receive table A's row"
		);
	});

	cleanup_server(Some(server));
}
