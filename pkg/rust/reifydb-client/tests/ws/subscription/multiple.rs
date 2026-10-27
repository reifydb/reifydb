// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_client::{SubscribeItem, SubscriptionConfig, WireFormat, WsClient};
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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table1 = unique_table_name("sub_multi_t1");
		let table2 = unique_table_name("sub_multi_t2");
		create_test_table(&client, &table1, &[("id", "int4"), ("name", "utf8")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		let rql = format!("from test::{}", table1);
		let mut sub1 = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);
		let rql = format!("from test::{}", table2);
		let mut sub2 = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

		assert_ne!(sub1.subscription_id(), sub2.subscription_id(), "Subscription IDs should be different");

		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'alice' }}]", table1), None).await.unwrap();
		client.command(&format!("INSERT test::{} [{{ id: 2, value: 200 }}]", table2), None).await.unwrap();

		let mut changes = recv_multiple_with_timeout(&mut sub1, 1, 5000).await;
		changes.extend(recv_multiple_with_timeout(&mut sub2, 1, 5000).await);
		assert_eq!(changes.len(), 2, "Should receive 2 changes");

		let subs: Vec<_> = changes.iter().map(|c| c.subscription_id.as_str()).collect();
		assert!(subs.contains(&sub1.subscription_id()));
		assert!(subs.contains(&sub2.subscription_id()));

		client.unsubscribe(&[sub1.subscription_id()]).await.unwrap();
		client.unsubscribe(&[sub2.subscription_id()]).await.unwrap();
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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_same_table");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		let rql = format!("from test::{}", table);
		let mut sub1 = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);
		let rql = format!("from test::{}", table);
		let mut sub2 = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

		assert_ne!(
			sub1.subscription_id(),
			sub2.subscription_id(),
			"Different subscriptions should have different IDs"
		);

		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'test' }}]", table), None).await.unwrap();

		let mut changes = recv_multiple_with_timeout(&mut sub1, 1, 5000).await;
		changes.extend(recv_multiple_with_timeout(&mut sub2, 1, 5000).await);
		assert_eq!(changes.len(), 2, "Should receive 2 changes (one per subscription)");

		client.unsubscribe(&[sub1.subscription_id()]).await.unwrap();
		client.unsubscribe(&[sub2.subscription_id()]).await.unwrap();
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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table1 = unique_table_name("sub_route_t1");
		let table2 = unique_table_name("sub_route_t2");
		create_test_table(&client, &table1, &[("id", "int4")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4")]).await.unwrap();

		let rql = format!("from test::{}", table1);
		let mut sub1 = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);
		let rql = format!("from test::{}", table2);
		let mut sub2 = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

		client.command(&format!("INSERT test::{} [{{ id: 100 }}]", table1), None).await.unwrap();

		let change = recv_with_timeout(&mut sub1, 5000).await;
		assert!(change.is_some());

		let change = change.unwrap();
		assert_eq!(
			change.subscription_id,
			sub1.subscription_id(),
			"Change should be routed to correct subscription"
		);
		assert!(
			recv_with_timeout(&mut sub2, 500).await.is_none(),
			"Change should be routed to correct subscription"
		);

		let id_col = find_column(&change.body, "id").unwrap();
		assert_eq!(id_col.payload[0], "100");

		client.unsubscribe(&[sub1.subscription_id()]).await.unwrap();
		client.unsubscribe(&[sub2.subscription_id()]).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn subscribe_returns_one_handle_per_item_in_order() {
	// Catches the client pairing handles with ack order instead of item index, which swaps the streams.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table_a = unique_table_name("sub_order_a");
		let table_b = unique_table_name("sub_order_b");
		create_test_table(&client, &table_a, &[("id", "int4")]).await.unwrap();
		create_test_table(&client, &table_b, &[("id", "int4")]).await.unwrap();

		let rql_a = format!("from test::{}", table_a);
		let rql_b = format!("from test::{}", table_b);
		let mut handles = client
			.subscribe(&[
				SubscribeItem::new(&rql_a, SubscriptionConfig::default()),
				SubscribeItem::new(&rql_b, SubscriptionConfig::default()),
			])
			.await
			.unwrap();
		assert_eq!(handles.len(), 2, "one handle per item");

		client.command(&format!("INSERT test::{} [{{ id: 200 }}]", table_b), None).await.unwrap();

		let change = recv_with_timeout(&mut handles[1], 5000).await.expect("the second handle streams table B");
		assert_eq!(change.subscription_id, handles[1].subscription_id());
		assert_eq!(find_column(&change.body, "id").unwrap().payload[0], "200");
		assert!(
			recv_with_timeout(&mut handles[0], 1000).await.is_none(),
			"the first handle streams table A only"
		);

		let ids: Vec<&str> = handles.iter().map(|h| h.subscription_id()).collect();
		client.unsubscribe(&ids).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
