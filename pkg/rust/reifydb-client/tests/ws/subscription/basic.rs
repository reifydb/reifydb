// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_client::{SubscribeItem, SubscriptionConfig, WireFormat, WsClient};
use reifydb_value::value::duration::Duration;
use tokio::{runtime::Runtime, time::timeout};

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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_basic");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		let rql = format!("from test::{}", table);
		let sub = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);
		assert_eq!(sub.subscription_id(), "1");

		client.unsubscribe(&[sub.subscription_id()]).await.unwrap();
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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_unsub");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let rql = format!("from test::{}", table);
		let sub = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

		let result = client.unsubscribe(&[sub.subscription_id()]).await;
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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let rql = "INVALID RQL SYNTAX HERE";
		let result = client.subscribe(&[SubscribeItem::new(rql, SubscriptionConfig::default())]).await;
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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let rql = "from nonexistent_table_xyz_12345";
		let result = client.subscribe(&[SubscribeItem::new(rql, SubscriptionConfig::default())]).await;
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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		// An unknown id may or may not error server-side; this only pins that it never panics.
		let result = client.unsubscribe(&["fake-subscription-id-12345"]).await;
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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_try_recv");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let rql = format!("from test::{}", table);
		let mut sub = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

		let result = timeout(Duration::from_milliseconds(50).unwrap().to_std(), sub.recv()).await;
		assert!(result.is_err(), "a fresh handle must have nothing to receive");

		client.unsubscribe(&[sub.subscription_id()]).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
