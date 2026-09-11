// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use reifydb_client::{GrpcClient, SubscriptionConfig, WireFormat, WsClient, build_subscription_rql};
use reifydb_value::value::duration::Duration;
use serde_json::{Value, from_str, json};
use tokio::{net::TcpStream, runtime::Runtime, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_ws_port},
	ws::subscription::{create_test_table, recv_with_timeout, unique_table_name},
};

#[test]
fn test_no_changes_after_unsubscribe() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_after_unsub");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let sub_id = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

		client.unsubscribe(&sub_id).await.unwrap();

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_close");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let _sub_id = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

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
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_rapid");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		for _ in 0..10 {
			let sub_id = client
				.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
				.await
				.unwrap();
			client.unsubscribe(&sub_id).await.unwrap();
		}

		let sub_id = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();
		assert!(!sub_id.is_empty());

		client.command(&format!("INSERT test::{} [{{ id: 999 }}]", table), None).await.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some(), "Should still receive changes after rapid cycles");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

type RawSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn raw_connect(url: &str) -> RawSocket {
	let (mut socket, _) = connect_async(url).await.unwrap();
	let auth =
		raw_request(&mut socket, json!({"id": "auth", "type": "Auth", "payload": {"token": "mysecrettoken"}}))
			.await;
	assert_eq!(auth["type"], "Auth", "the raw connection must authenticate: {auth}");
	socket
}

async fn raw_request(socket: &mut RawSocket, request: Value) -> Value {
	let id = request["id"].clone();
	socket.send(Message::Text(request.to_string().into())).await.unwrap();
	loop {
		match socket.next().await {
			Some(Ok(Message::Text(text))) => {
				let response: Value = from_str(&text).unwrap();
				if response["id"] == id {
					return response;
				}
			}
			Some(Ok(_)) => {}
			other => panic!("the connection ended before answering {id}: {other:?}"),
		}
	}
}

async fn receives_change_for(socket: &mut RawSocket, subscription_id: &str, timeout_ms: i64) -> bool {
	let wait = async {
		loop {
			match socket.next().await {
				Some(Ok(Message::Text(text))) => {
					let push: Value = from_str(&text).unwrap();
					let entries = push["payload"]["entries"].as_array();
					if (push["type"] == "Change"
						&& push["payload"]["subscription_id"] == subscription_id)
						|| (push["type"] == "BatchChange"
							&& entries.is_some_and(|entries| {
								entries.iter().any(|entry| {
									entry["subscription_id"] == subscription_id
								})
							})) {
						return true;
					}
				}
				Some(Ok(_)) => {}
				_ => return false,
			}
		}
	};
	timeout(Duration::from_milliseconds(timeout_ms).unwrap().to_std(), wait).await.unwrap_or(false)
}

#[test]
fn another_connection_cannot_unsubscribe_a_subscription_it_does_not_own() {
	// Server ids restart from the same counter, so an unscoped unsubscribe lets any client end another's stream.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let url = format!("ws://[::1]:{}", port);
		let mut setup = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		setup.authenticate("mysecrettoken").await.unwrap();
		let table = unique_table_name("sub_foreign_unsub");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = raw_connect(&url).await;
		let rql = build_subscription_rql(&format!("from test::{}", table), &SubscriptionConfig::default());
		let subscribed =
			raw_request(&mut owner, json!({"id": "sub", "type": "Subscribe", "payload": {"rql": rql}}))
				.await;
		let subscription_id = subscribed["payload"]["subscription_id"]
			.as_str()
			.unwrap_or_else(|| panic!("the owner's subscribe was refused: {subscribed}"))
			.to_string();

		let mut intruder = raw_connect(&url).await;
		let foreign = raw_request(
			&mut intruder,
			json!({"id": "unsub", "type": "Unsubscribe", "payload": {"subscription_id": subscription_id}}),
		)
		.await;
		let unknown = raw_request(
			&mut intruder,
			json!({"id": "unsub", "type": "Unsubscribe", "payload": {"subscription_id": "999999"}}),
		)
		.await;

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			receives_change_for(&mut owner, &subscription_id, 5000).await,
			"a foreign unsubscribe must leave the owner's subscription streaming"
		);
		assert_eq!(
			foreign["type"], unknown["type"],
			"a foreign id must be answered exactly like an unknown one"
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn another_connection_cannot_unsubscribe_a_batch_it_does_not_own() {
	// Dashboards stream through batches, so an unscoped batch unsubscribe ends every other user's page.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let url = format!("ws://[::1]:{}", port);
		let mut setup = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		setup.authenticate("mysecrettoken").await.unwrap();
		let table = unique_table_name("sub_foreign_batch_unsub");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = raw_connect(&url).await;
		let rql = build_subscription_rql(&format!("from test::{}", table), &SubscriptionConfig::default());
		let subscribed = raw_request(
			&mut owner,
			json!({"id": "batch", "type": "BatchSubscribe", "payload": {"queries": [rql]}}),
		)
		.await;
		let batch_id = subscribed["payload"]["batch_id"]
			.as_str()
			.unwrap_or_else(|| panic!("the owner's batch subscribe was refused: {subscribed}"))
			.to_string();
		let subscription_id =
			subscribed["payload"]["members"][0]["subscription_id"].as_str().unwrap().to_string();

		let mut intruder = raw_connect(&url).await;
		let foreign = raw_request(
			&mut intruder,
			json!({"id": "unsub", "type": "BatchUnsubscribe", "payload": {"batch_id": batch_id}}),
		)
		.await;
		let unknown = raw_request(
			&mut intruder,
			json!({"id": "unsub", "type": "BatchUnsubscribe", "payload": {"batch_id": "12345"}}),
		)
		.await;

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			receives_change_for(&mut owner, &subscription_id, 5000).await,
			"a foreign batch unsubscribe must leave the owner's batch streaming"
		);
		assert_eq!(
			foreign["type"], unknown["type"],
			"a foreign batch id must be answered exactly like an unknown one"
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn a_grpc_unsubscribe_cannot_end_a_websocket_subscription() {
	// WebSocket and gRPC share one id counter, so a gRPC unsubscribe by id must never end a socket's stream.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
	let grpc_port = server.sub_server_grpc().unwrap().admin_port().unwrap();

	runtime.block_on(async {
		let url = format!("ws://[::1]:{}", port);
		let mut setup = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		setup.authenticate("mysecrettoken").await.unwrap();
		let table = unique_table_name("sub_grpc_unsub_ws");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = raw_connect(&url).await;
		let rql = build_subscription_rql(&format!("from test::{}", table), &SubscriptionConfig::default());
		let subscribed =
			raw_request(&mut owner, json!({"id": "sub", "type": "Subscribe", "payload": {"rql": rql}}))
				.await;
		let subscription_id = subscribed["payload"]["subscription_id"]
			.as_str()
			.unwrap_or_else(|| panic!("the owner's subscribe was refused: {subscribed}"))
			.to_string();

		let mut intruder =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), WireFormat::Rbcf).await.unwrap();
		intruder.authenticate("mysecrettoken");
		intruder.unsubscribe(&subscription_id).await.unwrap();

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			receives_change_for(&mut owner, &subscription_id, 5000).await,
			"a gRPC unsubscribe must never drop a subscription another transport holds"
		);
	});

	cleanup_server(Some(server));
}
