// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use reifydb_client::{GrpcClient, SubscribeItem, SubscriptionConfig, WireFormat, WsClient};
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

		let rql = format!("from test::{}", table);
		let mut sub = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

		client.unsubscribe(&[sub.subscription_id()]).await.unwrap();

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		let change = recv_with_timeout(&mut sub, 500).await;
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

		let rql = format!("from test::{}", table);
		let _sub = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

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
			let rql = format!("from test::{}", table);
			let sub = client
				.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
				.await
				.unwrap()
				.remove(0);
			client.unsubscribe(&[sub.subscription_id()]).await.unwrap();
		}

		let rql = format!("from test::{}", table);
		let mut sub = client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);
		assert!(!sub.subscription_id().is_empty());

		client.command(&format!("INSERT test::{} [{{ id: 999 }}]", table), None).await.unwrap();

		let change = recv_with_timeout(&mut sub, 5000).await;
		assert!(change.is_some(), "Should still receive changes after rapid cycles");

		client.unsubscribe(&[sub.subscription_id()]).await.unwrap();
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

fn change_entry_ids(push: &Value) -> Vec<String> {
	if push["type"] != "Change" {
		return Vec::new();
	}
	let Some(entries) = push["payload"]["entries"].as_array() else {
		return Vec::new();
	};
	entries.iter().filter_map(|entry| entry["subscription_id"].as_str()).map(str::to_string).collect()
}

/// True once `Change` pushes have carried an entry for every id in `subscription_ids`.
async fn receives_change_for(socket: &mut RawSocket, subscription_ids: &[&str], timeout_ms: i64) -> bool {
	let mut pending: HashSet<&str> = subscription_ids.iter().copied().collect();
	let wait = async {
		loop {
			match socket.next().await {
				Some(Ok(Message::Text(text))) => {
					let push: Value = from_str(&text).unwrap();
					for id in change_entry_ids(&push) {
						pending.remove(id.as_str());
					}
					if pending.is_empty() {
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

/// The entry ids of every `Change` push that arrives within the whole window, one list per push.
async fn change_pushes_within(socket: &mut RawSocket, timeout_ms: i64) -> Vec<Vec<String>> {
	let mut pushes = Vec::new();
	let wait = async {
		loop {
			match socket.next().await {
				Some(Ok(Message::Text(text))) => {
					let push: Value = from_str(&text).unwrap();
					if push["type"] == "Change" {
						pushes.push(change_entry_ids(&push));
					}
				}
				Some(Ok(_)) => {}
				_ => return,
			}
		}
	};
	let _ = timeout(Duration::from_milliseconds(timeout_ms).unwrap().to_std(), wait).await;
	pushes
}

async fn raw_subscribe(socket: &mut RawSocket, queries: &[&str]) -> Value {
	let subscriptions: Vec<Value> =
		queries.iter().map(|rql| json!({"rql": rql, "options": {"hydration": {"enabled": false}}})).collect();
	raw_request(socket, json!({"id": "sub", "type": "Subscribe", "payload": {"subscriptions": subscriptions}}))
		.await
}

async fn raw_unsubscribe(socket: &mut RawSocket, subscription_ids: &[&str]) -> Value {
	let payload = json!({"subscription_ids": subscription_ids});
	raw_request(socket, json!({"id": "unsub", "type": "Unsubscribe", "payload": payload})).await
}

fn subscribed_ids(subscribed: &Value) -> Vec<String> {
	let Some(subscriptions) = subscribed["payload"]["subscriptions"].as_array() else {
		panic!("the subscribe was refused: {subscribed}");
	};
	subscriptions
		.iter()
		.map(|entry| entry["subscription_id"].as_str().expect("every item carries an id").to_string())
		.collect()
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
		let rql = format!("from test::{}", table);
		let subscribed = raw_request(
			&mut owner,
			json!({"id": "sub", "type": "Subscribe", "payload": {"subscriptions": [{"rql": rql}]}}),
		)
		.await;
		let subscription_id = subscribed["payload"]["subscriptions"][0]["subscription_id"]
			.as_str()
			.unwrap_or_else(|| panic!("the owner's subscribe was refused: {subscribed}"))
			.to_string();

		let mut intruder = raw_connect(&url).await;
		let foreign = raw_unsubscribe(&mut intruder, &[subscription_id.as_str()]).await;
		let unknown = raw_unsubscribe(&mut intruder, &["999999"]).await;

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			receives_change_for(&mut owner, &[subscription_id.as_str()], 5000).await,
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
fn a_foreign_list_unsubscribe_leaves_every_listed_subscription_streaming() {
	// Dashboards open many subscriptions per frame, so an unscoped list unsubscribe ends every other user's page.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let url = format!("ws://[::1]:{}", port);
		let mut setup = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		setup.authenticate("mysecrettoken").await.unwrap();
		let table = unique_table_name("sub_foreign_list_unsub");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = raw_connect(&url).await;
		let rql = format!("from test::{}", table);
		let subscribed = raw_subscribe(&mut owner, &[rql.as_str(), rql.as_str()]).await;
		let ids = subscribed_ids(&subscribed);
		assert_eq!(ids.len(), 2, "the owner's subscribe must open both items: {subscribed}");

		let mut intruder = raw_connect(&url).await;
		let foreign = raw_unsubscribe(&mut intruder, &[ids[0].as_str(), ids[1].as_str()]).await;
		let unknown = raw_unsubscribe(&mut intruder, &["12345", "12346"]).await;

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			receives_change_for(&mut owner, &[ids[0].as_str(), ids[1].as_str()], 5000).await,
			"a foreign list unsubscribe must leave every listed subscription streaming"
		);
		assert_eq!(foreign["type"], unknown["type"], "foreign ids must be answered exactly like unknown ones");
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
		let rql = format!("from test::{}", table);
		let subscribed = raw_request(
			&mut owner,
			json!({"id": "sub", "type": "Subscribe", "payload": {"subscriptions": [{"rql": rql}]}}),
		)
		.await;
		let subscription_id = subscribed["payload"]["subscriptions"][0]["subscription_id"]
			.as_str()
			.unwrap_or_else(|| panic!("the owner's subscribe was refused: {subscribed}"))
			.to_string();

		let mut intruder =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), WireFormat::Rbcf).await.unwrap();
		intruder.authenticate("mysecrettoken");
		intruder.unsubscribe(&[subscription_id.as_str()]).await.unwrap();

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			receives_change_for(&mut owner, &[subscription_id.as_str()], 5000).await,
			"a gRPC unsubscribe must never drop a subscription another transport holds"
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn one_subscribe_frame_opens_every_item_in_index_order() {
	// Catches a server that drops a later item or acks the items out of index order, which misroutes every stream.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let url = format!("ws://[::1]:{}", port);
		let mut setup = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		setup.authenticate("mysecrettoken").await.unwrap();
		let table_a = unique_table_name("sub_frame_order_a");
		let table_b = unique_table_name("sub_frame_order_b");
		create_test_table(&setup, &table_a, &[("id", "int4")]).await.unwrap();
		create_test_table(&setup, &table_b, &[("id", "int4")]).await.unwrap();

		let mut owner = raw_connect(&url).await;
		let rql_a = format!("from test::{}", table_a);
		let rql_b = format!("from test::{}", table_b);
		let subscribed = raw_subscribe(&mut owner, &[rql_a.as_str(), rql_b.as_str()]).await;
		assert_eq!(subscribed["type"], "Subscribed", "both items must open: {subscribed}");
		let indexes: Vec<u64> = subscribed["payload"]["subscriptions"]
			.as_array()
			.unwrap()
			.iter()
			.map(|entry| entry["index"].as_u64().expect("every item carries its index"))
			.collect();
		assert_eq!(indexes, vec![0, 1], "the ack must list every item in index order");
		let ids = subscribed_ids(&subscribed);
		assert_ne!(ids[0], ids[1], "every item must get its own subscription id");

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table_b), None).await.unwrap();

		let seen: Vec<String> = change_pushes_within(&mut owner, 2000).await.into_iter().flatten().collect();
		assert!(seen.contains(&ids[1]), "the item on table B must receive its insert");
		assert!(!seen.contains(&ids[0]), "the item on table A must not receive table B's insert");
	});

	cleanup_server(Some(server));
}

#[test]
fn one_change_push_carries_every_ready_subscription() {
	// Catches a sink that sends one push per subscription instead of one per connection per flush.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let url = format!("ws://[::1]:{}", port);
		let mut setup = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		setup.authenticate("mysecrettoken").await.unwrap();
		let table = unique_table_name("sub_one_push");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = raw_connect(&url).await;
		let rql = format!("from test::{}", table);
		let subscribed = raw_subscribe(&mut owner, &[rql.as_str(), rql.as_str()]).await;
		let ids = subscribed_ids(&subscribed);
		assert_eq!(ids.len(), 2, "both items must open: {subscribed}");

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		let pushes = change_pushes_within(&mut owner, 2000).await;
		let first = pushes.first().expect("the insert must reach the connection as a change push");
		assert_eq!(first.len(), 2, "the first push must carry one entry per subscription: {first:?}");
		let first_ids: HashSet<&str> = first.iter().map(String::as_str).collect();
		let expected: HashSet<&str> = ids.iter().map(String::as_str).collect();
		assert_eq!(first_ids, expected, "the first push must carry an entry for each subscription");
	});

	cleanup_server(Some(server));
}

#[test]
fn one_unsubscribe_frame_ends_every_listed_subscription_and_no_other() {
	// Catches a server that ends only the first listed id, or every subscription of the connection.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let url = format!("ws://[::1]:{}", port);
		let mut setup = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		setup.authenticate("mysecrettoken").await.unwrap();
		let table = unique_table_name("sub_list_unsub");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = raw_connect(&url).await;
		let rql = format!("from test::{}", table);
		let subscribed = raw_subscribe(&mut owner, &[rql.as_str(), rql.as_str(), rql.as_str()]).await;
		let ids = subscribed_ids(&subscribed);
		assert_eq!(ids.len(), 3, "all three items must open: {subscribed}");

		let unsubscribed = raw_unsubscribe(&mut owner, &[ids[0].as_str(), ids[1].as_str()]).await;
		assert_eq!(unsubscribed["type"], "Unsubscribed", "the list unsubscribe must succeed: {unsubscribed}");
		assert_eq!(
			unsubscribed["payload"]["subscription_ids"],
			json!([ids[0], ids[1]]),
			"the reply must name exactly the listed ids"
		);

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		let seen: Vec<String> = change_pushes_within(&mut owner, 2000).await.into_iter().flatten().collect();
		assert!(seen.contains(&ids[2]), "the unlisted subscription must keep streaming");
		assert!(!seen.contains(&ids[0]), "the first listed subscription must end");
		assert!(!seen.contains(&ids[1]), "the second listed subscription must end");
	});

	cleanup_server(Some(server));
}

#[test]
fn a_subscribe_with_one_bad_query_opens_none() {
	// Catches partial success, where the valid item stays open after the call as a whole was refused.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let url = format!("ws://[::1]:{}", port);
		let mut setup = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		setup.authenticate("mysecrettoken").await.unwrap();
		let table = unique_table_name("sub_bad_item");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = raw_connect(&url).await;
		let valid = format!("from test::{}", table);
		let missing = format!("from test::{}_missing", table);
		let subscribed = raw_subscribe(&mut owner, &[valid.as_str(), missing.as_str()]).await;
		assert_eq!(subscribed["type"], "Err", "a call with a missing table must be refused: {subscribed}");

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		let pushes = change_pushes_within(&mut owner, 2000).await;
		assert!(pushes.is_empty(), "a refused call must leave no item streaming: {pushes:?}");
	});

	cleanup_server(Some(server));
}
