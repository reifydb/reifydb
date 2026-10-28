// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_client::{
	GrpcClient, SubscribeItem, SubscriptionConfig, WireFormat,
	grpc::generated::{
		SubscribeItem as ProtoSubscribeItem, SubscribeRequest, SubscriptionEvent,
		reify_db_client::ReifyDbClient, subscription_event,
	},
};
use reifydb_value::value::duration::Duration;
use tokio::{runtime::Runtime, time::timeout};
use tonic::{Request, Streaming};

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port},
	grpc::subscription::{create_test_table, recv_with_timeout, unique_table_name},
};

#[test]
fn test_no_changes_after_drop_subscription() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_after_unsub");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let sub = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);

		drop(sub);

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		let mut sub2 = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);

		client.command(&format!("INSERT test::{} [{{ id: 2 }}]", table), None).await.unwrap();

		let frames = recv_with_timeout(&mut sub2, 5000).await;
		assert!(frames.is_some(), "Should receive changes on new subscription");

		drop(sub2);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_drop_cleans_up_subscriptions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_close");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let _sub = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);

		drop(_sub);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_rapid_subscribe_drop() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_rapid");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		for _ in 0..10 {
			let sub = client
				.subscribe(&[SubscribeItem::new(
					&format!("from test::{}", table),
					SubscriptionConfig::default(),
				)])
				.await
				.unwrap()
				.remove(0);
			drop(sub);
		}

		let mut sub = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);
		assert!(!sub.subscription_id().is_empty());

		client.command(&format!("INSERT test::{} [{{ id: 999 }}]", table), None).await.unwrap();

		let frames = recv_with_timeout(&mut sub, 5000).await;
		assert!(frames.is_some(), "Should still receive changes after rapid cycles");

		drop(sub);
	});

	cleanup_server(Some(server));
}

async fn connect_as_root(port: u16) -> GrpcClient {
	let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
	client.authenticate("mysecrettoken");
	client
}

async fn receives_change_for(
	stream: &mut Streaming<SubscriptionEvent>,
	subscription_id: &str,
	timeout_ms: i64,
) -> bool {
	let wait = async {
		while let Ok(Some(event)) = stream.message().await {
			if let Some(subscription_event::Event::Change(change)) = event.event
				&& change.entries.iter().any(|entry| entry.subscription_id == subscription_id)
			{
				return true;
			}
		}
		false
	};
	timeout(Duration::from_milliseconds(timeout_ms).unwrap().to_std(), wait).await.unwrap_or(false)
}

#[test]
fn another_stream_cannot_unsubscribe_a_subscription_it_does_not_own() {
	// Ids come from a counter, so an unscoped unsubscribe lets any caller end another stream by guessing.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let owner = connect_as_root(port).await;
		let table = unique_table_name("sub_foreign_unsub");
		create_test_table(&owner, &table, &[("id", "int4")]).await.unwrap();
		let query = format!("from test::{}", table);
		let mut sub = owner
			.subscribe(&[SubscribeItem::new(&query, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

		let intruder = connect_as_root(port).await;
		let foreign = intruder.unsubscribe(&[sub.subscription_id()]).await.map_err(|e| e.to_string());
		let unknown = intruder.unsubscribe(&["999999"]).await.map_err(|e| e.to_string());

		owner.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			recv_with_timeout(&mut sub, 5000).await.is_some(),
			"a foreign unsubscribe must leave the owner's stream delivering"
		);
		assert_eq!(foreign, unknown, "a foreign id must be answered exactly like an unknown one");
	});

	cleanup_server(Some(server));
}

#[test]
fn another_stream_cannot_unsubscribe_a_call_it_does_not_own() {
	// Dashboards open many subscriptions per call, so an unscoped list unsubscribe ends every other user's page.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let setup = connect_as_root(port).await;
		let table = unique_table_name("sub_foreign_list_unsub");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = ReifyDbClient::connect(format!("http://[::1]:{}", port)).await.unwrap();
		let mut request = Request::new(SubscribeRequest {
			subscriptions: vec![
				ProtoSubscribeItem {
					rql: format!("from test::{}", table),
					options: None,
				},
				ProtoSubscribeItem {
					rql: format!("from test::{}", table),
					options: None,
				},
			],
		});
		request.metadata_mut().insert("authorization", "Bearer mysecrettoken".parse().unwrap());
		let mut stream = owner.subscribe(request).await.unwrap().into_inner();
		let Some(subscription_event::Event::Subscribed(subscribed)) =
			stream.message().await.unwrap().and_then(|event| event.event)
		else {
			panic!("the owner's subscribe must be acknowledged first");
		};
		let first = subscribed.subscriptions[0].subscription_id.clone();
		let second = subscribed.subscriptions[1].subscription_id.clone();

		let intruder = connect_as_root(port).await;
		let foreign = intruder.unsubscribe(&[&first, &second]).await.map_err(|e| e.to_string());
		let unknown = intruder.unsubscribe(&["12345", "12346"]).await.map_err(|e| e.to_string());

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		assert!(
			receives_change_for(&mut stream, &first, 5000).await,
			"a foreign list unsubscribe must leave the owner's first subscription delivering"
		);
		setup.command(&format!("INSERT test::{} [{{ id: 2 }}]", table), None).await.unwrap();
		assert!(
			receives_change_for(&mut stream, &second, 5000).await,
			"a foreign list unsubscribe must leave the owner's second subscription delivering"
		);
		assert_eq!(foreign, unknown, "foreign ids must be answered exactly like unknown ones");
	});

	cleanup_server(Some(server));
}

#[test]
fn the_owner_still_unsubscribes_its_own_subscription() {
	// Scoping must never lock the owner out, or an explicit unsubscribe silently leaves its stream running.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let owner = connect_as_root(port).await;
		let table = unique_table_name("sub_own_unsub");
		create_test_table(&owner, &table, &[("id", "int4")]).await.unwrap();
		let query = format!("from test::{}", table);
		let mut sub = owner
			.subscribe(&[SubscribeItem::new(&query, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);

		owner.unsubscribe(&[sub.subscription_id()]).await.unwrap();
		owner.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			recv_with_timeout(&mut sub, 500).await.is_none(),
			"the owner's own unsubscribe must end its stream"
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn the_owner_unsubscribes_every_item_of_its_call_in_one_request() {
	// A list unsubscribe that honours only its first id would leave the other items of the call streaming.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let owner = connect_as_root(port).await;
		let table = unique_table_name("sub_own_list_unsub");
		create_test_table(&owner, &table, &[("id", "int4")]).await.unwrap();
		let query = format!("from test::{}", table);
		let mut handles = owner
			.subscribe(&[
				SubscribeItem::new(&query, SubscriptionConfig::default()),
				SubscribeItem::new(&query, SubscriptionConfig::default()),
			])
			.await
			.unwrap();
		let first = handles[0].subscription_id().to_string();
		let second = handles[1].subscription_id().to_string();

		owner.unsubscribe(&[&first, &second]).await.unwrap();
		owner.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			recv_with_timeout(&mut handles[0], 500).await.is_none(),
			"the owner's list unsubscribe must stop delivery to the first item"
		);
		assert!(
			recv_with_timeout(&mut handles[1], 500).await.is_none(),
			"the owner's list unsubscribe must stop delivery to the second item"
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn dropping_one_handle_keeps_its_siblings_streaming() {
	// Catches dropping one handle of a call closing the stream its sibling handles share.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let owner = connect_as_root(port).await;
		let table = unique_table_name("sub_drop_one_handle");
		create_test_table(&owner, &table, &[("id", "int4")]).await.unwrap();
		let query = format!("from test::{}", table);
		let mut handles = owner
			.subscribe(&[
				SubscribeItem::new(&query, SubscriptionConfig::default()),
				SubscribeItem::new(&query, SubscriptionConfig::default()),
			])
			.await
			.unwrap();

		drop(handles.remove(0));
		let mut sibling = handles.remove(0);
		owner.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			recv_with_timeout(&mut sibling, 5000).await.is_some(),
			"dropping one handle must leave its sibling delivering"
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn dropping_every_handle_closes_the_stream() {
	// Catches the shared stream task outliving the last handle of its call and keeping the subscriptions open.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let owner = connect_as_root(port).await;
		let table = unique_table_name("sub_drop_all_handles");
		create_test_table(&owner, &table, &[("id", "int4")]).await.unwrap();
		let query = format!("from test::{}", table);
		let handles = owner
			.subscribe(&[
				SubscribeItem::new(&query, SubscriptionConfig::default()),
				SubscribeItem::new(&query, SubscriptionConfig::default()),
			])
			.await
			.unwrap();
		let first = handles[0].subscription_id().to_string();
		let second = handles[1].subscription_id().to_string();

		drop(handles);

		let mut fresh = owner
			.subscribe(&[SubscribeItem::new(&query, SubscriptionConfig::default())])
			.await
			.unwrap()
			.remove(0);
		owner.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		assert!(
			recv_with_timeout(&mut fresh, 5000).await.is_some(),
			"a new subscription on the same table must still deliver after the old stream closed"
		);

		let closed = owner.unsubscribe(&[&first, &second]).await.map_err(|e| e.to_string());
		let unknown = owner.unsubscribe(&["12345", "12346"]).await.map_err(|e| e.to_string());
		assert_eq!(closed, unknown, "the dropped call's ids must be answered exactly like unknown ones");
	});

	cleanup_server(Some(server));
}
