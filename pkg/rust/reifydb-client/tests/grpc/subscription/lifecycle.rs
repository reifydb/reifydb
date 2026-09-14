// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_client::{
	BatchStreamEvent, GrpcClient, SubscriptionConfig, WireFormat,
	grpc::generated::{
		BatchSubscribeItem, BatchSubscribeRequest, BatchSubscriptionEvent, batch_subscription_event,
		reify_db_client::ReifyDbClient,
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
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

		drop(sub);

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		let mut sub2 = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

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
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

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
				.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
				.await
				.unwrap();
			drop(sub);
		}

		let mut sub = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();
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

async fn batch_receives_change_for(
	stream: &mut Streaming<BatchSubscriptionEvent>,
	subscription_id: &str,
	timeout_ms: i64,
) -> bool {
	let wait = async {
		while let Ok(Some(event)) = stream.message().await {
			if let Some(batch_subscription_event::Event::Change(change)) = event.event
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
		let mut sub =
			owner.subscribe(&format!("from test::{}", table), SubscriptionConfig::default()).await.unwrap();

		let intruder = connect_as_root(port).await;
		let foreign = intruder.unsubscribe(sub.subscription_id()).await.map_err(|e| e.to_string());
		let unknown = intruder.unsubscribe("999999").await.map_err(|e| e.to_string());

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
fn another_stream_cannot_unsubscribe_a_batch_it_does_not_own() {
	// Dashboards stream through batches, so an unscoped batch unsubscribe ends every other user's page.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let setup = connect_as_root(port).await;
		let table = unique_table_name("sub_foreign_batch_unsub");
		create_test_table(&setup, &table, &[("id", "int4")]).await.unwrap();

		let mut owner = ReifyDbClient::connect(format!("http://[::1]:{}", port)).await.unwrap();
		let mut request = Request::new(BatchSubscribeRequest {
			subscriptions: vec![BatchSubscribeItem {
				rql: format!("from test::{}", table),
				options: None,
			}],
		});
		request.metadata_mut().insert("authorization", "Bearer mysecrettoken".parse().unwrap());
		let mut stream = owner.batch_subscribe(request).await.unwrap().into_inner();
		let Some(batch_subscription_event::Event::Subscribed(subscribed)) =
			stream.message().await.unwrap().and_then(|event| event.event)
		else {
			panic!("the owner's batch subscribe must be acknowledged first");
		};
		let subscription_id = subscribed.subscriptions[0].subscription_id.clone();

		let intruder = connect_as_root(port).await;
		let foreign = intruder.batch_unsubscribe(&subscribed.batch_id).await.map_err(|e| e.to_string());
		let unknown = intruder.batch_unsubscribe("12345").await.map_err(|e| e.to_string());

		setup.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			batch_receives_change_for(&mut stream, &subscription_id, 5000).await,
			"a foreign batch unsubscribe must leave the owner's batch delivering"
		);
		assert_eq!(foreign, unknown, "a foreign batch id must be answered exactly like an unknown one");
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
		let mut sub =
			owner.subscribe(&format!("from test::{}", table), SubscriptionConfig::default()).await.unwrap();

		owner.unsubscribe(sub.subscription_id()).await.unwrap();
		owner.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		assert!(
			recv_with_timeout(&mut sub, 500).await.is_none(),
			"the owner's own unsubscribe must end its stream"
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn the_owner_still_unsubscribes_its_own_batch() {
	// A batch unsubscribe by the handle's own id must stop delivery, or every member keeps streaming.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let owner = connect_as_root(port).await;
		let table = unique_table_name("sub_own_batch_unsub");
		create_test_table(&owner, &table, &[("id", "int4")]).await.unwrap();
		let query = format!("from test::{}", table);
		let mut batch = owner
			.batch_subscribe(&[reifydb_client::BatchSubscribeItem::new(
				&query,
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap();

		owner.batch_unsubscribe(batch.batch_id()).await.unwrap();
		owner.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		let after = timeout(Duration::from_milliseconds(500).unwrap().to_std(), batch.recv()).await;
		assert!(
			!matches!(after, Ok(Some(BatchStreamEvent::Change(_)))),
			"the owner's own batch unsubscribe must stop delivery"
		);
	});

	cleanup_server(Some(server));
}
