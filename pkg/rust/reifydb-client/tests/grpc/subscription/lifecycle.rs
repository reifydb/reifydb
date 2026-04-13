// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::{GrpcClient, WireFormat};
use tokio::runtime::Runtime;

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
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_after_unsub");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// Drop subscription
		drop(sub);

		// Insert data after dropping subscription
		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();

		// Re-subscribe and verify only new data arrives
		let mut sub2 = client.subscribe(&format!("from test::{}", table)).await.unwrap();

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
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_close");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let _sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// Drop without explicit cleanup - should not panic
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
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_rapid");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		// Rapid subscribe/drop cycles
		for _ in 0..10 {
			let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			drop(sub);
		}

		// Should still work after rapid cycles
		let mut sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
		assert!(!sub.subscription_id().is_empty());

		client.command(&format!("INSERT test::{} [{{ id: 999 }}]", table), None).await.unwrap();

		let frames = recv_with_timeout(&mut sub, 5000).await;
		assert!(frames.is_some(), "Should still receive changes after rapid cycles");

		drop(sub);
	});

	cleanup_server(Some(server));
}
