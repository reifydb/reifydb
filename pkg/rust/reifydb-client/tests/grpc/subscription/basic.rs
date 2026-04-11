// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::{Encoding, GrpcClient};
use tokio::runtime::Runtime;

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port},
	grpc::subscription::{create_test_table, recv_with_timeout, unique_table_name},
};

#[test]
fn test_subscribe_returns_subscription_id() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), Encoding::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_basic");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
		assert!(!sub.subscription_id().is_empty(), "Subscription ID should be > 0");

		drop(sub);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_drop_subscription_cleans_up() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), Encoding::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_unsub");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// Drop subscription should succeed without error
		drop(sub);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscribe_invalid_query() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), Encoding::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		// Invalid RQL should return an error
		let result = client.subscribe("INVALID RQL SYNTAX HERE").await;
		assert!(result.is_err(), "Invalid query should return error");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscribe_nonexistent_table() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), Encoding::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		// Non-existent table should return an error
		let result = client.subscribe("from nonexistent_table_xyz_12345").await;
		assert!(result.is_err(), "Non-existent table should return error");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_recv_with_timeout_returns_none_when_empty() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), Encoding::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_try_recv");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let mut sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// recv_with_timeout should return None when no changes
		let result = recv_with_timeout(&mut sub, 500).await;
		assert!(result.is_none(), "recv should return None when no changes pending");

		drop(sub);
	});

	cleanup_server(Some(server));
}
