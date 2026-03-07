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
fn test_filtered_subscription() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_filter");
		create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		// Subscribe with filter: only id > 10
		let mut sub = client.subscribe(&format!("from test::{} filter {{ id > 10 }}", table)).await.unwrap();

		// Insert matching data
		client.command(&format!("INSERT test::{} [{{ id: 15, value: 150 }}]", table), None).await.unwrap();

		let frames = recv_with_timeout(&mut sub, 5000).await;
		assert!(frames.is_some(), "Should receive matching insert");

		let frames = frames.unwrap();
		let id_col = find_column(&frames[0], "id").unwrap();
		assert_eq!(id_col.data.get_value(0), Value::Int4(15));

		drop(sub);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_no_callback_for_non_matching() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_no_match");
		create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		// Subscribe with filter: only id > 100
		let mut sub = client.subscribe(&format!("from test::{} filter {{ id > 100 }}", table)).await.unwrap();

		// Insert non-matching data (id = 5, which is < 100)
		client.command(&format!("INSERT test::{} [{{ id: 5, value: 50 }}]", table), None).await.unwrap();

		// Should NOT receive any change (use short timeout)
		let frames = recv_with_timeout(&mut sub, 500).await;
		assert!(frames.is_none(), "Should NOT receive notification for non-matching insert");

		drop(sub);
	});

	cleanup_server(Some(server));
}
