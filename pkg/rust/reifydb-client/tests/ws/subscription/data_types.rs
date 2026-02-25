// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::WsClient;
use tokio::runtime::Runtime;

use super::{create_test_table, find_column, recv_with_timeout, unique_table_name};
use crate::common::{cleanup_server, create_server_instance, start_server_and_get_ws_port};

#[test]
fn test_subscription_int_types() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_int_types");
		create_test_table(&client, &table, &[("i1", "int1"), ("i2", "int2"), ("i4", "int4"), ("i8", "int8")])
			.await
			.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		client.command(
			&format!(
				"INSERT test::{} [{{ i1: 127, i2: 32767, i4: 2147483647, i8: 9223372036854775807 }}]",
				table
			),
			None,
		)
		.await
		.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some());

		let frame = &change.unwrap().frame;
		assert_eq!(find_column(frame, "i1").unwrap().data[0], "127");
		assert_eq!(find_column(frame, "i2").unwrap().data[0], "32767");
		assert_eq!(find_column(frame, "i4").unwrap().data[0], "2147483647");
		assert_eq!(find_column(frame, "i8").unwrap().data[0], "9223372036854775807");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscription_uint_types() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_uint_types");
		create_test_table(
			&client,
			&table,
			&[("u1", "uint1"), ("u2", "uint2"), ("u4", "uint4"), ("u8", "uint8")],
		)
		.await
		.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		client.command(
			&format!(
				"INSERT test::{} [{{ u1: 255, u2: 65535, u4: 4294967295, u8: 18446744073709551615 }}]",
				table
			),
			None,
		)
		.await
		.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some());

		let frame = &change.unwrap().frame;
		assert_eq!(find_column(frame, "u1").unwrap().data[0], "255");
		assert_eq!(find_column(frame, "u2").unwrap().data[0], "65535");
		assert_eq!(find_column(frame, "u4").unwrap().data[0], "4294967295");
		assert_eq!(find_column(frame, "u8").unwrap().data[0], "18446744073709551615");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscription_float_types() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_float_types");
		create_test_table(&client, &table, &[("f4", "float4"), ("f8", "float8")]).await.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		client.command(&format!("INSERT test::{} [{{ f4: 3.14, f8: 2.718281828459045 }}]", table), None)
			.await
			.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some());

		let frame = &change.unwrap().frame;
		let f4_val: f32 = find_column(frame, "f4").unwrap().data[0].parse().unwrap();
		let f8_val: f64 = find_column(frame, "f8").unwrap().data[0].parse().unwrap();

		assert!((f4_val - 3.14).abs() < 0.01);
		assert!((f8_val - 2.718281828459045).abs() < 0.0001);

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscription_string_types() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_string");
		create_test_table(&client, &table, &[("s", "utf8"), ("s2", "utf8")]).await.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		client.command(&format!("INSERT test::{} [{{ s: 'hello world', s2: 'test data' }}]", table), None)
			.await
			.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some());

		let frame = &change.unwrap().frame;
		assert_eq!(find_column(frame, "s").unwrap().data[0], "hello world");
		assert_eq!(find_column(frame, "s2").unwrap().data[0], "test data");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscription_temporal() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_temporal");
		create_test_table(&client, &table, &[("d", "date"), ("t", "time"), ("dt", "datetime")]).await.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// Use quoted strings for temporal values (will be cast to temporal types)
		client.command(
			&format!(
				"INSERT test::{} [{{ d: '2025-01-15', t: '14:30:00', dt: '2025-01-15T14:30:00Z' }}]",
				table
			),
			None,
		)
		.await
		.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some());

		let frame = &change.unwrap().frame;
		assert!(find_column(frame, "d").unwrap().data[0].contains("2025"));
		assert!(find_column(frame, "t").unwrap().data[0].contains("14"));

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscription_uuid() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_uuid");
		create_test_table(&client, &table, &[("u4", "uuid4"), ("u7", "uuid7")]).await.unwrap();

		let sub_id = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		client.command(
			&format!(
				"INSERT test::{} [{{ u4: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', u7: '019478f0-d3af-7e22-9d7a-f8d7c7a3b3c4' }}]",
				table
			),
			None,
		)
		.await
		.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some());

		let frame = &change.unwrap().frame;
		let u4_val = &find_column(frame, "u4").unwrap().data[0];
		let u7_val = &find_column(frame, "u7").unwrap().data[0];

		// UUIDs should have the transaction format with hyphens
		assert!(u4_val.contains("-"), "UUID4 should contain hyphens");
		assert!(u7_val.contains("-"), "UUID7 should contain hyphens");
		assert_eq!(u4_val.len(), 36, "UUID should be 36 chars");
		assert_eq!(u7_val.len(), 36, "UUID should be 36 chars");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
