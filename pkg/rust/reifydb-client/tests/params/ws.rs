// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_client::{Value, ValueType, WireFormat, WsClient};
use tokio::runtime::Runtime;

use super::{assert_echoed, list_of_records, named, named_as, option, positional};
use crate::common::{cleanup_server, create_server_instance, start_server_and_get_ws_port};

fn run<F, Fut>(test_fn: F)
where
	F: FnOnce(WsClient) -> Fut,
	Fut: std::future::Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();
		test_fn(client).await;
	});

	cleanup_server(Some(server));
}

#[test]
fn a_present_int4_param_echoes_as_int4() {
	run(|client| async move {
		let frames = client.query("MAP { v: $1 }", Some(positional(Value::Int4(5)))).await.unwrap();
		assert_echoed(&frames, ValueType::Int4, Value::Int4(5));

		let frames = client.query("MAP { v: $v }", Some(named(Value::Int4(5)))).await.unwrap();
		assert_echoed(&frames, ValueType::Int4, Value::Int4(5));
		client.close().await.unwrap();
	});
}

#[test]
fn a_none_of_int4_param_echoes_as_a_none_of_int4() {
	run(|client| async move {
		let none = Value::none_of(ValueType::Int4);
		let frames = client.query("MAP { v: $1 }", Some(positional(none.clone()))).await.unwrap();
		assert_echoed(&frames, option(ValueType::Int4), none.clone());

		let frames = client.query("MAP { v: $v }", Some(named(none.clone()))).await.unwrap();
		assert_echoed(&frames, option(ValueType::Int4), none);
		client.close().await.unwrap();
	});
}

#[test]
fn a_none_of_option_int4_param_echoes_as_a_none_of_int4() {
	// The wire carries the two-layer type Option(Option(Int4)), and the server decodes it into
	// a none of Option(Int4); the engine then folds the outer none into the column, so the
	// echoed column is a single-layer Option(Int4) holding a none of Int4.
	run(|client| async move {
		let none = Value::none_of(option(ValueType::Int4));
		let frames = client.query("MAP { v: $1 }", Some(positional(none.clone()))).await.unwrap();
		assert_echoed(&frames, option(ValueType::Int4), Value::none_of(ValueType::Int4));

		let frames = client.query("MAP { v: $v }", Some(named(none))).await.unwrap();
		assert_echoed(&frames, option(ValueType::Int4), Value::none_of(ValueType::Int4));
		client.close().await.unwrap();
	});
}

fn regions() -> Value {
	list_of_records(vec![
		vec![("id", Value::Utf8("us".to_string())), ("label", Value::Utf8("US".to_string()))],
		vec![("id", Value::Utf8("eu".to_string())), ("label", Value::Utf8("EU".to_string()))],
	])
}

fn regions_type() -> ValueType {
	ValueType::List(Box::new(ValueType::Record(vec![
		("id".to_string(), ValueType::Utf8),
		("label".to_string(), ValueType::Utf8),
	])))
}

#[test]
fn a_list_of_records_param_echoes_as_list_of_records() {
	run(|client| async move {
		let frames = client.query("MAP { v: $v }", Some(named_as("v", regions()))).await.unwrap();
		assert_echoed(&frames, regions_type(), regions());
		client.close().await.unwrap();
	});
}

#[test]
fn a_list_of_records_param_inserts_one_row_per_element() {
	run(|client| async move {
		client.admin(
			"CREATE NAMESPACE test; CREATE TABLE test::monitor_regions { id: utf8, label: utf8 }",
			None,
		)
		.await
		.unwrap();

		client.command("INSERT test::monitor_regions $regions", Some(named_as("regions", regions())))
			.await
			.unwrap();

		let frames = client.query("FROM test::monitor_regions", None).await.unwrap();
		assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
		let id_col = frames[0].columns.iter().find(|c| c.name == "id").unwrap();
		let label_col = frames[0].columns.iter().find(|c| c.name == "label").unwrap();
		assert_eq!(id_col.data.len(), 2, "expected one row per list element");

		let mut rows: Vec<(Value, Value)> = (0..id_col.data.len())
			.map(|i| (id_col.data.get_value(i), label_col.data.get_value(i)))
			.collect();
		rows.sort_by_key(|(id, _)| id.to_string());
		assert_eq!(
			rows,
			vec![
				(Value::Utf8("eu".to_string()), Value::Utf8("EU".to_string())),
				(Value::Utf8("us".to_string()), Value::Utf8("US".to_string())),
			]
		);
		client.close().await.unwrap();
	});
}
