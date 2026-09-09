// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_client::{HttpClient, Value, ValueType, WireFormat};
use tokio::runtime::Runtime;

use super::{assert_echoed, named, option, positional};
use crate::common::{cleanup_server, create_server_instance, start_server_and_get_http_port};

fn run<F, Fut>(test_fn: F)
where
	F: FnOnce(HttpClient) -> Fut,
	Fut: std::future::Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_http_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			HttpClient::connect(&format!("http://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken");
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
	});
}
