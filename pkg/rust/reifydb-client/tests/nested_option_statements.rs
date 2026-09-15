// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg(not(reifydb_single_threaded))]

mod common;

use std::{future::Future, sync::Arc};

use common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port};
use reifydb_client::{GrpcClient, WireFormat};
use tokio::runtime::Runtime;

fn run<F, Fut>(test_fn: F)
where
	F: FnOnce(GrpcClient) -> Fut,
	Fut: Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();
	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");
		test_fn(client).await;
	});
	cleanup_server(Some(server));
}

#[test]
fn a_column_nested_deeper_than_the_type_tag_holds_does_not_take_the_worker_down() {
	run(|client| async move {
		// A panic in the catalog kills the worker, so every later statement on the server times out.
		client.admin("CREATE NAMESPACE deep", None).await.unwrap();
		let result = client
			.admin("CREATE TABLE deep::layers { v: Option(Option(Option(Option(int4)))) }", None)
			.await
			.map(|frames| frames.len());
		assert!(
			!matches!(&result, Err(error) if error.code == "TRANSPORT"),
			"the statement took the server worker down: {result:?}"
		);
	});
}

#[test]
fn a_udf_returning_none_for_a_nested_option_type_does_not_take_the_worker_down() {
	run(|client| async move {
		// A panic while building the return column kills the worker, so every later statement times out.
		let rql =
			"UDF deep ($x: int4): Option(Option(Option(Option(int4)))) { RETURN none }; MAP { v: deep(5) }";
		let result = client.query(rql, None).await.map(|frames| frames.len());
		assert!(
			!matches!(&result, Err(error) if error.code == "TRANSPORT"),
			"the statement took the server worker down: {result:?}"
		);
	});
}
