// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::Arc,
	time::{SystemTime, UNIX_EPOCH},
};

use reifydb_client::{GrpcClient, Params, Value, WireFormat};
use tokio::runtime::Runtime;

use crate::common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port};

struct Fixture {
	greet: String,
	echo: String,
	missing: String,
}

fn run<F, Fut>(test_fn: F)
where
	F: FnOnce(GrpcClient, Fixture) -> Fut,
	Fut: std::future::Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	// Binding names are globally unique, so a per-run suffix keeps repeated runs against
	// a reused server from colliding.
	let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	let ns = format!("call_grpc_{suffix}");
	let fixture = Fixture {
		greet: format!("greet_{suffix}"),
		echo: format!("echo_{suffix}"),
		missing: format!("missing_{suffix}"),
	};

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Rbcf).await.unwrap();
		client.authenticate("mysecrettoken");

		client.admin(&format!("CREATE NAMESPACE {ns}"), None).await.unwrap();
		client.admin(&format!("CREATE PROCEDURE {ns}::greet AS {{ MAP {{ result: 42 }} }}"), None)
			.await
			.unwrap();
		client.admin(&format!("CREATE PROCEDURE {ns}::echo {{ n: int4 }} AS {{ MAP {{ out: $n }} }}"), None)
			.await
			.unwrap();
		client.admin(
			&format!(
				"CREATE GRPC BINDING {ns}::greet_grpc FOR {ns}::greet WITH {{ name: \"{}\" }}",
				fixture.greet
			),
			None,
		)
		.await
		.unwrap();
		client.admin(
			&format!(
				"CREATE GRPC BINDING {ns}::echo_grpc FOR {ns}::echo WITH {{ name: \"{}\" }}",
				fixture.echo
			),
			None,
		)
		.await
		.unwrap();

		test_fn(client, fixture).await;
	});

	cleanup_server(Some(server));
}

fn named_n(n: i32) -> Params {
	Params::Named(Arc::new(HashMap::from([("n".to_string(), Value::Int4(n))])))
}

#[test]
fn call_zero_param_binding_returns_procedure_frame() {
	run(|client, fx| async move {
		let frames = client.call(&fx.greet, None).await.unwrap();
		assert_eq!(frames.len(), 1);
		assert!(frames[0].to_string().contains("42"));
	});
}

#[test]
fn call_passes_named_params_through() {
	run(|client, fx| async move {
		// 12345 appears only if the param reached the procedure body; a dropped
		// param would surface an INVALID_PARAMS error instead of this value.
		let frames = client.call(&fx.echo, Some(named_n(12345))).await.unwrap();
		assert!(frames[0].to_string().contains("12345"));
	});
}

#[test]
fn call_with_meta_populates_meta() {
	run(|client, fx| async move {
		let result = client.call_with_meta(&fx.greet, None).await.unwrap();
		let meta = result.meta.expect("meta should be populated");
		assert!(!meta.duration.is_empty());
		assert_eq!(result.frames.len(), 1);
	});
}

#[test]
fn call_missing_required_param_errors() {
	run(|client, fx| async move {
		// A required-but-omitted param must be rejected server-side, not silently defaulted.
		// The gRPC server now sends the structured Diagnostic (parity with WS), so the client
		// recovers the INVALID_PARAMS code rather than a generic TRANSPORT error.
		let err = client.call(&fx.echo, None).await.unwrap_err();
		assert_eq!(err.code, "INVALID_PARAMS");
		assert!(err.message.contains("missing required parameter"), "message was: {}", err.message);
	});
}

#[test]
fn call_unknown_binding_errors() {
	run(|client, fx| async move {
		// An unresolved binding name must surface as NOT_FOUND, distinct from a param error.
		let err = client.call(&fx.missing, None).await.unwrap_err();
		assert_eq!(err.code, "NOT_FOUND");
		assert!(err.message.contains("no gRPC binding named"), "message was: {}", err.message);
	});
}
