// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::Arc,
	time::{SystemTime, UNIX_EPOCH},
};

use reifydb_client::{Params, Value, WireFormat, WsClient};
use tokio::runtime::Runtime;

use crate::common::{cleanup_server, create_server_instance, start_server_and_get_ws_port};

struct Fixture {
	greet: String,
	greet_rbcf: String,
	echo: String,
	missing: String,
}

fn run<F, Fut>(test_fn: F)
where
	F: FnOnce(WsClient, Fixture, String) -> Fut,
	Fut: std::future::Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
	let url = format!("ws://[::1]:{}", port);

	// Binding names are globally unique, so a per-run suffix keeps repeated runs against
	// a reused server from colliding.
	let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	let ns = format!("call_ws_{suffix}");
	let fixture = Fixture {
		greet: format!("greet_{suffix}"),
		greet_rbcf: format!("greet_rbcf_{suffix}"),
		echo: format!("echo_{suffix}"),
		missing: format!("missing_{suffix}"),
	};

	runtime.block_on(async {
		let mut client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		client.admin(&format!("CREATE NAMESPACE {ns}"), None).await.unwrap();
		client.admin(&format!("CREATE PROCEDURE {ns}::greet AS {{ MAP {{ result: 42 }} }}"), None)
			.await
			.unwrap();
		client.admin(&format!("CREATE PROCEDURE {ns}::echo {{ n: int4 }} AS {{ MAP {{ out: $n }} }}"), None)
			.await
			.unwrap();
		// Default binding format is frames (text path); the rbcf binding drives the binary path.
		client.admin(
			&format!(
				"CREATE WS BINDING {ns}::greet_ws FOR {ns}::greet WITH {{ name: \"{}\" }}",
				fixture.greet
			),
			None,
		)
		.await
		.unwrap();
		client.admin(
			&format!(
				"CREATE WS BINDING {ns}::greet_rbcf_ws FOR {ns}::greet WITH {{ name: \"{}\", format: \"rbcf\" }}",
				fixture.greet_rbcf
			),
			None,
		)
		.await
		.unwrap();
		client.admin(
			&format!(
				"CREATE WS BINDING {ns}::echo_ws FOR {ns}::echo WITH {{ name: \"{}\" }}",
				fixture.echo
			),
			None,
		)
		.await
		.unwrap();

		test_fn(client, fixture, url).await;
	});

	cleanup_server(Some(server));
}

fn named_n(n: i32) -> Params {
	Params::Named(Arc::new(HashMap::from([("n".to_string(), Value::Int4(n))])))
}

#[test]
fn call_zero_param_binding_returns_procedure_frame() {
	run(|client, fx, _url| async move {
		let frames = client.call(&fx.greet, None).await.unwrap();
		assert_eq!(frames.len(), 1);
		assert!(frames[0].to_string().contains("42"));
		client.close().await.unwrap();
	});
}

#[test]
fn call_passes_named_params_through() {
	run(|client, fx, _url| async move {
		// 12345 appears only if the param reached the procedure body; a dropped
		// param would surface an INVALID_PARAMS error instead of this value.
		let frames = client.call(&fx.echo, Some(named_n(12345))).await.unwrap();
		assert!(frames[0].to_string().contains("12345"));
		client.close().await.unwrap();
	});
}

#[test]
fn rbcf_client_and_frames_binding_interop() {
	// Cross-format interop guard: an rbcf client calling a frames-format binding must still get a
	// correctly decoded result. The Rust client decodes both wire encodings transparently, so this
	// asserts interop, not which encoding was chosen; the observable proof that the server honors
	// the requested format lives in the TypeScript wireformat tests (a json-only client there fails
	// against a non-json binding without the server-side format honoring).
	run(|_client, fx, url| async move {
		let mut rbcf_client = WsClient::connect(&url, WireFormat::Rbcf).await.unwrap();
		rbcf_client.authenticate("mysecrettoken").await.unwrap();
		let frames = rbcf_client.call(&fx.greet, None).await.unwrap();
		assert_eq!(frames.len(), 1);
		assert!(frames[0].to_string().contains("42"));
		rbcf_client.close().await.unwrap();
	});
}

#[test]
fn frames_client_and_rbcf_binding_interop() {
	// The reverse direction: a frames client calling an rbcf-format binding still decodes.
	run(|client, fx, _url| async move {
		let frames = client.call(&fx.greet_rbcf, None).await.unwrap();
		assert_eq!(frames.len(), 1);
		assert!(frames[0].to_string().contains("42"));
		client.close().await.unwrap();
	});
}

#[test]
fn call_with_meta_populates_meta() {
	run(|client, fx, _url| async move {
		let result = client.call_with_meta(&fx.greet, None).await.unwrap();
		let meta = result.meta.expect("meta should be populated");
		assert!(!meta.fingerprint.is_empty());
		assert!(!meta.duration.is_empty());
		assert_eq!(result.frames.len(), 1);
		client.close().await.unwrap();
	});
}

#[test]
fn call_missing_required_param_errors() {
	run(|client, fx, _url| async move {
		// A required-but-omitted param must be rejected server-side, not silently defaulted.
		let err = client.call(&fx.echo, None).await.unwrap_err();
		assert_eq!(err.code, "INVALID_PARAMS");
		assert!(err.message.contains("missing required parameter"), "message was: {}", err.message);
		client.close().await.unwrap();
	});
}

#[test]
fn call_unknown_binding_errors() {
	run(|client, fx, _url| async move {
		// An unresolved binding name must surface as NOT_FOUND, distinct from a param error.
		let err = client.call(&fx.missing, None).await.unwrap_err();
		assert_eq!(err.code, "NOT_FOUND");
		assert!(err.message.contains("no WS binding named"), "message was: {}", err.message);
		client.close().await.unwrap();
	});
}
