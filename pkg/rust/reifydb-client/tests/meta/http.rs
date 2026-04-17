// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::{HttpClient, WireFormat};
use tokio::runtime::Runtime;

use crate::common::{cleanup_server, create_server_instance, start_server_and_get_http_port};

fn run_with_format<F, Fut>(format: WireFormat, test_fn: F)
where
	F: FnOnce(HttpClient) -> Fut,
	Fut: std::future::Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_http_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = HttpClient::connect(&format!("http://[::1]:{}", port), format).await.unwrap();
		client.authenticate("mysecrettoken");
		test_fn(client).await;
	});

	cleanup_server(Some(server));
}

use super::assert_duration;

#[test]
fn admin_with_meta_json() {
	run_with_format(WireFormat::Json, |client| async move {
		let result = client.admin_with_meta(";", None).await.unwrap();
		let meta = result.meta.expect("meta should be populated");
		assert_eq!(meta.fingerprint, "0x99aa06d3014798d86001c324468d497f");
		assert_duration(&meta.duration);
	});
}

#[test]
fn command_with_meta_json() {
	run_with_format(WireFormat::Json, |client| async move {
		let result = client.command_with_meta(";", None).await.unwrap();
		let meta = result.meta.expect("meta should be populated");
		assert_eq!(meta.fingerprint, "0x99aa06d3014798d86001c324468d497f");
		assert_duration(&meta.duration);
	});
}

#[test]
fn query_with_meta_json() {
	run_with_format(WireFormat::Json, |client| async move {
		let result = client.query_with_meta("MAP {v: 1}", None).await.unwrap();
		let meta = result.meta.expect("meta should be populated");
		assert_eq!(meta.fingerprint, "0x2090d86891040dcd68a505f1a1ae93f9");
		assert_duration(&meta.duration);
		assert_eq!(result.frames.len(), 1);
	});
}

#[test]
fn admin_with_meta_rbcf() {
	run_with_format(WireFormat::Rbcf, |client| async move {
		let result = client.admin_with_meta(";", None).await.unwrap();
		let meta = result.meta.expect("meta should be populated");
		assert_eq!(meta.fingerprint, "0x99aa06d3014798d86001c324468d497f");
		assert_duration(&meta.duration);
	});
}

#[test]
fn command_with_meta_rbcf() {
	run_with_format(WireFormat::Rbcf, |client| async move {
		let result = client.command_with_meta(";", None).await.unwrap();
		let meta = result.meta.expect("meta should be populated");
		assert_eq!(meta.fingerprint, "0x99aa06d3014798d86001c324468d497f");
		assert_duration(&meta.duration);
	});
}

#[test]
fn query_with_meta_rbcf() {
	run_with_format(WireFormat::Rbcf, |client| async move {
		let result = client.query_with_meta("MAP {v: 1}", None).await.unwrap();
		let meta = result.meta.expect("meta should be populated");
		assert_eq!(meta.fingerprint, "0x2090d86891040dcd68a505f1a1ae93f9");
		assert_duration(&meta.duration);
		assert_eq!(result.frames.len(), 1);
	});
}

#[test]
fn bare_query_drops_meta() {
	run_with_format(WireFormat::Json, |client| async move {
		let frames = client.query("MAP {v: 1}", None).await.unwrap();
		assert_eq!(frames.len(), 1);
	});
}
