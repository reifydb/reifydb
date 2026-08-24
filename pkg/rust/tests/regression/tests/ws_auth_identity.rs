// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, time::Duration as StdDuration};

use reifydb::{RuntimeConfig, server};
use reifydb_client::{WireFormat, WsClient, subscription::SubscriptionConfig};
use reifydb_value::params::Params;
use tokio::{runtime::Runtime, time::timeout};

#[test]
fn failed_auth_must_not_subscribe_as_root() {
	// A failed auth clears conn.identity to None; subscribe then resolves None to root and bypasses policy.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();

	let mut instance = server::memory()
		.with_runtime_config(RuntimeConfig::default().seeded(0))
		.with_ws(|ws| ws.bind_addr("::1:0").admin_bind_addr("::1:0"))
		.build()
		.unwrap();

	instance.admin_as_root("CREATE AUTHENTICATION FOR root { method: token; token: 'goodtoken' }", Params::None)
		.unwrap();
	instance.admin_as_root("CREATE NAMESPACE test", Params::None).unwrap();
	instance.admin_as_root("CREATE TABLE test::t { id: int4 }", Params::None).unwrap();
	// Denies every non-privileged read, so any row reaching the client proves the caller was privileged.
	instance.admin_as_root("CREATE TABLE POLICY deny ON test::t { from: { filter { false } } }", Params::None)
		.unwrap();
	instance.command_as_root("INSERT test::t [{ id: 1 }]", Params::None).unwrap();

	let root_rows: usize =
		instance.query_as_root("FROM test::t", Params::None).unwrap().iter().map(|f| f.rows().count()).sum();
	assert_eq!(root_rows, 1, "control: root must see the row, otherwise the test proves nothing");

	let port = instance.sub_server_ws().unwrap().port().unwrap();

	let rows = runtime.block_on(async move {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();

		let auth = client.authenticate("badtoken").await;
		assert!(auth.is_err(), "a bad token must fail authentication, got: {:?}", auth);

		let sub = client.subscribe("FROM test::t", SubscriptionConfig::default()).await;
		let Ok(_id) = sub else {
			return 0;
		};

		match timeout(StdDuration::from_secs(3), client.recv()).await {
			Ok(Some(payload)) => payload.changes.iter().map(|c| c.frame.rows().count()).sum::<usize>(),
			Ok(None) => 0,
			Err(_) => 0,
		}
	});

	assert_eq!(rows, 0, "an unauthenticated subscriber must not receive policy-protected rows");

	let _ = instance.stop();
}
