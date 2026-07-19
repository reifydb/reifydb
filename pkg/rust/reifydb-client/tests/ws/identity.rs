// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::Arc,
	time::{SystemTime, UNIX_EPOCH},
};

use reifydb_client::{Frame, Value, WireFormat, WsClient};
use reifydb_value::params::Params;
use tokio::runtime::Runtime;

use crate::common::{cleanup_server, create_server_instance, start_server_and_get_ws_port};

fn column_value(frames: &[Frame], column: &str) -> Value {
	let frame = frames.first().expect("one frame");
	let col = frame.columns.iter().find(|c| c.name == column).unwrap_or_else(|| panic!("column `{column}`"));
	col.data.get_value(0)
}

// Proves the caller identity that authenticated over the WS transport is the identity the called
// procedure observes: alice's call must see alice's id, bob's must see bob's, and the two must
// differ. A regression that dropped identity (e.g. defaulting to root/anonymous) would make the
// two observed ids equal or wrong.
#[test]
fn call_observes_the_authenticated_caller_identity() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	let ns = format!("ident_ws_{suffix}");
	let binding = format!("whoami_ws_{suffix}");
	let alice = format!("alice_{suffix}");
	let bob = format!("bob_{suffix}");
	let alice_token = format!("tok_alice_{suffix}");
	let bob_token = format!("tok_bob_{suffix}");

	server.admin_as_root(&format!("CREATE USER {alice}"), Params::None).unwrap();
	server.admin_as_root(
		&format!("CREATE AUTHENTICATION FOR {alice} {{ method: token; token: '{alice_token}' }}"),
		Params::None,
	)
	.unwrap();
	server.admin_as_root(&format!("CREATE USER {bob}"), Params::None).unwrap();
	server.admin_as_root(
		&format!("CREATE AUTHENTICATION FOR {bob} {{ method: token; token: '{bob_token}' }}"),
		Params::None,
	)
	.unwrap();

	server.admin_as_root(&format!("CREATE NAMESPACE {ns}"), Params::None).unwrap();
	server.admin_as_root(
		&format!("CREATE PROCEDURE {ns}::whoami AS {{ MAP {{ caller: identity::id() }} }}"),
		Params::None,
	)
	.unwrap();
	// Non-privileged callers need a call policy; `filter { true }` admits any authenticated identity.
	server.admin_as_root(
		&format!("CREATE PROCEDURE POLICY ON {ns}::whoami {{ call: {{ filter {{ true }} }} }}"),
		Params::None,
	)
	.unwrap();
	server.admin_as_root(
		&format!("CREATE WS BINDING {ns}::whoami_ws FOR {ns}::whoami WITH {{ name: \"{binding}\" }}"),
		Params::None,
	)
	.unwrap();

	let alice_id = column_value(
		&server.query_as_root(
			&format!("from system::identities filter {{ name == '{alice}' }} map {{ id }}"),
			Params::None,
		)
		.unwrap(),
		"id",
	);
	let bob_id = column_value(
		&server.query_as_root(
			&format!("from system::identities filter {{ name == '{bob}' }} map {{ id }}"),
			Params::None,
		)
		.unwrap(),
		"id",
	);
	assert_ne!(alice_id, bob_id, "distinct users must have distinct ids");

	runtime.block_on(async {
		let mut alice_client =
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		alice_client.authenticate(&alice_token).await.unwrap();
		let observed = column_value(&alice_client.call(&binding, None).await.unwrap(), "caller");
		assert_eq!(observed, alice_id, "procedure must observe alice as the caller");
		alice_client.close().await.unwrap();

		let mut bob_client =
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		bob_client.authenticate(&bob_token).await.unwrap();
		let observed = column_value(&bob_client.call(&binding, None).await.unwrap(), "caller");
		assert_eq!(observed, bob_id, "procedure must observe bob as the caller");
		bob_client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
