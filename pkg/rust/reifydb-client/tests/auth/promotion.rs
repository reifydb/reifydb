// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_client::{GrpcClient, HttpClient, WireFormat, WsClient};
use reifydb_value::params::Params;
use tokio::runtime::Runtime;

use crate::{
	auth::start_server_with_auth_users,
	common::{cleanup_server, create_server_instance},
};

fn password(identifier: &str, password: &str) -> HashMap<String, String> {
	HashMap::from([
		("identifier".to_string(), identifier.to_string()),
		("password".to_string(), password.to_string()),
	])
}

#[test]
fn a_guest_signing_up_over_http_keeps_its_identity() {
	// The sdk must carry the guest session into the signup, or the guest work is orphaned.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();
	server.admin_as_root("CREATE AUTHENTICATION METHOD guest WITH { ttl: forever }", Params::None).unwrap();
	server.admin_as_root("ALTER AUTHENTICATION METHOD password WITH { promote_guest: true }", Params::None)
		.unwrap();

	runtime.block_on(async {
		let mut client =
			HttpClient::connect(&format!("http://[::1]:{}", http_port), WireFormat::Frames).await.unwrap();
		let guest = client.login("guest", HashMap::new()).await.unwrap().identity;

		let promoted = client.login("password", password("newbie@x", "pw")).await.unwrap().identity;
		assert_eq!(promoted, guest);
	});

	cleanup_server(Some(server));
}

#[test]
fn a_login_carrying_a_dead_session_is_refused_on_every_transport() {
	// Treating a dead bearer as anonymous would let a stale guest client create a fresh account silently.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (ws_port, grpc_port, http_port) = start_server_with_auth_users(&mut server).unwrap();
	server.admin_as_root("CREATE AUTHENTICATION METHOD guest WITH { ttl: forever }", Params::None).unwrap();

	runtime.block_on(async {
		let mut http =
			HttpClient::connect(&format!("http://[::1]:{}", http_port), WireFormat::Frames).await.unwrap();
		http.authenticate("not-a-session");
		let http_err = http.login("password", password("alice", "alice-pass")).await.unwrap_err().to_string();
		assert!(http_err.contains("401") || http_err.contains("Invalid authentication token"), "{http_err}");

		let mut grpc =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), WireFormat::Rbcf).await.unwrap();
		grpc.authenticate("not-a-session");
		let grpc_err = grpc.login("password", password("alice", "alice-pass")).await.unwrap_err().to_string();
		assert!(
			grpc_err.to_lowercase().contains("unauthenticated")
				|| grpc_err.contains("Invalid authentication token"),
			"{grpc_err}"
		);

		let mut ws = WsClient::connect(&format!("ws://[::1]:{}", ws_port), WireFormat::Frames).await.unwrap();
		let guest_token = ws.login("guest", HashMap::new()).await.unwrap().token;
		let mut revoker =
			HttpClient::connect(&format!("http://[::1]:{}", http_port), WireFormat::Frames).await.unwrap();
		revoker.authenticate(&guest_token);
		revoker.logout().await.unwrap();
		let ws_err = ws.login("password", password("alice", "alice-pass")).await.unwrap_err().to_string();
		assert!(ws_err.contains("AUTH_FAILED"), "{ws_err}");
	});

	cleanup_server(Some(server));
}
