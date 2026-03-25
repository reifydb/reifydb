// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod grpc;
mod http;
mod ws;

use std::error::Error;

use reifydb::Database;

/// Start a server with auth users configured for login testing.
///
/// Creates:
/// - root with token auth ('mysecrettoken')
/// - alice with password auth ('alice-pass')
/// - bob with token auth ('bob-secret-token')
///
/// Returns (ws_port, grpc_port, http_port).
pub fn start_server_with_auth_users(server: &mut Database) -> Result<(u16, u16, u16), Box<dyn Error>> {
	server.start()?;

	let params = reifydb_type::params::Params::None;

	// Root token auth (for admin setup)
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
		params.clone(),
	)?;

	// Password user
	server.admin_as_root("CREATE USER alice", params.clone())?;
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }",
		params.clone(),
	)?;

	// Token user
	server.admin_as_root("CREATE USER bob", params.clone())?;
	server.admin_as_root("CREATE AUTHENTICATION FOR bob { method: token; token: 'bob-secret-token' }", params)?;

	let ws_port = server.sub_server_ws().unwrap().admin_port().unwrap();
	let grpc_port = server.sub_server_grpc().unwrap().admin_port().unwrap();
	let http_port = server.sub_server_http().unwrap().admin_port().unwrap();
	Ok((ws_port, grpc_port, http_port))
}
