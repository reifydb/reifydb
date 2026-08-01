// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod grpc;
mod http;
mod ws;

use std::error::Error;

use reifydb::Database;

/// Creates root (token 'mysecrettoken'), alice (password 'alice-pass') and bob (token
/// 'bob-secret-token'). Returns (ws_port, grpc_port, http_port).
pub fn start_server_with_auth_users(server: &mut Database) -> Result<(u16, u16, u16), Box<dyn Error>> {
	let params = reifydb_value::params::Params::None;

	server.admin_as_root(
		"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
		params.clone(),
	)?;

	server.admin_as_root("CREATE USER alice", params.clone())?;
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }",
		params.clone(),
	)?;

	server.admin_as_root("CREATE USER bob", params.clone())?;
	server.admin_as_root("CREATE AUTHENTICATION FOR bob { method: token; token: 'bob-secret-token' }", params)?;

	let ws_port = server.sub_server_ws().unwrap().admin_port().unwrap();
	let grpc_port = server.sub_server_grpc().unwrap().admin_port().unwrap();
	let http_port = server.sub_server_http().unwrap().admin_port().unwrap();
	Ok((ws_port, grpc_port, http_port))
}
