// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, error::Error, fmt::Write, sync::Arc};

// === Native (network) test helpers ===
#[cfg(not(reifydb_single_threaded))]
use reifydb::{Database, SharedRuntimeConfig, WithSubsystem, server};
use reifydb_client::{Frame, Params, Value};
use reifydb_testing::testscript::command::Command;
#[cfg(not(reifydb_single_threaded))]
use tokio::runtime::Runtime;

#[cfg(not(reifydb_single_threaded))]
pub fn create_server_instance(_runtime: &Arc<Runtime>) -> Database {
	server::memory()
		.with_runtime_config(SharedRuntimeConfig::default().seeded(0))
		.with_flow(|f| f)
		.with_grpc(|grpc| grpc.admin_bind_addr("[::1]:0"))
		.with_http(|http| http.admin_bind_addr("::1:0"))
		.with_ws(|ws| ws.admin_bind_addr("::1:0"))
		.build()
		.unwrap()
}

/// Start server and return WebSocket admin port
#[allow(dead_code)]
#[cfg(not(reifydb_single_threaded))]
pub fn start_server_and_get_ws_port(_runtime: &Arc<Runtime>, server: &mut Database) -> Result<u16, Box<dyn Error>> {
	server.start()?;
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
		reifydb_type::params::Params::None,
	)
	.unwrap();
	Ok(server.sub_server_ws().unwrap().admin_port().unwrap())
}

/// Start server and return gRPC admin port
#[allow(dead_code)]
#[cfg(not(reifydb_single_threaded))]
pub fn start_server_and_get_grpc_port(_runtime: &Arc<Runtime>, server: &mut Database) -> Result<u16, Box<dyn Error>> {
	server.start()?;
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
		reifydb_type::params::Params::None,
	)
	.unwrap();
	Ok(server.sub_server_grpc().unwrap().admin_port().unwrap())
}

/// Start server and return HTTP admin port
#[allow(dead_code)]
#[cfg(not(reifydb_single_threaded))]
pub fn start_server_and_get_http_port(_runtime: &Arc<Runtime>, server: &mut Database) -> Result<u16, Box<dyn Error>> {
	server.start()?;
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
		reifydb_type::params::Params::None,
	)
	.unwrap();
	Ok(server.sub_server_http().unwrap().admin_port().unwrap())
}

/// Clean up server instance
#[cfg(not(reifydb_single_threaded))]
pub fn cleanup_server(mut server: Option<Database>) {
	if let Some(mut srv) = server.take() {
		let _ = srv.stop();
		drop(srv);
	}
}

// === DST test helpers ===

#[cfg(reifydb_single_threaded)]
use reifydb::{Database, SharedRuntimeConfig, embedded};
#[cfg(reifydb_single_threaded)]
use reifydb_client::DstClient;
#[cfg(reifydb_single_threaded)]
use reifydb_core::actors::server::{ServerAuthResponse, ServerMessage, ServerResponse};
#[cfg(reifydb_single_threaded)]
use reifydb_runtime::actor::system::{ActorHandle, ActorSystem};
#[cfg(reifydb_single_threaded)]
use reifydb_sub_server::actor::ServerActor;
#[cfg(reifydb_single_threaded)]
use reifydb_type::value::identity::IdentityId;

#[cfg(reifydb_single_threaded)]
pub struct DstTestContext {
	pub db: Database,
	pub system: ActorSystem,
	pub identity: IdentityId,
	_handle: ActorHandle<ServerMessage>,
	pub client: DstClient,
}

#[cfg(reifydb_single_threaded)]
impl DstTestContext {
	pub fn new() -> Self {
		let db = embedded::memory()
			.with_runtime_config(SharedRuntimeConfig::default().seeded(0))
			.build()
			.unwrap();

		db.admin_as_root(
			"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
			reifydb_type::params::Params::None,
		)
		.unwrap();

		let engine = db.engine().clone();
		let auth_service = db.auth_service().clone();
		let system = db.shared_runtime().actor_system();
		let clock = db.shared_runtime().clock().clone();

		let handle = system.spawn_query("server", ServerActor::new(engine, auth_service, clock));
		let client = DstClient::new(handle.actor_ref().clone(), system.clone());

		// Authenticate to get identity
		let auth_response = client.authenticate(
			"token".to_string(),
			HashMap::from([("token".to_string(), "mysecrettoken".to_string())]),
		);
		let identity = match auth_response {
			ServerAuthResponse::Authenticated {
				identity,
				..
			} => identity,
			ServerAuthResponse::Failed {
				reason,
			} => panic!("authentication failed: {}", reason),
			ServerAuthResponse::Error(e) => panic!("authentication error: {}", e),
			ServerAuthResponse::Challenge {
				..
			} => panic!("unexpected challenge response"),
		};

		Self {
			db,
			system,
			identity,
			_handle: handle,
			client,
		}
	}
}

#[cfg(reifydb_single_threaded)]
pub fn dst_response_to_result(response: ServerResponse) -> Result<Vec<Frame>, Box<dyn Error>> {
	match response {
		ServerResponse::Success {
			frames,
			..
		} => Ok(frames),
		ServerResponse::EngineError {
			diagnostic,
			..
		} => {
			let err = reifydb_type::error::Error(diagnostic);
			Err(err.to_string().into())
		}
	}
}

// === Shared helpers (used by both native and DST) ===

/// Parse RQL command from testscript Command
#[allow(dead_code)]
pub fn parse_rql(command: &Command) -> String {
	command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ")
}

/// Parse positional parameters from command arguments
/// First argument is the RQL, rest are positional parameters
#[allow(dead_code)]
pub fn parse_positional_params(command: &Command) -> (String, Params) {
	let args: Vec<&str> = command.args.iter().map(|a| a.value.as_str()).collect();

	if args.is_empty() {
		return (String::new(), Params::Positional(Arc::new(vec![])));
	}

	let rql = args[0].to_string();
	let params: Vec<_> = args[1..].iter().map(|s| parse_param_value(s)).collect();

	(rql, Params::Positional(Arc::new(params)))
}

/// Parse named parameters from command arguments
/// First argument is the RQL, rest are name=value pairs
#[allow(dead_code)]
pub fn parse_named_params(command: &Command) -> (String, Params) {
	let args: Vec<&str> = command.args.iter().map(|a| a.value.as_str()).collect();

	if args.is_empty() {
		return (String::new(), Params::Named(Arc::new(HashMap::new())));
	}

	let rql = args[0].to_string();
	let mut params = HashMap::new();

	for arg in &args[1..] {
		if let Some((name, value)) = arg.split_once('=') {
			params.insert(name.to_string(), parse_param_value(value));
		}
	}

	(rql, Params::Named(Arc::new(params)))
}

/// Parse a parameter value from string
#[allow(dead_code)]
fn parse_param_value(s: &str) -> Value {
	// Try to parse as number first
	if let Ok(i) = s.parse::<i32>() {
		return Value::Int4(i);
	}
	if let Ok(i) = s.parse::<i64>() {
		return Value::Int8(i);
	}
	if let Ok(f) = s.parse::<f64>() {
		if let Ok(ordered) = reifydb_client::OrderedF64::try_from(f) {
			return Value::Float8(ordered);
		}
	}

	// Handle boolean
	if s == "true" {
		return Value::Boolean(true);
	}
	if s == "false" {
		return Value::Boolean(false);
	}

	// Handle quoted strings
	if s.starts_with('\'') && s.ends_with('\'') && s.len() > 1 {
		return Value::Utf8(s[1..s.len() - 1].to_string());
	}
	if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
		return Value::Utf8(s[1..s.len() - 1].to_string());
	}

	// Default to string
	Value::Utf8(s.to_string())
}

/// Write frames to output string
#[allow(dead_code)]
pub fn write_frames(frames: Vec<Frame>) -> Result<String, Box<dyn Error>> {
	let mut output = String::new();
	for frame in frames {
		writeln!(output, "{}", frame).unwrap();
	}
	Ok(output)
}
