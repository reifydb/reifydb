// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
// This file is licensed under the MIT, see license.md file

use std::{collections::HashMap, error::Error, fmt::Write, sync::Arc};

use reifydb::{Database, server, sub_server_http::factory::HttpConfig, sub_server_ws::factory::WsConfig};
use reifydb_client::{Frame, Params, Value};
use reifydb_testing::testscript::command::Command;
use tokio::runtime::Runtime;

pub fn create_server_instance(_runtime: &Arc<Runtime>) -> Database {
	server::memory()
		.with_http(HttpConfig::default().bind_addr("::1:0"))
		.with_ws(WsConfig::default().bind_addr("::1:0"))
		.build()
		.unwrap()
}

/// Start server and return WebSocket port
#[allow(dead_code)]
pub fn start_server_and_get_ws_port(_runtime: &Arc<Runtime>, server: &mut Database) -> Result<u16, Box<dyn Error>> {
	server.start()?;
	Ok(server.sub_server_ws().unwrap().port().unwrap())
}

/// Start server and return HTTP port
#[allow(dead_code)]
pub fn start_server_and_get_http_port(_runtime: &Arc<Runtime>, server: &mut Database) -> Result<u16, Box<dyn Error>> {
	server.start()?;
	Ok(server.sub_server_http().unwrap().port().unwrap())
}

/// Parse RQL command from testscript Command
pub fn parse_rql(command: &Command) -> String {
	command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ")
}

/// Parse positional parameters from command arguments
/// First argument is the SQL, rest are positional parameters
pub fn parse_positional_params(command: &Command) -> (String, Params) {
	let args: Vec<&str> = command.args.iter().map(|a| a.value.as_str()).collect();

	if args.is_empty() {
		return (String::new(), Params::Positional(vec![]));
	}

	let sql = args[0].to_string();
	let params = args[1..].iter().map(|s| parse_param_value(s)).collect();

	(sql, Params::Positional(params))
}

/// Parse named parameters from command arguments
/// First argument is the SQL, rest are name=value pairs
pub fn parse_named_params(command: &Command) -> (String, Params) {
	let args: Vec<&str> = command.args.iter().map(|a| a.value.as_str()).collect();

	if args.is_empty() {
		return (String::new(), Params::Named(HashMap::new()));
	}

	let sql = args[0].to_string();
	let mut params = HashMap::new();

	for arg in &args[1..] {
		if let Some((name, value)) = arg.split_once('=') {
			params.insert(name.to_string(), parse_param_value(value));
		}
	}

	(sql, Params::Named(params))
}

/// Parse a parameter value from string
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
pub fn write_frames(frames: Vec<Frame>) -> Result<String, Box<dyn Error>> {
	let mut output = String::new();
	for frame in frames {
		writeln!(output, "{}", frame).unwrap();
	}
	Ok(output)
}

/// Clean up server instance
pub fn cleanup_server(mut server: Option<Database>) {
	if let Some(mut srv) = server.take() {
		let _ = srv.stop();
		drop(srv);
	}
}
