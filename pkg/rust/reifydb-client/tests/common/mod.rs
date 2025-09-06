// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, error::Error, fmt::Write, net::ToSocketAddrs};

use reifydb::{
	Database, ServerBuilder,
	core::{
		event::EventBus,
		interface::{
			CdcTransaction, UnversionedTransaction,
			VersionedTransaction,
		},
	},
	sub_server::{NetworkConfig, ServerConfig},
};
use reifydb_client::{Client, Frame, HttpClient, Params, Value, WsClient};
use reifydb_testing::testscript::Command;

pub fn create_server_instance<VT, UT, C>(
	input: (VT, UT, C, EventBus),
) -> Database<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	let (versioned, unversioned, cdc, eventbus) = input;
	// Use only 1 worker for tests to avoid file descriptor exhaustion
	let network_config = NetworkConfig {
		workers: Some(1), // Limit to 1 worker for tests
		..Default::default()
	};
	ServerBuilder::new(versioned, unversioned, cdc, eventbus)
		.with_config(
			ServerConfig::new()
				.bind_addr("::1:0")
				.network(network_config),
		)
		.build()
		.unwrap()
}

/// Start server and return WebSocket port
pub fn start_server_and_get_port<VT, UT, C>(
	server: &mut Database<VT, UT, C>,
) -> Result<u16, Box<dyn Error>>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	server.start()?;
	Ok(server.sub_server().unwrap().port().unwrap())
}

#[allow(dead_code)]
pub fn connect_ws<A: ToSocketAddrs>(
	addr: A,
) -> Result<WsClient, Box<dyn Error>> {
	Client::ws(addr)
}

#[allow(dead_code)]
pub fn connect_http<A: ToSocketAddrs>(
	addr: A,
) -> Result<HttpClient, Box<dyn Error>> {
	Client::http(addr)
}

/// Parse RQL command from testscript Command
pub fn parse_rql(command: &Command) -> String {
	command.args
		.iter()
		.map(|a| a.value.as_str())
		.collect::<Vec<_>>()
		.join(" ")
}

/// Parse positional parameters from command arguments
/// First argument is the SQL, rest are positional parameters
pub fn parse_positional_params(command: &Command) -> (String, Params) {
	let args: Vec<&str> =
		command.args.iter().map(|a| a.value.as_str()).collect();

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
	let args: Vec<&str> =
		command.args.iter().map(|a| a.value.as_str()).collect();

	if args.is_empty() {
		return (String::new(), Params::Named(HashMap::new()));
	}

	let sql = args[0].to_string();
	let mut params = HashMap::new();

	for arg in &args[1..] {
		if let Some((name, value)) = arg.split_once('=') {
			params.insert(
				name.to_string(),
				parse_param_value(value),
			);
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

#[allow(dead_code)]
pub fn cleanup_ws_client(client: Option<WsClient>) {
	if let Some(client) = client {
		let _ = client.close();
	}
}

#[allow(dead_code)]
pub fn cleanup_http_client(client: Option<HttpClient>) {
	// HTTP clients don't maintain persistent connections
	// so no cleanup needed
	drop(client);
}

/// Clean up server instance
pub fn cleanup_server<VT, UT, C>(mut server: Option<Database<VT, UT, C>>)
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	if let Some(mut srv) = server.take() {
		let _ = srv.stop();
		drop(srv);
	}
}
