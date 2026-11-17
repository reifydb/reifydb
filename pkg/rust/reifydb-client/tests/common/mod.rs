// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, error::Error, fmt::Write, sync::Arc};

#[cfg(not(reifydb_single_threaded))]
use reifydb::{Database, RuntimeConfig, WithSubsystem, server};
use reifydb_client::{Frame, Params, Value};
use reifydb_codec::json::from::parse_json_value;
use reifydb_sub_server::wire::resolve_type;
use reifydb_testing::testscript::command::Command;
#[cfg(not(reifydb_single_threaded))]
use tokio::runtime::Runtime;

#[cfg(not(reifydb_single_threaded))]
pub fn create_server_instance(_runtime: &Arc<Runtime>) -> Database {
	// Cargo unifies reqwest's `rustls-no-provider` feature across the workspace, so reqwest
	// expects a process-wide provider that only uptime's own `main` installs. Install ring
	// once; an error on later calls just means it is already installed.
	let _ = rustls::crypto::ring::default_provider().install_default();
	server::memory()
		.with_runtime_config(RuntimeConfig::default().seeded(0))
		.with_flow(|f| f)
		.with_grpc(|grpc| grpc.admin_bind_addr("[::1]:0"))
		.with_http(|http| http.admin_bind_addr("::1:0"))
		.with_ws(|ws| ws.admin_bind_addr("::1:0"))
		.build()
		.unwrap()
}

/// Throttle and linger gates compare against `clock.now()`; the default seeded clock is frozen
/// at 0, so a window never elapses on its own. Handing in a clock the test also holds lets it
/// step time forward on demand.
#[allow(dead_code)]
#[cfg(not(reifydb_single_threaded))]
pub fn create_server_instance_with_clock(
	_runtime: &Arc<Runtime>,
	clock: reifydb::runtime::context::clock::MockClock,
) -> Database {
	let mut config = RuntimeConfig::default().seeded(0);
	config.clock = reifydb::runtime::context::clock::Clock::Mock(clock);
	server::memory()
		.with_runtime_config(config)
		.with_flow(|f| f)
		.with_grpc(|grpc| grpc.admin_bind_addr("[::1]:0"))
		.with_http(|http| http.admin_bind_addr("::1:0"))
		.with_ws(|ws| ws.admin_bind_addr("::1:0"))
		.build()
		.unwrap()
}

#[allow(dead_code)]
#[cfg(not(reifydb_single_threaded))]
pub fn start_server_and_get_ws_port(_runtime: &Arc<Runtime>, server: &mut Database) -> Result<u16, Box<dyn Error>> {
	server.admin_as_root(
		"CREATE AUTHENTICATION METHOD token WITH { ttl: forever }",
		reifydb_value::params::Params::None,
	)
	.unwrap();
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
		reifydb_value::params::Params::None,
	)
	.unwrap();
	Ok(server.sub_server_ws().unwrap().admin_port().unwrap())
}

#[allow(dead_code)]
#[cfg(not(reifydb_single_threaded))]
pub fn start_server_and_get_grpc_port(_runtime: &Arc<Runtime>, server: &mut Database) -> Result<u16, Box<dyn Error>> {
	server.admin_as_root(
		"CREATE AUTHENTICATION METHOD token WITH { ttl: forever }",
		reifydb_value::params::Params::None,
	)
	.unwrap();
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
		reifydb_value::params::Params::None,
	)
	.unwrap();
	Ok(server.sub_server_grpc().unwrap().admin_port().unwrap())
}

#[allow(dead_code)]
#[cfg(not(reifydb_single_threaded))]
pub fn start_server_and_get_http_port(_runtime: &Arc<Runtime>, server: &mut Database) -> Result<u16, Box<dyn Error>> {
	server.admin_as_root(
		"CREATE AUTHENTICATION METHOD token WITH { ttl: forever }",
		reifydb_value::params::Params::None,
	)
	.unwrap();
	server.admin_as_root(
		"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
		reifydb_value::params::Params::None,
	)
	.unwrap();
	Ok(server.sub_server_http().unwrap().admin_port().unwrap())
}

#[cfg(not(reifydb_single_threaded))]
pub fn cleanup_server(mut server: Option<Database>) {
	if let Some(mut srv) = server.take() {
		let _ = srv.stop();
		drop(srv);
	}
}

#[allow(dead_code)]
pub fn parse_rql(command: &Command) -> String {
	command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ")
}

/// First argument is the RQL, the rest are positional parameters.
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

/// First argument is the RQL, the rest are `name=value` pairs.
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

#[allow(dead_code)]
fn parse_typed_param(s: &str) -> Option<Value> {
	let (text, ty) = s.rsplit_once("::")?;
	let id = match ty {
		"bool" => "Boolean",
		"int1" => "Int1",
		"int2" => "Int2",
		"int4" => "Int4",
		"int8" => "Int8",
		"int16" => "Int16",
		"uint1" => "Uint1",
		"uint2" => "Uint2",
		"uint4" => "Uint4",
		"uint8" => "Uint8",
		"uint16" => "Uint16",
		"float4" => "Float4",
		"float8" => "Float8",
		"decimal" => "Decimal",
		"utf8" => "Utf8",
		"blob" => "Blob",
		"date" => "Date",
		"time" => "Time",
		"datetime" => "DateTime",
		"duration" => "Duration",
		"uuid4" => "Uuid4",
		"uuid7" => "Uuid7",
		"identity_id" => "IdentityId",
		_ => return None,
	};
	let text_json = serde_json::Value::String(text.to_string());
	let ty = resolve_type(&serde_json::json!({ "id": id }), &text_json)
		.unwrap_or_else(|e| panic!("typed param {s}: unknown type: {e}"));
	if text == "none" {
		return Some(Value::none_of(ty));
	}
	Some(parse_json_value(&ty, &text_json).unwrap_or_else(|e| panic!("typed param {s}: {e}")))
}

#[allow(dead_code)]
fn parse_param_value(s: &str) -> Value {
	let trimmed = s.trim();
	if let Some(value) = parse_typed_param(trimmed) {
		return value;
	}
	if trimmed.starts_with('[') {
		return parse_list_of_records(trimmed);
	}

	if let Ok(i) = s.parse::<i32>() {
		return Value::Int4(i);
	}
	if let Ok(i) = s.parse::<i64>() {
		return Value::Int8(i);
	}
	if let Ok(f) = s.parse::<f64>()
		&& let Ok(ordered) = reifydb_client::OrderedF64::try_from(f)
	{
		return Value::Float8(ordered);
	}

	if s == "true" {
		return Value::Boolean(true);
	}
	if s == "false" {
		return Value::Boolean(false);
	}

	if s.starts_with('\'') && s.ends_with('\'') && s.len() > 1 {
		return Value::Utf8(s[1..s.len() - 1].to_string());
	}
	if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
		return Value::Utf8(s[1..s.len() - 1].to_string());
	}

	Value::Utf8(s.to_string())
}

#[allow(dead_code)]
fn parse_list_of_records(s: &str) -> Value {
	let json: serde_json::Value = serde_json::from_str(s).expect("malformed list-of-records param literal");
	let serde_json::Value::Array(items) = json else {
		panic!("list-of-records param literal must be a JSON array, got: {s}");
	};
	Value::List(items.into_iter().map(json_object_to_record).collect())
}

#[allow(dead_code)]
fn json_object_to_record(json: serde_json::Value) -> Value {
	let serde_json::Value::Object(fields) = json else {
		panic!("list-of-records element must be a JSON object, got: {json}");
	};
	Value::Record(fields.into_iter().map(|(name, value)| (name, json_scalar_to_value(value))).collect())
}

#[allow(dead_code)]
fn json_scalar_to_value(json: serde_json::Value) -> Value {
	match json {
		serde_json::Value::String(s) => Value::Utf8(s),
		serde_json::Value::Bool(b) => Value::Boolean(b),
		serde_json::Value::Number(n) => {
			if let Some(i) = n.as_i64() {
				Value::Int8(i)
			} else {
				Value::Float8(
					reifydb_client::OrderedF64::try_from(n.as_f64().unwrap())
						.expect("param literal float is not NaN"),
				)
			}
		}
		other => panic!("unsupported field value in list-of-records param literal: {other}"),
	}
}

#[allow(dead_code)]
pub fn write_frames(frames: Vec<Frame>) -> Result<String, Box<dyn Error>> {
	let mut output = String::new();
	for frame in frames {
		writeln!(output, "{}", frame).unwrap();
	}
	Ok(output)
}
