// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

use reifydb::{WithSubsystem, server, sub_tracing::builder::TracingConfigurator};
use reifydb_type::params::Params;
use tracing::info;

fn tracing_configuration(tracing: TracingConfigurator) -> TracingConfigurator {
	tracing.with_console(|console| console.color(true).stderr_for_errors(true)).with_filter("debug,reifydb=trace")
}

fn main() {
	let mut db = server::memory()
		.with_tracing(tracing_configuration)
		.with_http(|c| c.admin_bind_addr("0.0.0.0:18091"))
		.with_ws(|c| c.admin_bind_addr("0.0.0.0:18090"))
		.with_flow(|flow| flow)
		.build()
		.unwrap();

	info!("Database built successfully");

	println!("Starting database...");
	println!("HTTP server: http://0.0.0.0:18091");
	println!("WebSocket server: ws://0.0.0.0:18090");
	println!();
	println!("Press Ctrl+C to stop...");

	db.start().unwrap();

	db.admin_as_root("CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }", Params::None)
		.unwrap();
	println!("Auth token configured for root user: mysecrettoken");

	db.admin_as_root("CREATE USER alice", Params::None).unwrap();
	db.admin_as_root("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }", Params::None)
		.unwrap();
	db.admin_as_root("CREATE USER bob", Params::None).unwrap();
	db.admin_as_root("CREATE AUTHENTICATION FOR bob { method: token; token: 'bob-secret-token' }", Params::None)
		.unwrap();
	println!("Test users configured: alice (password), bob (token)");

	db.await_signal().unwrap();
}
