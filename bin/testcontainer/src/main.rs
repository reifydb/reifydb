// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

use std::time::Duration;

use reifydb::{
	WithSubsystem, server, sub_server_http::factory::HttpConfig, sub_server_otel::config::OtelConfig,
	sub_server_ws::factory::WsConfig,
};
use reifydb_type::params::Params;
use tracing::info;

fn main() {
	let http_config = HttpConfig::default().admin_bind_addr("0.0.0.0:8091");
	let ws_config = WsConfig::default().admin_bind_addr("0.0.0.0:8090");

	// Build database with integrated OpenTelemetry
	let mut db = server::memory()
		.with_tracing_otel(
			OtelConfig::new()
				.service_name("testcontainer")
				.endpoint("http://localhost:4317")
				.sample_ratio(1.0)
				.scheduled_delay(Duration::from_millis(500)),
			|t| t.without_console()  // Disable console logging for better performance
				.with_filter("trace"),  // Only affects OpenTelemetry layer
		)
		.with_http(http_config)
		.with_ws(ws_config)
		.with_flow(|flow| flow)
		// .with_admin(AdminConfig::default())
		.build()
		.unwrap();

	info!("Database built successfully");

	// Start the database and wait for signal
	println!("Starting database...");
	println!("HTTP server: http://0.0.0.0:8091");
	println!("WebSocket server: ws://0.0.0.0:8090");
	// println!("Jaeger UI: http://localhost:16686 (if running)");
	println!();
	println!("Press Ctrl+C to stop...");

	db.start().unwrap();

	// Create a hardcoded auth token for root so clients can authenticate
	db.admin_as_root("CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }", Params::None)
		.unwrap();
	println!("Auth token configured for root user: mysecrettoken");

	// Create test users for login integration tests
	db.admin_as_root("CREATE USER alice", Params::None).unwrap();
	db.admin_as_root("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }", Params::None)
		.unwrap();
	db.admin_as_root("CREATE USER bob", Params::None).unwrap();
	db.admin_as_root("CREATE AUTHENTICATION FOR bob { method: token; token: 'bob-secret-token' }", Params::None)
		.unwrap();
	println!("Test users configured: alice (password), bob (token)");

	db.await_signal().unwrap();
}
