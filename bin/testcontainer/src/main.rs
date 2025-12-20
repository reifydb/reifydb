// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb::{
	WithSubsystem, server, sub_server_admin::AdminConfig, sub_server_http::HttpConfig, sub_server_otel::OtelConfig,
	sub_server_ws::WsConfig,
};
use tracing::{info, info_span};

fn main() {
	// Build database with integrated OpenTelemetry
	let mut db = server::memory()
		.with_http(HttpConfig::default())
		.with_ws(WsConfig::default())
		.with_tracing_otel(
			OtelConfig::new()
				.service_name("testcontainer")
				.endpoint("http://localhost:4317")
				.sample_ratio(1.0)
				.scheduled_delay(Duration::from_millis(500)),
			|t| t.with_filter("trace"),
		)
		.with_flow(|flow| flow)
		.with_admin(AdminConfig::default())
		.build()
		.unwrap();

	// Test spans to verify OpenTelemetry is working
	{
		let span = info_span!("testcontainer_startup", service = "testcontainer");
		let _guard = span.enter();
		info!("Database built successfully, testing OpenTelemetry pipeline");
	}

	// Start the database and wait for signal
	println!("Starting database...");
	println!("HTTP server: http://localhost:8091");
	println!("WebSocket server: ws://localhost:8090");
	println!("Jaeger UI: http://localhost:16686 (if running)");
	println!();
	println!("Press Ctrl+C to stop...");

	db.start_and_await_signal().unwrap();
}
