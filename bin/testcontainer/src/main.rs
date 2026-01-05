// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb::{
	WithSubsystem, server, sub_server_http::HttpConfig, sub_server_otel::OtelConfig, sub_server_ws::WsConfig,
};
use tracing::{info, info_span};

fn main() {
	tokio::runtime::Builder::new_multi_thread()
		.worker_threads(num_cpus::get())
		.max_blocking_threads(128)
		.thread_name("testcontainer")
		.enable_all()
		.build()
		.unwrap()
		.block_on(async_main());
}

async fn async_main() {
	let http_config = HttpConfig::default();
	let ws_config = WsConfig::default();

	// Build database with integrated OpenTelemetry
	let mut db = server::memory()
		.await
		.unwrap()
		.with_http(http_config)
		.with_ws(ws_config)
		.with_tracing_otel(
			OtelConfig::new()
				.service_name("testcontainer")
				.endpoint("http://localhost:4317")
				.sample_ratio(1.0)
				.scheduled_delay(Duration::from_millis(500)),
			|t| t
				.without_console()  // Disable console logging for better performance
				.with_filter("trace"),  // Only affects OpenTelemetry layer
		)
		.with_flow(|flow| flow)
		// .with_admin(AdminConfig::default())
		.build()
		.await
		.unwrap();

	// Test spans to verify OpenTelemetry is working
	{
		let span = info_span!("testcontainer_startup", service = "testcontainer");
		let _guard = span.enter();
		info!("Database built successfully, testing OpenTelemetry pipeline");
	}

	// Start the database and wait for signal
	println!("Starting database...");
	println!("HTTP server: http://0.0.0.0:8091");
	println!("WebSocket server: ws://0.0.0.0:8090");
	// println!("Jaeger UI: http://localhost:16686 (if running)");
	println!();
	println!("Press Ctrl+C to stop...");

	db.start_and_await_signal().await.unwrap();
}
