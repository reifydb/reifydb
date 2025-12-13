// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::{
	WithSubsystem, server,
	sub_server_http::HttpConfig,
	sub_server_ws::WsConfig,
	sub_tracing::TracingBuilder,
};

fn tracing_configuration(tracing: TracingBuilder) -> TracingBuilder {
	tracing.with_console(|console| console.color(true).stderr_for_errors(true)).with_filter("trace")
}

fn main() {
	let mut db = server::memory_optimistic()
		.with_http(HttpConfig::default())
		.with_ws(WsConfig::default())
		.with_tracing(tracing_configuration)
		.with_flow(|flow| flow)
		.build()
		.unwrap();

	// Start the database and wait for signal
	println!("Starting database...");
	println!("Press Ctrl+C to stop...");

	db.start_and_await_signal().unwrap();

	println!("Database stopped successfully!");
}
