// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb::{
	WithSubsystem, server, sub_server_admin::AdminConfig, sub_server_http::HttpConfig, sub_server_ws::WsConfig,
	sub_tracing::TracingBuilder,
};

fn tracing_configuration(tracing: TracingBuilder) -> TracingBuilder {
	tracing.with_console(|console| console.color(true).stderr_for_errors(true)).with_filter("debug,reifydb=trace")
}

#[tokio::main]
async fn main() {
	let mut db = server::memory()
		.await
		.unwrap()
		.with_http(HttpConfig::default().bind_addr("0.0.0.0:8090"))
		.with_ws(WsConfig::default().bind_addr("0.0.0.0:8091"))
		.with_admin(AdminConfig::default().bind_addr("127.0.0.1:9092"))
		.with_tracing(tracing_configuration)
		.build()
		.await
		.unwrap();

	// Start the database
	db.start().await.unwrap();
	println!("Database started successfully!");
	println!("Admin console available at http://127.0.0.1:9092/");

	// Run for a short time to test logging
	tokio::time::sleep(Duration::from_secs(2000)).await;

	// Stop the database
	println!("Shutting down database...");
	db.stop().unwrap();
	println!("Database stopped successfully!");
}
