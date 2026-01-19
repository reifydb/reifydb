// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb::{
	WithSubsystem, server, sub_server_admin::config::AdminConfig, sub_server_http::factory::HttpConfig,
	sub_server_ws::factory::WsConfig, sub_tracing::builder::TracingBuilder,
};

fn tracing_configuration(tracing: TracingBuilder) -> TracingBuilder {
	tracing.with_console(|console| console.color(true).stderr_for_errors(true)).with_filter("debug,reifydb=trace")
}

fn main() {
	let mut db = server::memory()
		.with_http(HttpConfig::default().bind_addr("0.0.0.0:8090"))
		.with_ws(WsConfig::default().bind_addr("0.0.0.0:8091"))
		.with_admin(AdminConfig::default().bind_addr("127.0.0.1:9092"))
		.with_tracing(tracing_configuration)
		.build()
		.unwrap();

	db.start_and_await_signal().unwrap();
}
