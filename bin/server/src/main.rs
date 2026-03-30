// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

use reifydb::{
	WithSubsystem, server, sub_server_admin::config::AdminConfig, sub_server_http::factory::HttpConfig,
	sub_server_ws::factory::WsConfig, sub_tracing::builder::TracingBuilder,
};

fn tracing_configuration(tracing: TracingBuilder) -> TracingBuilder {
	tracing.with_console(|console| console.color(true).stderr_for_errors(true)).with_filter("debug,reifydb=trace")
}

fn main() {
	let mut db = server::memory()
		.with_http(HttpConfig::default().bind_addr("0.0.0.0:8090").admin_bind_addr("127.0.0.1:9090"))
		.with_ws(WsConfig::default().bind_addr("0.0.0.0:8091").admin_bind_addr("127.0.0.1:9091"))
		.with_admin(AdminConfig::default().bind_addr("127.0.0.1:9092"))
		.with_tracing(tracing_configuration)
		.build()
		.unwrap();

	db.start_and_await_signal().unwrap();
}
