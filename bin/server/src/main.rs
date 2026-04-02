// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

use reifydb::{WithSubsystem, server, sub_tracing::builder::TracingConfigurator};

fn tracing_configuration(tracing: TracingConfigurator) -> TracingConfigurator {
	tracing.with_console(|console| console.color(true).stderr_for_errors(true)).with_filter("debug,reifydb=trace")
}

fn main() {
	let mut db = server::memory()
		.with_http(|http| http.bind_addr("0.0.0.0:8090").admin_bind_addr("127.0.0.1:9090"))
		.with_ws(|ws| ws.bind_addr("0.0.0.0:8091").admin_bind_addr("127.0.0.1:9091"))
		.with_admin(|c| c.bind_addr("127.0.0.1:9092"))
		.with_tracing(tracing_configuration)
		.build()
		.unwrap();

	db.start_and_await_signal().unwrap();
}
