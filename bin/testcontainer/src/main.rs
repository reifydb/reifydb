// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb::{
	WithSubsystem, core::interface::logging::LogLevel, server, sub_logging::LoggingBuilder,
	sub_server::ServerConfig,
};

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(LogLevel::Trace)
}

fn main() {
	let mut db = server::memory_optimistic()
		.with_config(ServerConfig::default())
		.with_logging(logger_configuration)
		.with_flow(|flow| flow)
		.build()
		.unwrap();

	// Start the database and wait for signal
	println!("Starting database...");
	println!("Press Ctrl+C to stop...");

	db.start_and_await_signal().unwrap();

	println!("Database stopped successfully!");
}
