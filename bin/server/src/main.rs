// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb::{
	WithSubsystem, core::interface::logging::LogLevel, server, sub_admin::AdminConfig, sub_logging::LoggingBuilder,
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
		.with_config(ServerConfig {
			bind_addr: "0.0.0.0:8090".to_string(),
			network: Default::default(),
			protocols: Default::default(),
		})
		.with_admin(AdminConfig::default().with_port(9092))
		.with_logging(logger_configuration)
		.build()
		.unwrap();

	// Start the database
	db.start().unwrap();
	println!("Database started successfully!");
	println!("Admin console available at http://localhost:9092/");

	// Run for a short time to test logging
	std::thread::sleep(Duration::from_secs(2000));

	// Stop the database
	println!("Shutting down database...");
	db.stop().unwrap();
	println!("Database stopped successfully!");
}
