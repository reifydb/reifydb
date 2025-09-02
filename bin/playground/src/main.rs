// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	FormatStyle, LoggingBuilder, MemoryDatabaseOptimistic, Params, Session,
	WithSubsystem, core::interface::subsystem::logging::LogLevel::Info,
	embedded, log_info,
};

pub type DB = MemoryDatabaseOptimistic;

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| {
		console.color(true)
			.stderr_for_errors(true)
			.format_style(FormatStyle::Timeline)
	})
	.buffer_capacity(20000)
	.batch_size(2000)
	.flush_interval(Duration::from_millis(50))
	.immediate_on_error(true)
	.level(Info)
}

fn main() {
	let mut db: DB = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.build()
		.unwrap();

	db.start().unwrap();

	// Test system.sequences virtual table query
	log_info!("=== Testing system.sequences virtual table ===");
	for frame in
		db.query_as_root("from system.tables", Params::None).unwrap()
	{
		log_info!("Basic query\n{}", frame);
	}

	// Test with projection
	log_info!("=== Testing system.sequences with projection ===");
	for frame in db
		.query_as_root(
			"from system.sequences map { name, value }",
			Params::None,
		)
		.unwrap()
	{
		log_info!("Projected query: {}", frame);
	}

	sleep(Duration::from_millis(10));
}
