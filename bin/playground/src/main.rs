// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Params, Session, WithSubsystem,
	core::interface::subsystem::logging::LogLevel::Info,
	embedded, log_info,
	sub_logging::{FormatStyle, LoggingBuilder},
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

	// Test system.versions table
	let versions = db
		.query_as_root("MAP {flag: cast(true, boolean)}", Params::None)
		.unwrap();
	for frame in versions {
		log_info!("{}", frame);
	}
}
