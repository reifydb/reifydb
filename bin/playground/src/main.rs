// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::time::Duration;

use reifydb::{
	core::interface::{subsystem::logging::LogLevel::Trace, Params}, sync, FormatStyle, LoggingBuilder,
	MemoryDatabaseOptimistic,
	SessionSync,
	WithSubsystem,
};

pub type DB = MemoryDatabaseOptimistic;
// pub type DB = SqliteDatabaseOptimistic;

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
	.level(Trace)
}

fn main() {
	let mut db: DB = sync::memory_optimistic()
		.with_logging(logger_configuration)
		.build()
		.unwrap();

	db.start().unwrap();

	// for frame in db
	// 	.query_as_root(&generate_large_filter_query(), Params::None)
	// 	.unwrap()
	// {
	// 	println!("{}", frame);
	// }
	println!("{}",&generate_large_filter_query());
}

fn generate_large_filter_query() -> String {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};

	let mut items = Vec::new();

	for i in 1..=2049 {
		let mut hasher = DefaultHasher::new();
		i.hash(&mut hasher);
		let hash = hasher.finish();

		let active = if i <= 1025 {
			(hash % 2) == 0
		} else {
			(hash % 2) == 1
		};

		items.push(format!("{{ id: {}, active: {} }}", i, active));
	}

	format!("from [{}] filter active = true", items.join(", "))
}
