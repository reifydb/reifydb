// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	FormatStyle, LoggingBuilder, MemoryDatabaseOptimistic, SessionSync,
	WithSubsystem,
	core::interface::{Params, subsystem::logging::LogLevel::Info},
	log_info, sync,
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
	let mut db: DB = sync::memory_optimistic()
		.with_logging(logger_configuration)
		.build()
		.unwrap();

	db.start().unwrap();

	log_info!("=== Distinct Operator Implementation Demo ===");
	log_info!("");
	log_info!("The DistinctOperator in reifydb-sub-flow:");
	log_info!("• Uses xxh3_128 to hash each row -> Hash128");
	log_info!("• Stores Hash128 in FlowDistinctStateKey");
	log_info!("• Maintains reference counts for duplicates");
	log_info!("• Emits rows only on first occurrence");
	log_info!("");

	// Create schema and table
	db.command_as_root(
		r#"
		create schema demo;
		create table demo.events { 
			id: int8, 
			category: text,
			value: int8
		};
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		create deferred view demo.all_events {
			id: int8,
			category: text,
			value: int8
		} with {
			FROM demo.events
		};
		"#,
		Params::None,
	)
	.unwrap();

	log_info!("=== Input Data (with duplicates) ===");
	log_info!("Inserting 10 rows with only 4 unique category/value pairs:");

	db.command_as_root(
		r#"
		from [
			{ id: 1, category: "A", value: 100 },
			{ id: 2, category: "B", value: 200 },
			{ id: 3, category: "A", value: 100 },
			{ id: 4, category: "C", value: 300 },
			{ id: 5, category: "B", value: 200 },
			{ id: 6, category: "A", value: 100 },
			{ id: 7, category: "D", value: 400 },
			{ id: 8, category: "B", value: 200 },
			{ id: 9, category: "A", value: 100 },
			{ id: 10, category: "C", value: 300 }
		]
		insert demo.events;
		"#,
		Params::None,
	)
	.unwrap();

	// Create another view with distinct
	db.command_as_root(
		r#"
		create deferred view demo.unique_events {
			id: int8,
			category: text,
			value: int8
		} with {
			FROM demo.events
			filter { value <= 200 }
			DISTINCT {category }
		};
		"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(10));

	for frame in db
		.query_as_root(
			r#"
		FROM demo.all_events
		"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	for frame in db
		.query_as_root(
			r#"
		FROM demo.unique_events
		"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	sleep(Duration::from_millis(10));
}
