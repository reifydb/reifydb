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

	// Split commands to isolate transaction issues
	for frame in
		db.command_as_root("create schema test", Params::None).unwrap()
	{
		log_info!("{frame}")
	}

	for frame in db
		.command_as_root(
			"create table test.users { id: int4, name: utf8 }",
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	for frame in db
		.command_as_root(
			"alter table test.users { create primary key users_pk { id} }",
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	// Check system catalog immediately after primary key creation
	log_info!("=== Primary keys immediately after creation ===");
	for frame in db
		.command_as_root(r#"from system.primary_keys"#, Params::None)
		.unwrap()
	{
		log_info!("{frame}")
	}

	for frame in db
		.command_as_root(
			r#"from [{id: 1, name: "M"}] insert test.users"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	// Test regular table scan without index
	log_info!("=== Regular table scan ===");
	for frame in
		db.command_as_root(r#"from test.users"#, Params::None).unwrap()
	{
		log_info!("{frame}")
	}

	// Let's check what primary keys exist in the system
	log_info!("=== Primary keys in system ===");
	for frame in db
		.command_as_root(r#"from system.primary_keys"#, Params::None)
		.unwrap()
	{
		log_info!("{frame}")
	}

	// Let's check primary key columns
	log_info!("=== Primary key columns in system ===");
	for frame in db
		.command_as_root(
			r#"from system.primary_key_columns"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	// Test index scan without filter
	log_info!("=== Index scan without filter ===");
	for frame in db
		.command_as_root(r#"from test.users::users_pk"#, Params::None)
		.unwrap()
	{
		log_info!("{frame}")
	}

	// Test index scan with filter
	log_info!("=== Index scan with filter ===");
	for frame in db
		.command_as_root(
			r#"from test.users::users_pk filter { id = 1 }"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	sleep(Duration::from_millis(10));
}
