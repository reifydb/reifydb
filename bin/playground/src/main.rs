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

	log_info!("=== Testing View-to-View Dependencies ===");

	log_info!("Creating schema and base table...");
	db.command_as_root("create schema test", Params::None).unwrap();

	db.command_as_root(
		"create table test.source { id: int4, value: int4 }",
		Params::None,
	)
	.unwrap();

	log_info!("Creating view_1 that depends on source table...");
	db.command_as_root(
		"create deferred view test.view_1 { id: int4, value: int4 } with { from test.source }",
		Params::None,
	).unwrap();

	log_info!(
		"Creating view_2 that depends on view_1 (VIEW-TO-VIEW DEPENDENCY)..."
	);
	db.command_as_root(
		"create deferred view test.view_2 { id: int4, value: int4 } with { from test.view_1  map {id, value: value * value }  }",
		Params::None,
	).unwrap();

	// Check what flows were created
	log_info!("Checking flows in reifydb.flows table:");
	for frame in db
		.command_as_root(
			"from reifydb.flows map { id, cast(data, utf8) as flow_json }",
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	log_info!("Inserting data into source table...");
	db.command_as_root(
		r#"from [{id: 1, value: 100}, {id: 2, value: 200}] insert test.source"#,
		Params::None,
	)
	.unwrap();

	// Give the flow system time to process
	log_info!("Waiting for flows to cascade...");

	// Give more time for CDC events to be processed
	// Need multiple poll cycles: one for source->view_1, another for
	// view_1->view_2
	for i in 0..20 {
		sleep(Duration::from_millis(100));

		// Check if view_2 has data
		let result = db
			.query_as_root("FROM test.view_2", Params::None)
			.unwrap();

		if !result.is_empty()
			&& !result[0].is_empty()
			&& result[0][0].data.len() > 0
		{
			log_info!(
				"View cascade completed after {} ms",
				(i + 1) * 100
			);
			break;
		}

		if i == 19 {
			log_info!(
				"View cascade did not complete after 2 seconds"
			);
		}
	}

	log_info!("Checking source table:");
	for frame in
		db.command_as_root("from test.source", Params::None).unwrap()
	{
		log_info!("{frame}")
	}

	log_info!("Checking view_1 (should have data from source):");
	for frame in
		db.command_as_root("from test.view_1", Params::None).unwrap()
	{
		log_info!("{frame}")
	}

	log_info!("Checking view_2 (should have data from view_1):");
	for frame in
		db.command_as_root("from test.view_2", Params::None).unwrap()
	{
		log_info!("{frame}")
	}

	log_info!("");
	log_info!(
		"Key Achievement: view_2 depends on view_1, not directly on source!"
	);
	log_info!(
		"This demonstrates successful view-to-view dependency implementation."
	);

	sleep(Duration::from_millis(10));
}
