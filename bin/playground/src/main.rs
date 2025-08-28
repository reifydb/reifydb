// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	FormatStyle, LoggingBuilder, MemoryDatabaseOptimistic, SessionSync,
	WithSubsystem,
	core::interface::{Params, subsystem::logging::LogLevel::Trace},
	sync,
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

		// .intercept(table_pre_insert(|_ctx| {
		// 	log_info!("Table pre insert interceptor called!");
		// 	Ok(())
		// }))
		// .intercept(table_post_insert(|_ctx| {
		// 	log_info!("Table post insert interceptor called!");
		// 	Ok(())
		// }))
		// .intercept(post_commit(|ctx| {
		// 	log_info!(
		// 		"Post-commit interceptor called with version: {:?}",
		// 		ctx.version
		// 	);
		// 	Ok(())
		// }))
		.build()
		.unwrap();
	// let mut db: DB =
	// sync::sqlite_optimistic(SqliteConfig::new("/tmp/reifydb"));

	db.start().unwrap();

	db.command_as_root(
		r#"
	    create schema test;
	    create table test.raw_data { x: int4, y: int4, z: int4 }
	"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
  from [
    { x: 10, y: 20, z: 30 },
    { x: 15, y: 25, z: 35 },
    { x: 5, y: 15, z: 25 }
  ] insert test.raw_data
		"#,
		Params::None,
	)
	.unwrap();

	// Original problematic query - should now work correctly
	for frame in db
		.command_as_root(
			r#"from test.raw_data map {id: x, value: y * 2}"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	sleep(Duration::from_millis(10))
}
