// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::time::Duration;

use reifydb::{
	FormatStyle, LoggingBuilder, MemoryDatabaseOptimistic, SessionSync,
	WithSubsystem,
	core::interface::{Params, subsystem::logging::LogLevel::Trace},
	log_info, sync,
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

	for frame in
		db.query_as_root(r#"MAP 1 != undefined"#, Params::None).unwrap()
	{
		log_info!("{}", frame);
	}

	db.command_as_root(
		r#"
	    create schema test;
	    create table test.users { id: int4, name: utf8, active: bool };
	"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
  from [
    { id: 1, name: "Alice", active: true },
    { id: 2, name: "Bob", active: false },
    { id: 3, name: "Charlie", active: true }
  ] insert test.users
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
from test.users filter active = true map { id: id, name: "ACTIVE", active: false } update
		"#,
		Params::None,
	)
	.unwrap();

	for frame in db
		.command_as_root(
			r#"
from test.users
		"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}
}
