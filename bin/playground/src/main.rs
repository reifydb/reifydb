// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread, time::Duration};

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

	db.command_as_root(
		r#"
	    create schema test;
	    create table test.users { value: int8, age: int8};
	"#,
		Params::None,
	)
	.unwrap();

	// Create first deferred view - all users
	db.command_as_root(
		r#"
	create deferred view test.all_users { value: int8, age: int8 } with {
	    from test.users
	}
		"#,
		Params::None,
	)
	.unwrap();

	// Create second deferred view - teenagers (age < 20)
	db.command_as_root(
		r#"
	create deferred view test.teenagers { value: int8, age: int8 } with {
	    from test.users
	    filter { age < 20 }
	}
		"#,
		Params::None,
	)
	.unwrap();

	// Create third deferred view - adults (age >= 20)
	db.command_as_root(
		r#"
	create deferred view test.adults { value: int8, age: int8 } with {
	    from test.users
	    filter { age >= 20 }
	}
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
    from [
        { value: 1, age: 19 },
        { value: 1, age: 20 },
        { value: 1, age: 19 },
    ]
    insert test.users;

    "#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
	from [
	    { value: 1, age: 40 },
	    { value: 1, age: 19 },
	    { value: 1, age: 19 },
	]
	insert test.users;

	"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
	from [
	    { value: 11, age: 40 },
	    { value: 1, age: 19 },
	    { value: 1, age: 19 },
	]
	insert test.users;

	"#,
		Params::None,
	)
	.unwrap();

	thread::sleep(Duration::from_millis(100));

	log_info!("=== All Users View ===");
	for frame in db
		.query_as_root(r#"FROM test.all_users"#, Params::None)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	log_info!("=== Teenagers View ===");
	for frame in db
		.query_as_root(r#"FROM test.teenagers"#, Params::None)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	log_info!("=== Adults View ===");
	for frame in
		db.query_as_root(r#"FROM test.adults"#, Params::None).unwrap()
	{
		log_info!("{}", frame);
	}

	// Test global aggregation with empty BY clause
	log_info!("=== Global Aggregation Examples ===");

	// Example 1: Count all rows
	log_info!("Example 1: Count all rows");
	for frame in db
		.query_as_root(
			r#"from test.users aggregate count(value) by {}"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 2: Multiple global aggregations with aliases
	log_info!("\nExample 2: Multiple global aggregations");
	for frame in db.query_as_root(
		r#"from test.users aggregate { total_count: count(value), total_sum: sum(value), avg_age: avg(age) } by {}"#,
		Params::None,
	).unwrap() {
		log_info!("{}", frame);
	}

	// Example 3: Compare with grouped aggregation
	log_info!("\nExample 3: Grouped aggregation by age");
	for frame in db.query_as_root(
		r#"from test.users aggregate { count: count(value), sum: sum(value) } by age"#,
		Params::None,
	).unwrap() {
		log_info!("{}", frame);
	}

	thread::sleep(Duration::from_millis(10));
}
