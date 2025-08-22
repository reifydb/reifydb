// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread, time::Duration};

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
	    create table test.users { value: int8, age: int8};
	"#,
		Params::None,
	)
	.unwrap();

	// 	db.command_as_root(
	// 		r#"
	// create deferred view test.basic { value: int8, age: int8 } with {
	//     from test.users
	//     aggregate sum(value) by age
	// }
	// 	"#,
	// 		Params::None,
	// 	)
	// 	.unwrap();

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

	// for frame in
	// 	db.query_as_root(r#"FROM test.users"#, Params::None).unwrap()
	// {
	// 	println!("{}", frame);
	// }

	// db.command_as_root(
	// 	r#"
	// from test.users
	// filter { name = "bob" }
	// map { name: "bob", age: 21}
	// update test.users;
	//
	// "#,
	// 	Params::None,
	// )
	// .unwrap();

	// for frame in
	// 	db.query_as_root(r#"FROM test.users"#, Params::None).unwrap()
	// {
	// 	println!("{}", frame);
	// }

	// loop {}
	thread::sleep(Duration::from_millis(10));

	// println!("Basic database operations completed successfully!");
	// rql_to_flow_example(&mut db);

	// for frame in
	// 	db.query_as_root(r#"FROM test.basic"#, Params::None).unwrap()
	// {
	// 	log_info!("{}", frame);
	// }
}
