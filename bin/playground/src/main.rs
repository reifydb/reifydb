// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{path::PathBuf, str::FromStr, thread::sleep, time::Duration};

use reifydb::{
	Params, Session, WithSubsystem,
	core::interface::logging::LogLevel::Info,
	embedded,
	sub_logging::{FormatStyle, LoggingBuilder},
};

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true).format_style(FormatStyle::Timeline))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(Info)
}

fn main() {
	// let mut db = embedded::sqlite_optimistic(SqliteConfig::new("/tmp/test/test.db"))
	// let mut db = embedded::sqlite_optimistic(SqliteConfig::in_memory())
	let mut db = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.with_worker(|wp| wp)
		.with_flow(|f| {
			f.operators_dir(
				PathBuf::from_str("/home/ddymke/Workspace/red/testsuite/fixture/target/debug").unwrap(),
			)
		})
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace and table
	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();
	db.command_as_root(r#"create table test.source { id: int4, name: utf8, status: utf8 }"#, Params::None).unwrap();

	println!("Created namespace and table");

	// Create deferred view with counter operator
	db.command_as_root(
		r#"
create deferred view test.counter_view {
    insert: uint1,
    update: uint1,
    delete: uint1
} as {
    from test.source
    apply counter{}
}
	"#,
		Params::None,
	)
	.unwrap();

	println!("Created counter view");

	// Wait for view to be ready
	sleep(Duration::from_millis(500));

	// Query the counter view
	println!("\nQuerying counter view (initial):");
	for frame in db.query_as_root(r#"from test.counter_view"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Insert some records
	println!("\nInserting 10 records...");
	db.command_as_root(
		r#"
from [
    {id: 1, name: "Alice", status: "active"},
    {id: 2, name: "Bob", status: "active"},
    {id: 3, name: "Charlie", status: "inactive"},
    {id: 4, name: "Diana", status: "active"},
    {id: 5, name: "Eve", status: "active"},
    {id: 6, name: "Frank", status: "inactive"},
    {id: 7, name: "Grace", status: "active"},
    {id: 8, name: "Hank", status: "active"},
    {id: 9, name: "Ivy", status: "inactive"},
    {id: 10, name: "Jack", status: "active"}
]
insert test.source
	"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	// Query the counter view again
	println!("\nQuerying counter view after inserts:");
	for frame in db.query_as_root(r#"from test.counter_view"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Update some records
	println!("\nUpdating 3 records...");
	db.command_as_root(
		r#"
from test.source
filter id == 2 OR id == 5 or id == 8
map {id, name,  status: "suspended" }
update test.source
	"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	// Query the counter view again
	println!("\nQuerying counter view after updates:");
	for frame in db.query_as_root(r#"from test.counter_view"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Delete some records
	println!("\nDeleting 2 records...");
	db.command_as_root(
		r#"
from test.source
filter id == 3 or id == 9
delete test.source
	"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	// Query the counter view final time
	println!("\nQuerying counter view after deletes:");
	for frame in db.query_as_root(r#"from test.counter_view"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	println!("\nExpected counts: insert_count=10, update_count=3, delete_count=2");

	for frame in db.query_as_root(r#"from system.flow_operators"#, Params::None).unwrap() {
		println!("{}", frame);
	}
}
