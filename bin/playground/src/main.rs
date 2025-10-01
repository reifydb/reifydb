// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Params, Session, WithSubsystem,
	core::interface::logging::LogLevel::Info,
	embedded, log_info,
	sub_logging::{FormatStyle, LoggingBuilder},
};

pub type DB = MemoryDatabaseOptimistic;

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true).format_style(FormatStyle::Timeline))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(Info)
}

fn main() {
	let mut db: DB =
		embedded::memory_optimistic().with_logging(logger_configuration).with_worker(|wp| wp).build().unwrap();

	db.start().unwrap();

	// Test left join with duplicate keys on both sides
	// Should produce Cartesian product for matching keys

	// Create namespace
	log_info!("Creating namespace test...");
	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();

	// Create tables
	log_info!("Creating table test.students...");
	db.command_as_root(r#"create table test.students { id: int4, name: utf8, class_id: int4 }"#, Params::None)
		.unwrap();

	log_info!("Creating table test.courses...");
	db.command_as_root(
		r#"create table test.courses { id: int4, class_id: int4, subject: utf8, teacher: utf8 }"#,
		Params::None,
	)
	.unwrap();

	// Create LEFT JOIN view for student courses
	log_info!("Creating deferred view test.student_courses...");
	db.command_as_root(
		r#"
create deferred view test.student_courses {
    student_name: utf8,
    subject: utf8,
    teacher: utf8
} as {
    from test.students
    left join { from test.courses } courses on class_id == courses.class_id with { strategy: lazy_loading }
    map {
        student_name: name,
        subject: subject,
        teacher: teacher
    }
}
	"#,
		Params::None,
	)
	.unwrap();

	// Insert students with duplicate class_id
	log_info!("Inserting students with duplicate class_id...");
	db.command_as_root(
		r#"
from [
    {id: 1, name: "Alice", class_id: 10},
    {id: 2, name: "Bob", class_id: 10},
    {id: 3, name: "Charlie", class_id: 20},
    {id: 4, name: "Diana", class_id: 30}
] insert test.students
	"#,
		Params::None,
	)
	.unwrap();

	// Insert courses with duplicate class_id
	log_info!("Inserting courses with duplicate class_id...");
	db.command_as_root(
		r#"
from [
    {id: 101, class_id: 10, subject: "Math", teacher: "P Smith"},
    {id: 102, class_id: 10, subject: "Science", teacher: "P Jones"},
    {id: 103, class_id: 20, subject: "English", teacher: "P Brown"}
] insert test.courses
	"#,
		Params::None,
	)
	.unwrap();

	// Let the background task process
	sleep(Duration::from_millis(100));

	// Should show Cartesian product: Alice and Bob each get 2 rows (Math and Science)
	// Charlie gets 1 row (English), Diana gets 1 row with Undefined
	log_info!("Querying LEFT JOIN view with sorting...");
	let result = db
		.query_as_root("from test.student_courses sort { student_name asc, subject asc }", Params::None)
		.unwrap();
	for frame in result {
		println!("Student courses (sorted):\n{}", frame);
	}

	log_info!("✅ Test completed successfully!");
	log_info!("Shutting down...");
}
