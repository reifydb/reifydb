// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

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
	let mut db =
		embedded::memory_optimistic().with_logging(logger_configuration).with_worker(|wp| wp).build().unwrap();

	db.start().unwrap();

	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();
	db.command_as_root(r#"create table test.source { id: int4, name: utf8, age: int4, city: utf8 }"#, Params::None)
		.unwrap();
	db.command_as_root(
		r#"create deferred view test.projection { id: int4, name: utf8 } as {
  from test.source
  map {id: id, name: name + " overwritten"}
  take 1
}"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
from [
  {id: 1, name: "Alice", age: 30, city: "NYC"},
  {id: 2, name: "Bob", age: 25, city: "LA"},
  {id: 3, name: "Charlie", age: 35, city: "Chicago"},
  {id: 4, name: "Diana", age: 28, city: "Boston"}
] insert test.source

	"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(10));

	for frame in db
		.query_as_root(
			r#"
from test.projection
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	sleep(Duration::from_millis(100));
}
