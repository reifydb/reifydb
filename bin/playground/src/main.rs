// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::time::Duration;

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

	// Test let mut $var syntax with := operator
	for frame in db.command_as_root(r#"let $user_data := FROM [{ name: "Alice", age: 25 }, { name: "Bob", age: 17 }, { name: "Carol", age: 30 }] | FILTER age > 21; $user_data"#, Params::None).unwrap() {
		println!("let mut $counter result:");
		println!("{}", frame);
	}
}
