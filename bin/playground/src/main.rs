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

	for frame in db
		.query_as_root(
			r#"
let $base := "Result"; let $status := "success"; let $message := $base + ": " + (if $status = "success" { "Operation completed" } else { "Operation failed" }); $message
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	for frame in db
		.query_as_root(
			r#"
		let $x := if false { "condition is false" } else { "condition is true" }; $x
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test our new text functions
	for frame in db
		.query_as_root(
			r#"
		text::trim("   hello world   ")
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	for frame in db
		.query_as_root(
			r#"
		text::upper("Hello World!")
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test new math functions
	for frame in db
		.query_as_root(
			r#"
		max(15, 23, 8)
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	for frame in db
		.query_as_root(
			r#"
		min(45, 12, 67)
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	for frame in db
		.query_as_root(
			r#"
		power(2, 3)
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	for frame in db
		.query_as_root(
			r#"
		round("3.14159", 2)
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	for frame in db
		.query_as_root(
			r#"
		text::substring("programming", 3, 4)
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test text::length function (returns byte count)
	for frame in db
		.query_as_root(
			r#"
		text::length("hello world")
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test with Unicode characters to show byte vs character difference
	for frame in db
		.query_as_root(
			r#"
		text::length("café 🌍")
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	sleep(Duration::from_millis(100));
}
