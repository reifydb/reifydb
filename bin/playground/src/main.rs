// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	Params, Session, WithSubsystem,
	core::{interface::logging::LogLevel::Info, util::clock},
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
	// Set mock time to a known value for testing time-based windows
	let base_time = 1000000; // Start at 1,000,000 milliseconds
	clock::mock_time_set(base_time);

	let mut db =
		embedded::memory_optimistic().with_logging(logger_configuration).with_worker(|wp| wp).build().unwrap();

	db.start().unwrap();

	// Test EXTEND expressions in scalar contexts
	println!("=== Testing: EXTEND expressions ===");
	for frame in db
		.query_as_root(
			r#"
if EXTEND { "test": true } = false { "extend1" }
            else if EXTEND { "test": true } = false { "extend2" }
            else if EXTEND { "test": true } = true { "extend3" }
            else { "no_extend" }
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	sleep(Duration::from_millis(100));
}
