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

	// Test variable shadowing (should work)
	println!("=== Testing Shadowing ===");
	for frame in db
		.command_as_root(
			r#"
		let $x := 10; 
		let $x := 20; 
		MAP { $x }
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test mutable assignment (should work)
	println!("=== Testing Mutable Assignment ===");
	for frame in db
		.command_as_root(
			r#"
		let mut $x := 10; 
		$x := 20; 
		MAP { $x }
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test immutable assignment (should fail)
	println!("=== Testing Immutable Assignment (should fail) ===");
	match db.command_as_root(
		r#"
		let $x := 10; 
		$x := 20; 
		MAP { $x }
	"#,
		Params::None,
	) {
		Ok(_) => println!("ERROR: Should have failed!"),
		Err(e) => println!("✓ Correctly failed: {}", e),
	}

	// Test conditional statements
	println!("=== Testing Conditional Statements ===");

	// Test basic if without else - should succeed by parsing but produce no output
	match db.command_as_root(r#"if false { MAP { "result": "no output" } }"#, Params::None) {
		Ok(frames) => {
			let count = frames.len();
			println!("✓ Simple if statement (false condition) succeeded, {} frames", count);
		}
		Err(e) => println!("✗ Simple if statement failed: {}", e),
	}

	// Test if with true condition
	match db.command_as_root(r#"if true { MAP { "result": "yes output" } }"#, Params::None) {
		Ok(frames) => {
			let count = frames.len();
			println!("✓ Simple if statement (true condition) succeeded, {} frames", count);
			for frame in frames {
				println!("   {}", frame);
			}
		}
		Err(e) => println!("✗ Simple if statement failed: {}", e),
	}

	// Test if-else with false condition
	match db.command_as_root(
		r#"if false { MAP { "result": "then" } } else { MAP { "result": "else" } }"#,
		Params::None,
	) {
		Ok(frames) => {
			let count = frames.len();
			println!("✓ If-else statement (false condition) succeeded, {} frames", count);
			for frame in frames {
				println!("   {}", frame);
			}
		}
		Err(e) => println!("✗ If-else statement failed: {}", e),
	}

	// Test else-if chain
	match db.command_as_root(
		r#"let $result := if false { MAP { "result": "first" } } else if true { MAP { "result": "second" } } else { MAP { "result": "third" } }; FROM $result;"#,
		Params::None,
	) {
		Ok(frames) => {
			let count = frames.len();
			println!("✓ Else-if chain succeeded, {} frames", count);
			for frame in frames {
				println!("   {}", frame);
			}
		}
		Err(e) => println!("✗ Else-if chain failed: {}", e),
	}

	sleep(Duration::from_millis(100));
}
