// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Simple Flow-to-Flow Communication Example
//!
//! This example demonstrates:
//! - Flow A: Filters data from a table (value > 5)
//! - Flow B: Consumes from Flow A via deferred view and transforms it (doubles values)
//! - Result: Materialized view showing filtered and transformed data

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
	let mut db = embedded::memory_optimistic().with_logging(logger_configuration).build().unwrap();

	db.start().unwrap();

	println!("\n=== Flow-to-Flow Communication Example ===\n");

	// Create namespace
	println!("1. Creating namespace...");
	db.command_as_root(r#"create namespace test"#, Params::None).unwrap();

	// Create source table
	println!("2. Creating source table...");
	db.command_as_root(
		r#"create table test.numbers {
    id: int4,
    value: int4
}"#,
		Params::None,
	)
	.unwrap();

	// Create Flow A: filters data (value > 5)
	println!("3. Creating Flow A (filters values > 5)...");
	db.command_as_root(
		r#"create flow test.flow_a as {
    		from test.numbers
    		filter { value > 5 }
    }"#,
		Params::None,
	)
	.unwrap();

	// Create deferred view that consumes from Flow A
	println!("4. Creating deferred view (consumes from Flow A, doubles values)...");
	db.command_as_root(
		r#"create deferred view test.result {
			id: int4,
			value: int4,
			doubled: int4
		} as {
			from test.flow_a
			extend { doubled: value * 2 }
    	}"#,
		Params::None,
	)
	.unwrap();

	// Show flow information
	for frame in db.query_as_root(r#"from system.flows map {id, name}"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// 	// Wait for view to be ready
	// 	sleep(Duration::from_millis(500));
	//
	// 	// Insert test data
	// 	println!("\n5. Inserting test data...");
	// 	db.command_as_root(
	// 		r#"from [
	//     {id: 1, value: 3},
	//     {id: 2, value: 10},
	//     {id: 3, value: 7},
	//     {id: 4, value: 2},
	//     {id: 5, value: 15}
	// ]
	// insert test.numbers"#,
	// 		Params::None,
	// 	)
	// 	.unwrap();
	//
	// 	sleep(Duration::from_millis(500));
	//
	// 	// Query the result
	// 	println!("\n6. Querying result (should only show values > 5, doubled):\n");
	// 	for frame in db.query_as_root(r#"from test.result"#, Params::None).unwrap() {
	// 		println!("{}", frame);
	// 	}
	//
	// 	println!("\n=== Expected Output ===");
	// 	println!("Only rows where value > 5 should appear:");
	// 	println!("  - id: 2, value: 10, doubled: 20");
	// 	println!("  - id: 3, value: 7, doubled: 14");
	// 	println!("  - id: 5, value: 15, doubled: 30");
	// 	println!("\n=== Flow Info ===");
}
