// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering::Relaxed},
	},
	thread::sleep,
	time::Duration,
};

use reifydb::{
	Identity, MemoryDatabaseOptimistic, Session, WithSubsystem,
	core::{
		flow::FlowChange,
		interface::{Engine, FlowNodeId, Transaction, logging::LogLevel::Info},
	},
	embedded,
	engine::{StandardCommandTransaction, StandardEvaluator},
	log_info,
	sub::task,
	sub_flow::{FlowBuilder, Operator, TransformOperator},
	sub_logging::{FormatStyle, LoggingBuilder},
	r#type::params,
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

struct MyOP;

impl<T: Transaction> Operator<T> for MyOP {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardEvaluator,
	) -> reifydb::Result<FlowChange> {
		println!("INVOKED");
		Ok(change)
	}
}

impl<T: Transaction> TransformOperator<T> for MyOP {
	fn id(&self) -> FlowNodeId {
		FlowNodeId(12345)
	}
}

fn flow_configuration<T: Transaction>(flow: FlowBuilder<T>) -> FlowBuilder<T> {
	flow.register_operator("test".to_string(), |_node, _exprs| Ok(Box::new(MyOP {})))
}

fn main() {
	let mut db: DB = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.with_flow(flow_configuration)
		.with_worker(|wp| wp)
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace and tables
	println!("Creating namespace and tables...");
	db.command_as_root("create namespace test", params![]).unwrap();
	db.command_as_root("create table test.left_table { id: int1, value: int1, extra: int1 }", params![]).unwrap();
	db.command_as_root("create table test.right_table { id: int1, value: int1, extra: int1 }", params![]).unwrap();

	// Insert data
	println!("Inserting data...");
	db.command_as_root(
		"from [{id: 1, value: 10, extra: 5}, {id: 2, value: 20, extra: 10}] insert test.left_table",
		params![],
	)
	.unwrap();
	db.command_as_root("from [{id: 1, value: 10, extra: 5}, {id: 1, value: 99, extra: 5}, {id: 3, value: 20, extra: 99}] insert test.right_table", params![]).unwrap();

	// Verify data
	println!("\nData in left_table:");
	let left_data = db.query_as_root("from test.left_table", params![]).unwrap();
	for frame in left_data {
		println!("  {}", frame);
	}

	println!("\nData in right_table:");
	let right_data = db.query_as_root("from test.right_table", params![]).unwrap();
	for frame in right_data {
		println!("  {}", frame);
	}

	// Try the failing join query with OR condition
	println!("\n=== Testing JOIN with OR condition ===");
	let query = "from test.left_table inner join { from test.right_table } r on (id == r.id and value == r.value) or extra == r.extra";
	println!("Query: {}", query);
	match db.query_as_root(query, params![]) {
		Ok(frames) => {
			println!("Query succeeded! Results:");
			for frame in frames {
				println!("  {}", frame);
			}
		}
		Err(e) => {
			println!("Query failed with error: {:?}", e);

			// Try to isolate the issue
			println!("\n--- Testing simpler variations ---");

			// Test just AND condition
			let test1 = "from test.left_table inner join { from test.right_table } r on id == r.id and value == r.value";
			println!("\nTest 1 (AND only): {}", test1);
			match db.query_as_root(test1, params![]) {
				Ok(frames) => {
					println!("  Success! Results:");
					for frame in frames {
						println!("    {}", frame);
					}
				}
				Err(e) => println!("  Failed: {:?}", e),
			}

			// Test just single equality
			let test2 = "from test.left_table inner join { from test.right_table } r on extra == r.extra";
			println!("\nTest 2 (single equality): {}", test2);
			match db.query_as_root(test2, params![]) {
				Ok(frames) => {
					println!("  Success! Results:");
					for frame in frames {
						println!("    {}", frame);
					}
				}
				Err(e) => println!("  Failed: {:?}", e),
			}

			// Test OR with simpler conditions
			let test3 = "from test.left_table inner join { from test.right_table } r on id == r.id or value == r.value";
			println!("\nTest 3 (simple OR): {}", test3);
			match db.query_as_root(test3, params![]) {
				Ok(frames) => {
					println!("  Success! Results:");
					for frame in frames {
						println!("    {}", frame);
					}
				}
				Err(e) => println!("  Failed: {:?}", e),
			}

			// Test with parentheses in different places
			let test4 = "from test.left_table inner join { from test.right_table } r on ((id == r.id) and (value == r.value)) or (extra == r.extra)";
			println!("\nTest 4 (fully parenthesized): {}", test4);
			match db.query_as_root(test4, params![]) {
				Ok(frames) => {
					println!("  Success! Results:");
					for frame in frames {
						println!("    {}", frame);
					}
				}
				Err(e) => println!("  Failed: {:?}", e),
			}
		}
	}

	log_info!("Test complete.");
}
