// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{sync::atomic::Ordering, thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Session, WithSubsystem,
	core::{
		flow::FlowChange,
		interface::{FlowNodeId, Params, Transaction, subsystem::logging::LogLevel::Info},
	},
	embedded,
	engine::{StandardCommandTransaction, StandardEvaluator},
	log_info,
	sub_flow::{FlowBuilder, Operator, TransformOperator},
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

struct MyOP;

impl<T: Transaction> Operator<T> for MyOP {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: &FlowChange,
		_evaluator: &StandardEvaluator,
	) -> reifydb::Result<FlowChange> {
		println!("INVOKED");
		Ok(change.clone())
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

	// Test ring buffer delete and re-insert
	log_info!("Testing ring buffer delete/insert issue...");

	// Create namespace and ring buffer
	log_info!("Creating namespace and ring buffer...");
	for frame in db.command_as_root("create namespace test_rb_del", Params::None).unwrap() {
		log_info!("Result: {}", frame);
	}
	for frame in db
		.command_as_root(
			"create ring buffer test_rb_del.buffer { id: int4, value: utf8 } with capacity = 10",
			Params::None,
		)
		.unwrap()
	{
		log_info!("Result: {}", frame);
	}

	// Insert initial data
	log_info!("Inserting initial data...");
	for frame in db
		.command_as_root(
			r#"
		from [
		  { id: 1, value: "One" },
		  { id: 2, value: "Two" },
		  { id: 3, value: "Three" }
		] insert test_rb_del.buffer
	"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("Insert result: {}", frame);
	}

	// Query to verify
	log_info!("Querying initial data...");
	for frame in db.query_as_root("from test_rb_del.buffer", Params::None).unwrap() {
		log_info!("Initial data: {}", frame);
	}

	// Delete all
	log_info!("Deleting all data...");
	for frame in db.command_as_root("delete test_rb_del.buffer", Params::None).unwrap() {
		log_info!("Delete result: {}", frame);
	}

	// Query to verify empty
	log_info!("Querying after delete...");
	for frame in db.query_as_root("from test_rb_del.buffer", Params::None).unwrap() {
		log_info!("After delete: {}", frame);
	}

	// Insert new data
	log_info!("Inserting new data...");
	for frame in db
		.command_as_root(r#"from [{ id: 10, value: "New" }] insert test_rb_del.buffer"#, Params::None)
		.unwrap()
	{
		log_info!("Insert new result: {}", frame);
	}

	// Query to verify new data
	log_info!("Querying new data...");
	for frame in db.query_as_root("from test_rb_del.buffer", Params::None).unwrap() {
		log_info!("New data: {}", frame);
	}

	log_info!("Test completed successfully!");

	// Small delay before shutdown
	sleep(Duration::from_secs(1));
	log_info!("Shutting down...");
}
