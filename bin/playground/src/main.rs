// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Params, Session, WithSubsystem,
	core::{
		flow::FlowChange,
		interface::{FlowNodeId, Transaction, logging::LogLevel::Info},
	},
	embedded,
	engine::{StandardCommandTransaction, StandardRowEvaluator},
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
	fn id(&self) -> FlowNodeId {
		FlowNodeId(12345)
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> reifydb::Result<FlowChange> {
		println!("INVOKED");
		Ok(FlowChange::internal(FlowNodeId(12345), change.diffs))
	}
}

impl<T: Transaction> TransformOperator<T> for MyOP {}

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

	// Create namespace
	log_info!("Creating namespace test...");
	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();

	// Create source table
	log_info!("Creating table test.source...");
	db.command_as_root(
		r#"
		create table test.source { 
			id: int4,
			value: int4,
			multiplier: int4,
			name: utf8
		}
	"#,
		Params::None,
	)
	.unwrap();

	// Create deferred view
	log_info!("Creating deferred view test.with_undefined...");
	db.command_as_root(
		r#"
		create deferred view test.with_undefined { 
			id: int4,
			result: int4,
			name: utf8,
			has_value: bool
		} as {
			from test.source
			map { 
				id,
				value * multiplier as result,
				name,
				value != undefined as has_value
			}
		}
	"#,
		Params::None,
	)
	.unwrap();

	// Insert data with undefined values
	log_info!("Inserting data with undefined values...");
	db.command_as_root(
		r#"
		from [
			{id: 1, value: 10, multiplier: 2, name: "First"},
			{id: 2, value: undefined, multiplier: 3, name: "Second"},
			{id: 3, value: 5, multiplier: undefined, name: "Third"},
			{id: 4, value: 8, multiplier: 4, name: undefined}
		] insert test.source
	"#,
		Params::None,
	)
	.unwrap();

	// Let the background task run for a while
	sleep(Duration::from_secs(1));

	// Query the source table
	log_info!("Querying test.source...");
	let result = db.query_as_root("from test.source", Params::None).unwrap();
	for frame in result {
		println!("Source data:\n{}", frame);
	}

	// Query the view
	log_info!("Querying test.with_undefined...");
	let result = db.query_as_root("from test.with_undefined", Params::None).unwrap();
	for frame in result {
		println!("View data:\n{}", frame);
	}

	log_info!("Shutting down...");
}
