// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Params, Session, WithSubsystem,
	core::interface::{FlowNodeId, Transaction, logging::LogLevel::Info},
	embedded,
	engine::{StandardCommandTransaction, StandardRowEvaluator},
	log_info,
	sub_flow::{FlowBuilder, Operator, TransformOperator, flow::FlowChange},
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
		create table test.products { id: int4, name: utf8 };
		create table test.categories { id: int4, product_id: int4, category: utf8 };
	"#,
		Params::None,
	)
	.unwrap();

	// Create deferred view
	log_info!("Creating deferred view test.with_undefined...");
	db.command_as_root(
		r#"
create deferred view test.product_catalog {
    id: int4,
    name: utf8,
    category: utf8
} as {
    from test.products
    left join { from test.categories } categories on id == categories.product_id
    map {
        id: id,
        name: name,
        category: category
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
    {id: 1, name: "Laptop"},
    {id: 2, name: "Phone"},
    {id: 3, name: "Tablet"}
] insert test.products
	"#,
		Params::None,
	)
	.unwrap();

	// Let the background task run for a while
	sleep(Duration::from_secs(1));

	// Query the view
	log_info!("Querying test.with_undefined...");
	let result = db.query_as_root("from test.product_catalog sort id asc", Params::None).unwrap();
	for frame in result {
		println!("View data:\n{}", frame);
	}

	log_info!("Shutting down...");
}
