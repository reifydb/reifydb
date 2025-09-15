// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Params, Session, WithSubsystem,
	core::{
		flow::FlowChange,
		interface::{
			FlowNodeId, Transaction,
			subsystem::logging::LogLevel::Info,
		},
	},
	embedded,
	engine::{StandardCommandTransaction, StandardEvaluator},
	log_info,
	sub_flow::{FlowBuilder, Operator, TransformOperator},
	sub_logging::{FormatStyle, LoggingBuilder},
};

pub type DB = MemoryDatabaseOptimistic;

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| {
		console.color(true)
			.stderr_for_errors(true)
			.format_style(FormatStyle::Timeline)
	})
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
	flow.register_operator("test".to_string(), |_node, _exprs| {
		Ok(Box::new(MyOP {}))
	})
}

fn main() {
	let mut db: DB = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.with_flow(flow_configuration)
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace and tables for mod operator tests
	db.command_as_root(r#"create namespace test"#, Params::None).unwrap();
	db.command_as_root(
		r#"
		create table test.transactions {
			id: int8,
			user_id: utf8,
			amount: float8,
			timestamp: int8
		}
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		create table test.users {
			user_id: utf8,
			name: utf8,
			account_type: utf8
		}
		"#,
		Params::None,
	)
	.unwrap();

	log_info!("=== Testing Apply operator in deferred views ===");

	// Create a view with row numbers using the counter operator
	db.command_as_root(
		r#"
		create deferred view test.numbered_transactions {
			row_number: int8,
			transaction_id: int8,
			user_id: utf8,
			amount: float8
		} as {
			from test.transactions
			apply test {}
			map {
				row_number: row_number,
				transaction_id: id,
				user_id: user_id,
				amount: amount
			}
		}
		"#,
		Params::None,
	)
	.unwrap();

	// Create a view using running_sum via Apply
	db.command_as_root(
		r#"
		create deferred view test.running_totals {
			user_id: utf8,
			amount: float8,
			running_total: float8
		} as {
			from test.transactions
			apply running_sum amount
			map {
				user_id: user_id,
				amount: amount,
				running_total: running_sum
			}
		}
		"#,
		Params::None,
	)
	.unwrap();

	// Create a view using running_avg via Apply
	db.command_as_root(
		r#"
		create deferred view test.running_averages {
			user_id: utf8,
			amount: float8,
			running_avg: float8
		} as {
			from test.transactions
			apply running_avg amount
			map {
				user_id: user_id,
				amount: amount,
				running_avg: running_avg
			}
		}
		"#,
		Params::None,
	)
	.unwrap();

	// Insert sample data AFTER creating views
	db.command_as_root(
		r#"
		from [
			{id: 1, user_id: 'user001', amount: 100.50, timestamp: 1700000000},
			{id: 2, user_id: 'user002', amount: 250.75, timestamp: 1700000100},
			{id: 3, user_id: 'user001', amount: 75.25, timestamp: 1700000200},
			{ id: 4, user_id: 'user003', amount: 500.00, timestamp: 1700000300},
			{id: 5, user_id: 'user002', amount: 150.00, timestamp: 1700000400}
			]
		insert test.transactions
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ user_id: 'user001', name: 'Alice', account_type: 'premium'},
			{ user_id: 'user002', name: 'Bob', account_type: 'standard' },
			{ user_id: 'user003', name: 'Charlie', account_type: 'premium'}
		]
		insert test.users
		"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(100));

	// Query the numbered transactions view
	log_info!("\n=== Numbered Transactions (with counter) ===");
	for frame in db
		.query_as_root(
			r#"from test.numbered_transactions"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}");
	}

	// Query the running totals view
	log_info!("\n=== Running Totals (with running_sum) ===");
	for frame in db
		.query_as_root(r#"from test.running_totals"#, Params::None)
		.unwrap()
	{
		log_info!("{frame}");
	}

	// Query the running averages view
	log_info!("\n=== Running Averages (with running_avg) ===");
	for frame in db
		.query_as_root(r#"from test.running_averages"#, Params::None)
		.unwrap()
	{
		log_info!("{frame}");
	}

	sleep(Duration::from_millis(100));
}
