// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering, Ordering::Relaxed},
	},
	thread::sleep,
	time::Duration,
};

use reifydb::{
	Identity, MemoryDatabaseOptimistic, WithSubsystem,
	core::{
		flow::FlowChange,
		interface::{Engine, FlowNodeId, Transaction, logging::LogLevel::Info},
	},
	embedded,
	engine::{StandardCommandTransaction, StandardEvaluator},
	log_info,
	sub_flow::{FlowBuilder, Operator, TransformOperator},
	sub_logging::{FormatStyle, LoggingBuilder},
	task,
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

	// Schedule a background task that prints every 2 seconds
	let counter = Arc::new(AtomicUsize::new(0));
	let counter_clone = counter.clone();

	let task = task!(Low, "periodic_printer", move |ctx| {
		let frames = ctx
			.engine()
			.query_as(&Identity::root(), "MAP $1", params![counter.load(Relaxed) as u8])
			.unwrap();
		for frame in frames {
			println!("{}", frame);
		}

		let count = counter_clone.fetch_add(1, Ordering::Relaxed);
		log_info!("Background task execution #{}", count + 1);
		Ok(())
	});

	let _handle = db.scheduler().schedule_every(task, Duration::from_secs(2)).unwrap();

	db.start().unwrap();

	// Let the background task run for a while
	log_info!("Letting background task run for 7 seconds...");
	sleep(Duration::from_secs(7));
	log_info!("Shutting down...");
}
