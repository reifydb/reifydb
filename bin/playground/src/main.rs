// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Params, Session, WithSubsystem,
	core::interface::logging::LogLevel::Info,
	embedded, log_info,
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

fn main() {
	let mut db: DB =
		embedded::memory_optimistic().with_logging(logger_configuration).with_worker(|wp| wp).build().unwrap();

	db.start().unwrap();

	// Test left join where one left record matches multiple right records
	// Should produce multiple output rows for one-to-many relationships

	// Create namespace
	log_info!("Creating namespace test...");
	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();

	// Create tables
	log_info!("Creating table test.customers...");
	db.command_as_root(r#"create table test.customers { id: int4, name: utf8, city: utf8 }"#, Params::None)
		.unwrap();

	log_info!("Creating table test.orders...");
	db.command_as_root(
		r#"create table test.orders { id: int4, customer_id: int4, product: utf8, amount: float8 }"#,
		Params::None,
	)
	.unwrap();

	// Create LEFT JOIN view for customer orders
	log_info!("Creating deferred view test.customer_orders...");
	db.command_as_root(
		r#"
create deferred view test.customer_orders {
    customer_name: utf8,
    city: utf8,
    order_id: int4,
    product: utf8,
    amount: float8
} as {
    from test.customers
    left join { from test.orders } orders on id == orders.customer_id with { strategy: lazy_loading }
    map {
        customer_name: name,
        city: city,
        order_id: orders_id,
        product: product,
        amount: amount
    }
}
	"#,
		Params::None,
	)
	.unwrap();

	// Insert customers
	log_info!("Inserting customers...");
	db.command_as_root(
		r#"
from [
    {id: 1, name: "Alice", city: "New York"},
    {id: 2, name: "Bob", city: "Los Angeles"},
    {id: 3, name: "Charlie", city: "Chicago"}
] insert test.customers
	"#,
		Params::None,
	)
	.unwrap();

	// Insert multiple orders for customer 1, one order for customer 2, no orders for customer 3
	log_info!("Inserting orders...");
	db.command_as_root(
		r#"
from [
    {id: 101, customer_id: 1, product: "Laptop", amount: 12000},
    {id: 102, customer_id: 1, product: "Mouse", amount: 250},
    {id: 103, customer_id: 1, product: "Keyboard", amount: 750},
    {id: 104, customer_id: 2, product: "Phone", amount: 8000}
] insert test.orders
	"#,
		Params::None,
	)
	.unwrap();

	// Let the background task process
	// sleep(Duration::from_millis(500));

	sleep(Duration::from_millis(10));

	// Final query - Alice should now appear 5 times
	log_info!("Final view query after adding more orders...");
	let result = db
		.query_as_root("from test.customer_orders sort { customer_name asc, order_id asc }", Params::None)
		.unwrap();
	for frame in result {
		println!("Final customer orders:\n{}", frame);
	}

	log_info!("✅ Test completed successfully!");
	log_info!("Shutting down...");
}
