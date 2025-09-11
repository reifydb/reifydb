// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Params, Session, WithSubsystem,
	core::interface::subsystem::logging::LogLevel::Info,
	embedded, log_info,
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

fn main() {
	let mut db: DB = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.build()
		.unwrap();

	db.start().unwrap();

	log_info!("=== Testing LEFT JOIN with Flow System ===");

	log_info!("Creating schema and tables...");
	db.query_as_root("FROM system.versions TAKE 1", Params::None).unwrap();
	db.command_as_root("create schema test", Params::None).unwrap();

	// Create orders table (left side)
	db.command_as_root(
		"create table test.orders { order_id: int4, customer_id: int4, amount: float8 }",
		Params::None,
	)
	.unwrap();

	// Create customers table (right side)
	db.command_as_root(
		"create table test.customers { customer_id: int4, name: utf8, city: utf8 }",
		Params::None,
	)
	.unwrap();

	log_info!("Creating LEFT JOIN view...");
	// Create a view that performs a LEFT JOIN
	// This will include all orders, even those without matching customers
	// The view needs to map the joined columns to its output schema
	db.command_as_root(
		r#"create deferred view test.order_details {
			order_id: int4,
			customer_id: int4,
			amount: float8,
			customer_name: utf8,
			customer_city: utf8
		} as {
			from test.orders
			left join { from test.customers } on orders.customer_id = customers.customer_id
			map {
				order_id: orders.order_id,
				customer_id: orders.customer_id,
				amount: orders.amount,
				customer_name: customers.name,
				customer_city: customers.city
			}
		}"#,
		Params::None,
	).unwrap();

	// log_info!("Scenario 1: Insert orders with some matching customers");
	//
	// // Insert customers (only customer_id 1 and 2)
	db.command_as_root(
		r#"from [
			{customer_id: 1, name: "Alice", city: "New York"},
			{customer_id: 2, name: "Bob", city: "Los Angeles"}
		] insert test.customers"#,
		Params::None,
	)
	.unwrap();

	// Insert orders (customer_id 1, 2, and 3 - note 3 has no matching
	// customer)
	db.command_as_root(
		r#"from [
			{order_id: 101, customer_id: 1, amount: 250.50},
			{order_id: 102, customer_id: 2, amount: 175.00},
		] insert test.orders"#,
		Params::None,
	)
	.unwrap();

	// Check if the flow was created
	log_info!("Checking if flow was created:");
	for _frame in
		db.command_as_root("from reifydb.flows", Params::None).unwrap()
	{
		log_info!("Flow found in reifydb.flows");
		break;
	}

	// for frame in db.command_as_root(r#"
	// 		from test.orders
	// 		left join { from test.customers } on orders.customer_id =
	// customers.customer_id 		map {
	// 			order_id: orders.order_id,
	// 			customer_id: orders.customer_id,
	// 			amount: orders.amount,
	// 			customer_name: customers.name,
	// 			city: customers.city
	// 		}
	//
	// "#, Params::None).unwrap(){
	// 	println!("{frame}");
	// }

	// Add a third order that has no matching customer to test LEFT JOIN
	// behavior
	db.command_as_root(
		r#"from [{order_id: 103, customer_id: 3, amount: 300.00}] insert test.orders"#,
		Params::None,
	).unwrap();

	// Wait for flows to process
	sleep(Duration::from_millis(10));

	// First check if orders and customers tables have data
	log_info!("Checking base tables:");
	log_info!("Orders table:");
	for frame in
		db.command_as_root("from test.orders", Params::None).unwrap()
	{
		log_info!("{frame}");
	}
	log_info!("Customers table:");
	for frame in
		db.command_as_root("from test.customers", Params::None).unwrap()
	{
		log_info!("{frame}");
	}

	log_info!("Checking LEFT JOIN results (should show all orders):");
	for frame in db
		.command_as_root(
			r#"
		from test.order_details
	"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}
	log_info!(
		"Note: Order 103 has UNDEFINED customer details (LEFT JOIN behavior)"
	);

	log_info!(
		"\nScenario 2: Add a new customer that matches an existing order"
	);

	// Add customer_id 3 which will now match order 103
	db.command_as_root(
		r#"from [{customer_id: 3, name: "Charlie", city: "Chicago"}] insert test.customers"#,
		Params::None,
	).unwrap();

	sleep(Duration::from_millis(5));

	log_info!("After adding customer 3:");
	for frame in db
		.command_as_root(
			r#"
		from test.order_details
	"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}
	log_info!("Note: Order 103 now has customer details filled in");

	// Add more orders after view creation to verify they appear
	db.command_as_root(
		r#"from [
			{order_id: 105, customer_id: 1, amount: 350.00},
			{order_id: 106, customer_id: 2, amount: 425.00}
		] insert test.orders"#,
		Params::None,
	)
	.unwrap();
	sleep(Duration::from_millis(5));

	log_info!("After adding orders 105 and 106:");
	for frame in db
		.command_as_root(
			r#"
		from test.order_details
	"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	log_info!("\nScenario 3: Update a customer's information");

	// In RQL, update syntax is: from <source> filter <condition> update {
	// field: value }
	db.command_as_root(
		r#"from test.customers filter customer_id = 1 MAP { customer_id: customer_id, name: name, city: "San Francisco" } update test.customers"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(5));

	log_info!("After updating customer 1's city:");
	for frame in db
		.command_as_root(
			r#"
		from test.order_details
	"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	log_info!(
		"\nScenario 4: Delete a customer (order should remain with UNDEFINED customer)"
	);

	db.command_as_root(
		"from test.customers filter customer_id = 2 delete test.customers",
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(5));

	log_info!("After deleting customer 2:");
	for frame in db
		.command_as_root(
			r#"
		from test.order_details
	"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}
	log_info!(
		"Note: Order 102 still exists but with UNDEFINED customer details"
	);

	log_info!("\nScenario 5: Add a new order without a matching customer");

	db.command_as_root(
		r#"from [{order_id: 104, customer_id: 99, amount: 5.00}] insert test.orders"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(5));

	log_info!("After adding order 104 with non-existent customer 99:");
	for frame in db
		.command_as_root(
			r#"
		from test.order_details
	"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{frame}")
	}

	// Keep process alive briefly to ensure logs flush
	sleep(Duration::from_millis(100));
}
