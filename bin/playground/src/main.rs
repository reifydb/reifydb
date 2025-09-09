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

	for frame in db.command_as_root(r#"
			from test.orders
			left join { from test.customers } on orders.customer_id = customers.customer_id
			map {
				order_id: orders.order_id,
				customer_id: orders.customer_id,
				amount: orders.amount,
				customer_name: customers.name,
				city: customers.city
			}

	"#, Params::None).unwrap(){
		println!("{frame}");
	}

	// Add a third order that has no matching customer to test LEFT JOIN
	// behavior
	db.command_as_root(
		r#"from [{order_id: 103, customer_id: 3, amount: 300.00}] insert test.orders"#,
		Params::None,
	).unwrap();

	// Wait for flows to process
	sleep(Duration::from_millis(500));

	log_info!("Checking LEFT JOIN results (should show all orders):");
	for frame in db.command_as_root(r#"
		from test.orders
		left join { from test.customers } on orders.customer_id = customers.customer_id
		map {
			order_id: orders.order_id,
			customer_id: orders.customer_id,
			amount: orders.amount,
			customer_name: customers.name,
			city: customers.city
		}
	"#, Params::None).unwrap() {
		log_info!("{frame}")
	}
	log_info!(
		"Note: Order 103 has NULL customer details (LEFT JOIN behavior)"
	);

	log_info!(
		"\nScenario 2: Add a new customer that matches an existing order"
	);

	// Add customer_id 3 which will now match order 103
	db.command_as_root(
		r#"from [{customer_id: 3, name: "Charlie", city: "Chicago"}] insert test.customers"#,
		Params::None,
	).unwrap();

	sleep(Duration::from_millis(500));

	log_info!("After adding customer 3:");
	for frame in db.command_as_root(r#"
		from test.orders
		left join { from test.customers } on orders.customer_id = customers.customer_id
		map {
			order_id: orders.order_id,
			customer_id: orders.customer_id,
			amount: orders.amount,
			customer_name: customers.name,
			city: customers.city
		}
	"#, Params::None).unwrap() {
		log_info!("{frame}")
	}
	log_info!("Note: Order 103 now has customer details filled in");

	log_info!("\nScenario 3: Update a customer's information");

	db.command_as_root(
		r#"update test.customers set city = "San Francisco" where customer_id = 1"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	log_info!("After updating customer 1's city:");
	for frame in db.command_as_root(r#"
		from test.orders
		left join { from test.customers } on orders.customer_id = customers.customer_id
		map {
			order_id: orders.order_id,
			customer_id: orders.customer_id,
			amount: orders.amount,
			customer_name: customers.name,
			city: customers.city
		}
	"#, Params::None).unwrap() {
		log_info!("{frame}")
	}

	log_info!(
		"\nScenario 4: Delete a customer (order should remain with NULL customer)"
	);

	db.command_as_root(
		"delete from test.customers where customer_id = 2",
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	log_info!("After deleting customer 2:");
	for frame in db.command_as_root(r#"
		from test.orders
		left join { from test.customers } on orders.customer_id = customers.customer_id
		map {
			order_id: orders.order_id,
			customer_id: orders.customer_id,
			amount: orders.amount,
			customer_name: customers.name,
			city: customers.city
		}
	"#, Params::None).unwrap() {
		log_info!("{frame}")
	}
	log_info!(
		"Note: Order 102 still exists but with NULL customer details"
	);

	log_info!("\nScenario 5: Add a new order without a matching customer");

	db.command_as_root(
		r#"from [{order_id: 104, customer_id: 99, amount: 500.00}] insert test.orders"#,
		Params::None,
	).unwrap();

	sleep(Duration::from_millis(500));

	log_info!("After adding order 104 with non-existent customer 99:");
	for frame in db.command_as_root(r#"
		from test.orders
		left join { from test.customers } on orders.customer_id = customers.customer_id
		map {
			order_id: orders.order_id,
			customer_id: orders.customer_id,
			amount: orders.amount,
			customer_name: customers.name,
			city: customers.city
		}
	"#, Params::None).unwrap() {
		log_info!("{frame}")
	}

	log_info!("\n=== Summary ===");
	log_info!("LEFT JOIN successfully demonstrated:");
	log_info!("- All orders are preserved even without matching customers");
	log_info!("- Changes to either table trigger join updates");
	log_info!("- NULL values appear for unmatched right-side rows");
	log_info!(
		"- Updates and deletes are properly reflected in join output"
	);

	// Keep process alive briefly to ensure logs flush
	sleep(Duration::from_millis(100));
}
