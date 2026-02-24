// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! # Basic Events and Handlers Example
//!
//! Demonstrates event and handler support in ReifyDB:
//! - Creating events with structured variants
//! - Creating handlers that react to dispatched event variants
//! - Accessing event fields inside handler bodies via `event_<fieldname>`
//! - Dispatching events and observing the `handlers_fired` result
//! - Querying tables populated as side-effects by handlers
//!
//! Run with: `make basic-events-and-handlers` or `cargo run --bin basic-events-and-handlers`

use reifydb::{Params, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	let mut db = embedded::memory().build().unwrap();
	db.start().unwrap();

	info!("Creating namespace...");
	log_query("CREATE NAMESPACE shop");
	db.admin_as_root(
		r#"
		CREATE NAMESPACE shop;
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating order_event with two variants...");
	log_query(
		"CREATE EVENT shop.order_event { OrderPlaced { id: Int4, amount: Float8 }, OrderShipped { id: Int4, tracking_code: Utf8 } }",
	);
	db.admin_as_root(
		r#"
		CREATE EVENT shop.order_event {
			OrderPlaced { id: Int4, amount: Float8 },
			OrderShipped { id: Int4, tracking_code: Utf8 }
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating orders table...");
	log_query("CREATE TABLE shop.orders { id: Int4, amount: Float8 }");
	db.admin_as_root(
		r#"
		CREATE TABLE shop.orders {
			id: Int4,
			amount: Float8
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating shipments table...");
	log_query("CREATE TABLE shop.shipments { order_id: Int4, tracking_code: Utf8 }");
	db.admin_as_root(
		r#"
		CREATE TABLE shop.shipments {
			order_id: Int4,
			tracking_code: Utf8
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating handler: on_order_placed inserts into shop.orders...");
	log_query(
		"CREATE HANDLER shop.on_order_placed ON shop.order_event::OrderPlaced { INSERT shop.orders [{ id: event_id, amount: event_amount }] }",
	);
	db.admin_as_root(
		r#"
		CREATE HANDLER shop.on_order_placed ON shop.order_event::OrderPlaced {
			INSERT shop.orders [{ id: event_id, amount: event_amount }]
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating handler: on_order_shipped inserts into shop.shipments...");
	log_query(
		"CREATE HANDLER shop.on_order_shipped ON shop.order_event::OrderShipped { INSERT shop.shipments [{ order_id: event_id, tracking_code: event_tracking_code }] }",
	);
	db.admin_as_root(
		r#"
		CREATE HANDLER shop.on_order_shipped ON shop.order_event::OrderShipped {
			INSERT shop.shipments [{ order_id: event_id, tracking_code: event_tracking_code }]
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Dispatching OrderPlaced for order 1 (amount: 49.99)...");
	log_query("DISPATCH shop.order_event::OrderPlaced { id: 1, amount: 49.99 }");
	let results = db
		.command_as_root(
			r#"
			DISPATCH shop.order_event::OrderPlaced { id: 1, amount: 49.99 }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}

	info!("Dispatching OrderPlaced for order 2 (amount: 129.50)...");
	log_query("DISPATCH shop.order_event::OrderPlaced { id: 2, amount: 129.50 }");
	let results = db
		.command_as_root(
			r#"
			DISPATCH shop.order_event::OrderPlaced { id: 2, amount: 129.50 }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}

	info!("Dispatching OrderShipped for order 1 (tracking: TRK-001)...");
	log_query("DISPATCH shop.order_event::OrderShipped { id: 1, tracking_code: \"TRK-001\" }");
	let results = db
		.command_as_root(
			r#"
			DISPATCH shop.order_event::OrderShipped { id: 1, tracking_code: "TRK-001" }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}

	info!("Querying all orders (populated by on_order_placed handler)...");
	log_query("FROM shop.orders");
	let results = db
		.query_as_root(
			r#"
			FROM shop.orders
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}

	info!("Querying all shipments (populated by on_order_shipped handler)...");
	log_query("FROM shop.shipments");
	let results = db
		.query_as_root(
			r#"
			FROM shop.shipments
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}
}
