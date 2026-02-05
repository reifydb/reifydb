// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! # Subscription Cursor Example
//!
//! Demonstrates pull-based subscription consumption in ReifyDB:
//! - Creating a subscription with an AS query
//! - Inserting data that flows through CDC into the subscription
//! - Consuming changes via `SubscriptionCursor::next`
//!
//! Run with: `make subscription-cursor` or `cargo run --bin subscription-cursor`

use std::{thread::sleep, time::Duration};

use reifydb::{Frame, Params, WithSubsystem, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	let mut db = embedded::memory()
		.with_tracing(|c| c.with_console(|f| f.color(true)))
		.with_flow(|f| f)
		.build()
		.unwrap();

	db.start().unwrap();

	// Create a namespace and table
	info!("Creating namespace and table...");
	log_query("create namespace test");
	db.admin_as_root("create namespace test;", Params::None).unwrap();

	log_query("create table test.events { id: int4, name: utf8, status: utf8 }");
	db.admin_as_root(
		r#"
		create table test.events {
			id: int4,
			name: utf8,
			status: utf8
		};
		"#,
		Params::None,
	)
	.unwrap();

	// Create a subscription that watches the events table
	info!("Creating subscription...");
	log_query("create subscription { } as { from test.events }");
	let mut cursor = db.subscribe_as_root("from test.events", 100).unwrap();

	// Insert data into the table — this triggers the CDC pipeline
	// which writes to subscription storage
	info!("Inserting events...");
	log_query(
		r#"INSERT test.events [
    { id: 1, name: "deploy", status: "success" },
    { id: 2, name: "build", status: "failed" },
    { id: 3, name: "test", status: "success" }
]"#,
	);
	db.command_as_root(
		r#"
		INSERT test.events [
			{ id: 1, name: "deploy", status: "success" },
			{ id: 2, name: "build", status: "failed" },
			{ id: 3, name: "test", status: "success" }
		];
		"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(100));

	// Consume subscription data via cursor
	info!("Consuming subscription data via cursor...");

	if let Some(columns) = cursor.next().unwrap() {
		let frame: Frame = columns.into();
		info!("Received subscription data:");
		info!("{}", frame);
	} else {
		info!("No subscription data available (CDC pipeline may not have flushed yet)");
	}

	// Second call should return None — data was consumed and deleted
	match cursor.next().unwrap() {
		Some(_) => info!("More data available"),
		None => info!("No more data — subscription fully consumed"),
	}
}
