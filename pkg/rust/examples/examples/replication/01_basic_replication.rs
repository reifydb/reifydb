// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! # Basic CDC Replication Example
//!
//! Demonstrates CDC-based replication between two ReifyDB instances:
//! - Starts a primary instance that streams CDC over gRPC
//! - Starts a replica instance that connects to the primary and applies changes
//! - Schema (namespaces, tables) and row data replicate automatically
//! - Inserts data on the primary, reads it back from the replica
//!
//! Run with: `make replication-basic` or `cargo run --bin replication-basic`

use std::{thread::sleep, time::Duration};

use reifydb::{Params, WithSubsystem, server, sub_replication::subsystem::ReplicationSubsystem};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	info!("Starting primary instance...");
	let mut primary = server::memory()
		.with_tracing(|c| c.with_console(|f| f.color(true)))
		.with_replication(|c| c.primary().bind_addr("127.0.0.1:0"))
		.build()
		.unwrap();
	primary.start().unwrap();

	// Discover the port the replication server bound to
	let repl_port = primary
		.subsystem::<ReplicationSubsystem>()
		.expect("replication subsystem not found")
		.port()
		.expect("replication port not bound");
	info!("Primary replication server listening on port {}", repl_port);

	info!("Creating schema on primary...");
	primary.admin_as_root("create namespace shop;", Params::None).unwrap();
	primary.admin_as_root(
		r#"
            create table shop::products {
                id:    int4,
                name:  utf8,
                price: float8
            };
            "#,
		Params::None,
	)
	.unwrap();

	info!("Inserting products on primary...");
	log_query(
		r#"INSERT shop::products [
    { id: 1, name: "Laptop",  price: 999.99 },
    { id: 2, name: "Mouse",   price: 29.99  },
    { id: 3, name: "Monitor", price: 549.00 }
]"#,
	);
	primary.command_as_root(
		r#"
            INSERT shop::products [
                { id: 1, name: "Laptop",  price: 999.99 },
                { id: 2, name: "Mouse",   price: 29.99  },
                { id: 3, name: "Monitor", price: 549.00 }
            ];
            "#,
		Params::None,
	)
	.unwrap();

	// Verify data on primary
	info!("--- Data on PRIMARY ---");
	log_query("from shop::products");
	for frame in primary.query_as_root("from shop::products", Params::None).unwrap() {
		info!("{}", frame);
	}

	// The replica connects to the primary's replication endpoint.
	// Schema and data replicate automatically — no need to create
	// tables on the replica.
	info!("Starting replica instance...");
	let mut replica = server::memory()
		.with_replication(move |c| c.replica().primary_addr(format!("http://127.0.0.1:{}", repl_port)))
		.build()
		.unwrap();
	replica.start().unwrap();

	// Give the replica time to connect and replicate
	info!("Waiting for replication to catch up...");
	sleep(Duration::from_millis(500));

	info!("--- Data on REPLICA ---");
	log_query("from shop::products");
	for frame in replica.query_as_root("from shop::products", Params::None).unwrap() {
		info!("{}", frame);
	}

	info!("Inserting more data on primary...");
	log_query(r#"INSERT shop::products [{ id: 4, name: "Keyboard", price: 79.99 }]"#);
	primary.command_as_root(
		r#"
            INSERT shop::products [
                { id: 4, name: "Keyboard", price: 79.99 }
            ];
            "#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	info!("--- Updated data on REPLICA ---");
	log_query("from shop::products");
	for frame in replica.query_as_root("from shop::products", Params::None).unwrap() {
		info!("{}", frame);
	}

	info!("Shutting down...");
	primary.stop().unwrap();
	info!("Done!");
}
