// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! # Table Interceptors
//!
//! Demonstrates the fluent interceptor API for ReifyDB:
//! - Registering pre/post insert hooks on tables
//! - Filtering interceptors by namespace.table pattern
//! - Using closures for lightweight interceptor logic
//!
//! Interceptors allow you to:
//! - Audit data changes
//! - Validate data before/after operations
//! - Trigger side effects (notifications, logging, etc.)
//!
//! Run with: `make intercept-table-view` or `cargo run --bin intercept-table-view`

use std::{thread::sleep, time::Duration};

use reifydb::{Params, WithInterceptorBuilder, WithSubsystem, embedded};
use reifydb_examples::log_query;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
	tracing_subscriber::registry()
		.with(fmt::layer().with_span_events(FmtSpan::CLOSE))
		.with(EnvFilter::from_default_env())
		.init();

	// Step 1: Create database with interceptors configured
	// The fluent API allows chaining interceptor registrations
	info!("Creating database with interceptors...");

	let mut db = embedded::memory()
		.intercept()
		.table("test::users")
			.pre_insert(|ctx| {
				info!("[TABLE INTERCEPTOR] Pre-insert into: {}", ctx.table.name);
				Ok(())
			})
			.post_insert(|ctx| {
				info!("[TABLE INTERCEPTOR] Post-insert into: {}", ctx.table.name);
				Ok(())
			})
		.done()
		// Enable required subsystems
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("debug"))
		.with_flow(|f| f) // Required for deferred views
		.build()
		.unwrap();

	db.start().unwrap();

	// Step 2: Create namespace and table
	info!("\n--- Creating namespace and table ---");
	db.admin_as_root(r#"create namespace test;"#, Params::None).unwrap();

	log_query("create table test::users { id: int4, username: utf8, active: bool }");
	db.admin_as_root(r#"create table test::users { id: int4, username: utf8, active: bool }"#, Params::None)
		.unwrap();

	// Step 3: Create a deferred view that filters active users
	info!("\n--- Creating deferred view ---");
	log_query(
		"create deferred view test::active_users { id: int4, username: utf8 } as { from test::users filter active == true map { id: id, username: username } }",
	);
	db.admin_as_root(
		r#"create deferred view test::active_users {
				id: int4,
				username: utf8
			} as {
				from test::users filter active == true map { id: id, username: username }
			}"#,
		Params::None,
	)
	.unwrap();

	// Step 4: Insert data - this triggers the table interceptors
	info!("\n--- Inserting users (triggers table interceptors) ---");
	log_query(
		r#"INSERT test::users [
    {id: 1, username: "alice", active: true},
    {id: 2, username: "bob", active: false},
    {id: 3, username: "charlie", active: true}
]"#,
	);
	db.command_as_root(
		r#"INSERT test::users [
            {id: 1, username: "alice", active: true},
            {id: 2, username: "bob", active: false},
            {id: 3, username: "charlie", active: true}
        ]"#,
		Params::None,
	)
	.unwrap();

	// Wait for deferred view to process the data
	info!("\n--- Waiting for deferred view to process ---");
	sleep(Duration::from_millis(100));

	// Step 5: Query the results
	info!("\n--- All users (from table) ---");
	log_query("from test::users");
	for frame in db.query_as_root(r#"from test::users"#, Params::None).unwrap() {
		info!("{}", frame);
	}

	info!("\n--- Active users only (from deferred view) ---");
	log_query("from test::active_users");
	for frame in db.query_as_root(r#"from test::active_users"#, Params::None).unwrap() {
		info!("{}", frame);
	}

	info!("\nExample complete. Notice how table interceptors fired for each insert.");
}
