// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! # Deferred View Interceptors
//!
//! Demonstrates the fluent interceptor API for deferred views:
//! - Registering post_insert and post_delete hooks on view data
//! - Filtering interceptors by namespace.view pattern
//! - Inspecting view metadata (name, kind, columns) in interceptor callbacks
//!
//! View data interceptors allow you to:
//! - Audit row-level changes in materialized views
//! - React to inserts, updates, and deletes on view data
//! - Trigger side effects when view rows change
//!
//! Run with: `make intercept-deferred-view` or `cargo run --bin intercept-deferred-view`

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

	// Step 1: Create database with view interceptors configured
	// The fluent API allows chaining interceptor registrations for views
	info!("Creating database with view interceptors...");

	let mut db = embedded::memory()
		.intercept()
		.view("test.active_users")
		.post_insert(|ctx| {
			info!("[VIEW INTERCEPTOR] Post-insert into view: {}", ctx.view.name);
			Ok(())
		})
		.post_delete(|ctx| {
			info!("[VIEW INTERCEPTOR] Post-delete from view: {}", ctx.view.name);
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

	log_query("create table test.users { id: int4, username: utf8, active: bool }");
	db.admin_as_root(r#"create table test.users { id: int4, username: utf8, active: bool }"#, Params::None)
		.unwrap();

	// Step 3: Create a deferred view
	info!("\n--- Creating deferred view ---");
	log_query(
		"create deferred view test.active_users { id: int4, username: utf8 } as { from test.users filter active == true map { id: id, username: username } }",
	);
	db.admin_as_root(
		r#"create deferred view test.active_users { id: int4, username: utf8 } as { from test.users filter active == true map { id: id, username: username } }"#,
		Params::None,
	)
	.unwrap();

	// Step 4: Insert data into the source table — triggers post_insert on the view
	info!("\n--- Inserting users into source table (triggers view post_insert interceptor) ---");
	log_query(
		r#"INSERT test.users [
    {id: 1, username: "alice", active: true},
    {id: 2, username: "bob", active: false},
    {id: 3, username: "charlie", active: true}
]"#,
	);
	db.command_as_root(
		r#"INSERT test.users [
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

	// Step 5: Query the deferred view to verify it works
	info!("\n--- Active users (from deferred view) ---");
	log_query("from test.active_users");
	for frame in db.query_as_root(r#"from test.active_users"#, Params::None).unwrap() {
		info!("{}", frame);
	}

	info!(
		"\nExample complete. Notice how the view data interceptors fired when rows were inserted into the materialized view."
	);
}
