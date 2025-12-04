// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{thread::sleep, time::Duration};

use reifydb::{Params, Session, WithInterceptorBuilder, WithSubsystem, embedded};
use tracing_subscriber::{EnvFilter, fmt, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
	tracing_subscriber::registry()
		.with(fmt::layer().with_span_events(FmtSpan::CLOSE))
		.with(EnvFilter::from_default_env())
		.init();

	let mut db = embedded::memory_optimistic()
		// Table interceptors for users
		.intercept_table("test.users")
			.pre_insert(|ctx| {
				println!("[TABLE] Pre-insert into: {}", ctx.table.name);
				Ok(())
			})
			.post_insert(|ctx| {
				println!("[TABLE] Post-insert into: {}", ctx.table.name);
				Ok(())
			})
		// View interceptors for active_users deferred view
		.intercept_view("test.active_users")
			.pre_insert(|ctx| {
				println!("[VIEW] Pre-insert into view: {}", ctx.view.name);
				Ok(())
			})
			.post_insert(|ctx| {
				println!("[VIEW] Post-insert into view: {}", ctx.view.name);
				Ok(())
			})
		.done()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("debug"))
		.with_worker(|w| w)
		.with_flow(|f| f)
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace and table
	println!("\n=== Creating namespace and table ===");
	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();
	db.command_as_root(r#"create table test.users { id: int4, username: utf8, active: bool }"#, Params::None)
		.unwrap();

	// Create deferred view with filter for active users only
	println!("\n=== Creating deferred view ===");
	db.command_as_root(
		r#"create deferred view test.active_users { id: int4, username: utf8 } as { from test.users filter active == true map { id: id, username: username } }"#,
		Params::None,
	)
	.unwrap();

	// Insert into users - triggers table interceptors
	// The deferred view will also be populated, triggering view interceptors
	println!("\n=== Inserting users (triggers table + view interceptors) ===");
	db.command_as_root(
		r#"from [
            {id: 1, username: "alice", active: true},
            {id: 2, username: "bob", active: false},
            {id: 3, username: "charlie", active: true}
        ] insert test.users"#,
		Params::None,
	)
	.unwrap();

	// Wait for deferred view to process
	println!("\n=== Waiting for deferred view to process ===");
	sleep(Duration::from_millis(10));

	// Query tables and views
	println!("\n=== All users ===");
	for frame in db.query_as_root(r#"from test.users"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	println!("\n=== Active users (from deferred view) ===");
	for frame in db.query_as_root(r#"from test.active_users"#, Params::None).unwrap() {
		println!("{}", frame);
	}
}
