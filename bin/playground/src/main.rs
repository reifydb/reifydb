// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::{Params, Session, WithSubsystem, embedded};
use tracing_subscriber::{EnvFilter, fmt, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
	tracing_subscriber::registry()
		.with(fmt::layer().with_span_events(FmtSpan::CLOSE))
		.with(EnvFilter::from_default_env())
		.init();

	let mut db = embedded::memory_optimistic()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("debug"))
		.with_worker(|w| w)
		.with_flow(|f| f)
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace
	db.command_as_root(r#"create namespace test"#, Params::None).unwrap();

	// Create table
	db.command_as_root(r#"create table test.users { id: int4, name: utf8, active: bool }"#, Params::None).unwrap();

	// Create deferred view (filters active users)
	db.command_as_root(
		r#"create deferred view test.active_users { id: int4, name: utf8 } as { from test.users filter active == true map { id: id, name: name } }"#,
		Params::None,
	).unwrap();

	// Insert some data
	db.command_as_root(
		r#"from [
			{id: 1, name: "alice", active: true},
			{id: 2, name: "bob", active: false},
			{id: 3, name: "charlie", active: true}
		] insert test.users"#,
		Params::None,
	)
	.unwrap();

	// Query table
	println!("\n=== test.users ===");
	for frame in db.query_as_root(r#"from test.users"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Query deferred view
	println!("\n=== test.active_users (deferred view) ===");
	for frame in db.query_as_root(r#"from test.active_users"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Query system.flow_nodes
	println!("\n=== system.flow_nodes ===");
	for frame in db.query_as_root(r#"from system.flow_nodes"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Query system.flow_node_types
	println!("\n=== system.flow_node_types ===");
	for frame in db.query_as_root(r#"from system.flow_node_types"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Left join flow_nodes with flow_node_types
	println!("\n=== flow_nodes left join flow_node_types ===");
	for frame in db
		.query_as_root(
			r#"from system.flow_nodes left join { from system.flow_node_types } t on t.id = node_type"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}
}
