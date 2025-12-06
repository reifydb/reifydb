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
		// .with_worker(|w| w)  // Disabled to use single-threaded worker
		.with_flow(|f| f)
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace
	db.command_as_root(r#"create namespace rb-delete"#, Params::None).unwrap();

	// Create ring buffer with capacity 100
	db.command_as_root(
		r#"create ringbuffer rb-delete.event-log { id: int4, severity: utf8 } with { capacity: 100 }"#,
		Params::None,
	)
	.unwrap();

	// Insert 3 rows
	db.command_as_root(
		r#"from [{ id: 1, severity: "info" }, { id: 2, severity: "error" }, { id: 3, severity: "info" }] insert rb-delete.event-log"#,
		Params::None,
	)
	.unwrap();

	println!("\n=== Initial buffer (3 items) ===");
	for frame in db.query_as_root(r#"from rb-delete.event-log"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Delete rows with severity == "info" (should delete 2 rows: id 1 and 3)
	println!("\n=== Deleting rows with severity == 'info' ===");
	for frame in db
		.command_as_root(
			r#"from rb-delete.event-log filter severity == "info" delete rb-delete.event-log"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Query remaining rows - should show ONLY id=2 with severity="error"
	// BUG: Currently returns empty result (all rows deleted)
	println!("\n=== After filtered delete (should show id=2, severity='error') ===");
	for frame in db.query_as_root(r#"from rb-delete.event-log"#, Params::None).unwrap() {
		println!("{}", frame);
	}
}
