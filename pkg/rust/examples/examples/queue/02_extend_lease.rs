// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, WithSubsystem, embedded};
use reifydb_examples::{command, utf8_column};
use tracing::info;

fn main() {
	let db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.build()
		.unwrap();

	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();
	db.admin_as_root(
		"CREATE QUEUE jobs::renders { clip: utf8 } WITH { fifo: { partitions: 1 } }",
		Params::None,
	)
	.unwrap();

	command(&db, r#"INSERT jobs::renders [{ clip: "intro.mov" }]"#);

	info!("The worker guesses this render takes under 10 seconds and leases accordingly...");
	let claimed = command(&db, r#"CALL queue::claim("renderer-1", "jobs::renders", 1, duration::seconds(10))"#);
	let token = utf8_column(&claimed, "token").remove(0);

	info!("A lease is a deadline, not a lock - once it passes, the item is handed to someone else.");
	info!("The render turns out to be long, so the worker buys more time before the deadline...");
	command(&db, &format!(r#"CALL queue::extend("{token}", duration::seconds(300))"#));

	info!("The deadline above moved out by five minutes, and the attempt number did not change.");
	info!("Extending never shortens a lease - a smaller ttl than the one already granted is ignored...");
	command(&db, &format!(r#"CALL queue::extend("{token}", duration::seconds(5))"#));

	info!("The render finishes inside the extended lease...");
	command(&db, &format!(r#"CALL queue::ack("{token}", "ok", none)"#));
}
