// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, WithSubsystem, embedded};
use reifydb_examples::{command, query, utf8_column};
use tracing::info;

fn main() {
	let db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.build()
		.unwrap();

	info!("A queue is declared like a table, with a fifo dispatch policy...");
	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();
	db.admin_as_root(
		r#"
		CREATE QUEUE jobs::emails {
			recipient: utf8,
			subject: utf8
		} WITH { fifo: { partitions: 1 } };
		"#,
		Params::None,
	)
	.unwrap();

	info!("Enqueueing is a plain INSERT - there is no special producer API...");
	command(
		&db,
		r#"
		INSERT jobs::emails [
			{ recipient: "ada@example.com", subject: "welcome" },
			{ recipient: "grace@example.com", subject: "invoice" }
		]
		"#,
	);

	info!("depth counts what is waiting, in_flight what a worker currently holds...");
	query(&db, r#"FROM system::queues FILTER { name == "emails" } MAP { name, depth, in_flight }"#);

	info!("A worker claims work by name, taking a lease for as long as it expects to need...");
	let claimed = command(&db, r#"CALL queue::claim("worker-1", "jobs::emails", 2, duration::seconds(30))"#);

	info!("The claim hands back a token per item, plus the item's payload columns...");
	query(&db, r#"FROM system::queues FILTER { name == "emails" } MAP { name, depth, in_flight }"#);

	info!("The token is what proves the worker still holds the lease when it reports back...");
	for token in utf8_column(&claimed, "token") {
		command(&db, &format!(r#"CALL queue::ack("{token}")"#));
	}

	info!("Both jobs are done - nothing waiting, nothing in flight...");
	query(&db, r#"FROM system::queues FILTER { name == "emails" } MAP { name, depth, in_flight }"#);
}
