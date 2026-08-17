// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, WithSubsystem, embedded};
use reifydb_examples::{command, query};
use tracing::info;

fn main() {
	let db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.build()
		.unwrap();

	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();

	info!("A producer that retries after an ambiguous failure would enqueue the same work twice.");
	info!("deduplicate names the columns that identify a job, and how long to remember it...");
	db.admin_as_root(
		r#"
		CREATE QUEUE jobs::receipts {
			order_id: int4,
			amount: int4
		} WITH { fifo: { partitions: 1 }, deduplicate: { by: {order_id}, ttl: 30d } };
		"#,
		Params::None,
	)
	.unwrap();

	info!("The first send - inserted counts what was enqueued, duplicates what was suppressed...");
	command(&db, "INSERT jobs::receipts [{ order_id: 100, amount: 4999 }]");

	info!("The producer never saw the reply and sends the same order again...");
	command(&db, "INSERT jobs::receipts [{ order_id: 100, amount: 4999 }]");

	info!("Only one receipt exists - the repeat was a no-op, not an error...");
	query(&db, "FROM jobs::receipts");

	info!("Deduplication is per key, so a different order still gets through...");
	command(&db, "INSERT jobs::receipts [{ order_id: 101, amount: 1250 }, { order_id: 100, amount: 4999 }]");

	info!("It also collapses repeats inside a single statement, not just across them...");
	command(&db, "INSERT jobs::receipts [{ order_id: 102, amount: 700 }, { order_id: 102, amount: 700 }]");

	info!("RETURNING on a suppressed row describes the job that survived, not the one rejected...");
	command(&db, "INSERT jobs::receipts [{ order_id: 100, amount: 9999 }] RETURNING { order_id, amount }");

	info!("The queue declaration is one way to key jobs; a producer can also key each statement.");
	db.admin_as_root("CREATE QUEUE jobs::alerts { text: utf8 } WITH { fifo: { partitions: 1 } }", Params::None)
		.unwrap();
	command(&db, r#"INSERT jobs::alerts [{ text: "disk full" }] WITH { deduplication_key: "disk-full-host-3" }"#);
	command(&db, r#"INSERT jobs::alerts [{ text: "disk full" }] WITH { deduplication_key: "disk-full-host-3" }"#);

	info!("One alert, however many times the monitor fired...");
	query(&db, "FROM jobs::alerts");
}
