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

	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();

	info!("ordered_by names the column that must be processed one job at a time, in order...");
	db.admin_as_root(
		r#"
		CREATE QUEUE jobs::updates {
			account: utf8,
			change: utf8
		} WITH { fifo: { partitions: 1, ordered_by: account } };
		"#,
		Params::None,
	)
	.unwrap();

	info!("Three changes for account a, one for account b - order matters within an account...");
	command(
		&db,
		r#"
		INSERT jobs::updates [
			{ account: "a", change: "set name" },
			{ account: "a", change: "set email" },
			{ account: "a", change: "close" },
			{ account: "b", change: "set name" }
		]
		"#,
	);

	info!("blocked_keys counts accounts with work parked behind an earlier job - here, just a...");
	query(&db, r#"FROM system::queues FILTER { name == "updates" } MAP { name, depth, blocked_keys }"#);

	info!("A worker asks for four jobs but can only get two - one per account...");
	let claimed = command(&db, r#"CALL queue::claim("worker-1", "jobs::updates", 4, duration::seconds(30))"#);

	info!("Without ordered_by all four would have been handed out and could run out of order.");
	info!("Acking a's first change releases only a's next change...");
	let accounts = utf8_column(&claimed, "account");
	let tokens = utf8_column(&claimed, "token");
	let for_a = accounts.iter().position(|account| account == "a").expect("account a must be claimable");
	command(&db, &format!(r#"CALL queue::ack("{}")"#, tokens[for_a]));

	info!("The second change for a is now claimable, the third still is not...");
	command(&db, r#"CALL queue::claim("worker-1", "jobs::updates", 4, duration::seconds(30))"#);
}
