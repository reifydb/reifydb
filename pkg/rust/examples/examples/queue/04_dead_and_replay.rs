// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, WithSubsystem, embedded};
use reifydb_examples::{command, query, uint8_column, utf8_column};
use tracing::info;

fn main() {
	let db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.build()
		.unwrap();

	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();
	db.admin_as_root("CREATE QUEUE jobs::imports { file: utf8 } WITH { fifo: { partitions: 1 } }", Params::None)
		.unwrap();

	command(&db, r#"INSERT jobs::imports [{ file: "customers.csv" }]"#);

	let claimed = command(&db, r#"CALL queue::claim("importer-1", "jobs::imports", 1, duration::seconds(30))"#);
	let token = utf8_column(&claimed, "token").remove(0);
	let item = uint8_column(&claimed, "item").remove(0);

	info!("The file is malformed - retrying will not help, so the worker buries it with dead...");
	info!("dead spends the whole retry budget at once, err would have spent one attempt of it.");
	command(&db, &format!(r#"CALL queue::ack("{token}", "dead", "column count mismatch on line 4")"#));

	info!("A dead job is invisible to workers - a claim over the whole queue returns nothing...");
	command(&db, r#"CALL queue::claim("importer-1", "jobs::imports", 10, duration::seconds(30))"#);

	info!("Dead is a parking spot, not a grave - the payload is still there to inspect...");
	query(&db, "FROM jobs::imports");

	info!("Once the file is fixed an operator puts the job back by its item number...");
	command(&db, &format!(r#"CALL queue::replay("jobs::imports", {item})"#));

	info!("It is ready again with a fresh retry budget, and this time it succeeds...");
	let replayed = command(&db, r#"CALL queue::claim("importer-1", "jobs::imports", 1, duration::seconds(30))"#);
	command(&db, &format!(r#"CALL queue::ack("{}", "ok", none)"#, utf8_column(&replayed, "token")[0]));
}
