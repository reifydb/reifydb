// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{thread::sleep, time::Duration};

use reifydb::{Params, WithSubsystem, embedded};
use reifydb_examples::{command, query, utf8_column};
use tracing::info;

fn main() {
	let db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.build()
		.unwrap();

	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();

	info!("attempts is how many tries a job gets in total, backoff how long it waits between them...");
	db.admin_as_root(
		r#"
		CREATE QUEUE jobs::charges {
			order_id: int4
		} WITH { fifo: { partitions: 1 }, retry: { attempts: 2, backoff: "1s" } };
		"#,
		Params::None,
	)
	.unwrap();

	command(&db, "INSERT jobs::charges [{ order_id: 7 }]");

	info!("First try - the payment gateway is down, so the worker reports err rather than ok...");
	let first = command(&db, r#"CALL queue::claim("biller-1", "jobs::charges", 1, duration::seconds(30))"#);
	command(&db, &format!(r#"CALL queue::ack("{}", "err", "gateway timeout")"#, utf8_column(&first, "token")[0]));

	info!("The job is neither lost nor ready - it is serving its backoff, so a claim finds nothing...");
	command(&db, r#"CALL queue::claim("biller-1", "jobs::charges", 1, duration::seconds(30))"#);

	info!("Waiting out the one second backoff...");
	sleep(Duration::from_millis(1_200));

	info!("The job comes back on its own, as attempt 2 - note the attempt column...");
	let second = command(&db, r#"CALL queue::claim("biller-1", "jobs::charges", 1, duration::seconds(30))"#);

	info!("It fails again, and attempt 2 of 2 spends the last of the retry budget...");
	command(&db, &format!(r#"CALL queue::ack("{}", "err", "gateway timeout")"#, utf8_column(&second, "token")[0]));

	info!("With the budget gone the job is dead, not retried - it will never be claimed again...");
	sleep(Duration::from_millis(1_200));
	command(&db, r#"CALL queue::claim("biller-1", "jobs::charges", 1, duration::seconds(30))"#);

	info!("A dead job leaves both counters at zero - it is out of the working set, not deleted...");
	query(&db, r#"FROM system::queues FILTER { name == "charges" } MAP { name, depth, in_flight }"#);
}
