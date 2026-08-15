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
	db.admin_as_root("CREATE QUEUE jobs::reminders { note: utf8 } WITH { fifo: { partitions: 1 } }", Params::None)
		.unwrap();

	info!("An ordinary insert is due immediately...");
	command(&db, r#"INSERT jobs::reminders [{ note: "send now" }]"#);

	info!("not_before holds a job back until a wall-clock instant, without a scheduler process...");
	command(
		&db,
		r#"
		INSERT jobs::reminders [{ note: "send in two seconds" }]
		WITH { not_before: datetime::add(datetime::now(), duration::seconds(2)) }
		"#,
	);

	info!("Both jobs are waiting, but oldest_due_at shows only one of them is claimable...");
	query(&db, r#"FROM system::queues FILTER { name == "reminders" } MAP { name, depth, oldest_due_at }"#);

	info!("A worker asking for both gets only the one that is due...");
	let now = command(&db, r#"CALL queue::claim("worker-1", "jobs::reminders", 10, duration::seconds(30))"#);
	command(&db, &format!(r#"CALL queue::ack("{}", "ok", none)"#, utf8_column(&now, "token")[0]));

	info!("Waiting for the delayed job to come due...");
	sleep(Duration::from_millis(2_200));

	info!("The same claim now finds it - nothing woke it, the claim simply reads the due index...");
	let later = command(&db, r#"CALL queue::claim("worker-1", "jobs::reminders", 10, duration::seconds(30))"#);
	command(&db, &format!(r#"CALL queue::ack("{}", "ok", none)"#, utf8_column(&later, "token")[0]));
}
