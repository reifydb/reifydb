// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{thread, thread::sleep, time::Duration as StdDuration, time::Instant};

use reifydb::{IdentityId, Params, WithSubsystem, embedded};
use reifydb_examples::{command, log_query};
use reifydb_value::value::duration::Duration;
use tracing::info;

fn main() {
	let db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.build()
		.unwrap();

	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();
	db.admin_as_root("CREATE QUEUE jobs::tasks { name: utf8 } WITH { fifo: { partitions: 1 } }", Params::None)
		.unwrap();

	info!("A plain claim on an empty queue returns nothing at once - polling it in a loop burns cpu...");
	command(&db, r#"CALL queue::claim("worker-1", "jobs::tasks", 10, duration::seconds(30))"#);

	info!("claim_wait parks the worker instead, and an insert releases it...");
	let worker = db.session(IdentityId::root());

	thread::scope(|scope| {
		let waiting = scope.spawn(|| {
			log_query(r#"claim_wait("jobs::tasks", "worker-1", 10, 30s, wait_for: 5s)"#);
			let started = Instant::now();
			let result = worker.claim_wait(
				"jobs::tasks",
				"worker-1",
				10,
				Duration::from_seconds(30).unwrap(),
				Duration::from_seconds(5).unwrap(),
			);
			let waited = started.elapsed();
			if let Some(error) = result.error {
				panic!("claim_wait failed: {error:?}");
			}
			let claimed: usize = result.frames.iter().map(|frame| frame.row_count()).sum();
			info!("worker woke after {waited:?} holding {claimed} job(s)");
			for frame in &result.frames {
				info!("{}", frame);
			}
		});

		sleep(StdDuration::from_millis(500));
		info!("A producer enqueues while the worker is parked...");
		command(&db, r#"INSERT jobs::tasks [{ name: "resize avatar" }]"#);

		waiting.join().unwrap();
	});

	info!("The worker returned in about half a second, not after the full five second budget.");
	info!("A budget that runs out is a success with zero rows, not an error - the worker just asks again...");

	let started = Instant::now();
	let empty = worker.claim_wait(
		"jobs::tasks",
		"worker-1",
		10,
		Duration::from_seconds(30).unwrap(),
		Duration::from_milliseconds(300).unwrap(),
	);
	let rows: usize = empty.frames.iter().map(|frame| frame.row_count()).sum();
	info!("waited {:?}, claimed {} job(s), error {:?}", started.elapsed(), rows, empty.error.is_some());
}
