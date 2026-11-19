// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread;

use reifydb_core::common::CommitVersion;
use reifydb_transaction::multi::done_until::done_until::DoneUntil;
use reifydb_value::value::duration::Duration;

#[test]
fn done_until_reaches_max_under_concurrent_burst() {
	// commit log, subscriptions and GC all block on done_until, so one lost mark_finished stalls the
	// done-until mark at v-1 and wedges every downstream consumer permanently.
	// The threads cover disjoint contiguous slices whose union is 1..=40000, so the live window
	// runs far wider than the done-until ring's initial capacity: the ring has to grow rather than
	// alias two versions onto one slot and merge their refcounts.
	let done_until = DoneUntil::new("done-until-burst".into());

	const THREADS: u64 = 8;
	const PER_THREAD: u64 = 5000;
	let total = THREADS * PER_THREAD;

	thread::scope(|scope| {
		for t in 0..THREADS {
			let done_until = &done_until;
			scope.spawn(move || {
				let base = t * PER_THREAD + 1;
				for v in base..base + PER_THREAD {
					done_until.register_in_flight(CommitVersion(v));
					done_until.mark_finished(CommitVersion(v));
				}
			});
		}
	});

	let reached = done_until.wait_for_mark_timeout(CommitVersion(total), Duration::from_seconds(10).unwrap());
	assert!(
		reached,
		"done_until stalled at {} of {} - a Begin/Done was lost under burst load",
		done_until.done_until().0,
		total
	);
}
