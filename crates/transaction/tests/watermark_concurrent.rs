// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread;

use reifydb_core::common::CommitVersion;
use reifydb_transaction::multi::watermark::watermark::WaterMark;
use reifydb_value::value::duration::Duration;

// The MVCC oracle's `done_until` watermark is the gate the CDC consumer waits on before it may
// process a version (deferred-view maintenance, subscriptions, GC all block on it). It is fed via
// `register_in_flight` (Begin) / `mark_finished` (Done) and advances to the highest version V where
// every version <= V has been both begun and finished. These updates are correctness-critical: drop
// a single `Done(v)` and v stays in-flight forever, freezing done_until at v-1 - which permanently
// wedges every downstream consumer.
//
// The watermark is a `Mutex`-guarded contiguous-frontier structure updated inline by the calling
// (commit/query) threads. This test drives a concurrent burst directly: 8 threads each begin+finish
// a disjoint contiguous slice so the union is versions 1..=40000 (kept under MAX_PENDING = 100000 so
// the pending-cleanup path is never involved). Because every version is begun and finished, a correct
// watermark MUST reach done_until == 40000 - proving no Begin/Done is lost and the frontier advances
// correctly under multi-producer contention.
#[test]
fn done_until_reaches_max_under_concurrent_burst() {
	let watermark = WaterMark::new("watermark-burst".into());

	const THREADS: u64 = 8;
	const PER_THREAD: u64 = 5000;
	let total = THREADS * PER_THREAD;

	thread::scope(|scope| {
		for t in 0..THREADS {
			let watermark = &watermark;
			scope.spawn(move || {
				let base = t * PER_THREAD + 1;
				for v in base..base + PER_THREAD {
					watermark.register_in_flight(CommitVersion(v));
					watermark.mark_finished(CommitVersion(v));
				}
			});
		}
	});

	let reached = watermark.wait_for_mark_timeout(CommitVersion(total), Duration::from_seconds(10).unwrap());
	assert!(
		reached,
		"done_until stalled at {} of {} - a Begin/Done was lost under burst load",
		watermark.done_until().0,
		total
	);
}
