// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::thread;

use reifydb_core::fingerprint::StatementFingerprint;
use reifydb_metric::accumulator::StatementStatsAccumulator;
use reifydb_runtime::hash::Hash128;

fn fp(n: u128) -> StatementFingerprint {
	StatementFingerprint(Hash128(n))
}

#[test]
fn first_record_creates_entry() {
	let acc = StatementStatsAccumulator::new();
	acc.record(fp(1), "From test::users", 100, 50, 10, true);
	let snap = acc.snapshot();
	assert_eq!(snap.len(), 1);
	let (key, stats) = &snap[0];
	assert_eq!(*key, fp(1));
	assert_eq!(stats.normalized_rql(), "From test::users");
	assert_eq!(stats.calls(), 1);
}

#[test]
fn subsequent_records_reuse_entry() {
	let acc = StatementStatsAccumulator::new();
	acc.record(fp(1), "q", 100, 0, 0, true);
	acc.record(fp(1), "q", 200, 0, 0, true);
	acc.record(fp(1), "q", 50, 0, 0, true);
	let snap = acc.snapshot();
	assert_eq!(snap.len(), 1);
	assert_eq!(snap[0].1.calls(), 3);
	assert_eq!(snap[0].1.total_duration_us(), 350);
}

#[test]
fn distinct_fingerprints_create_separate_entries() {
	let acc = StatementStatsAccumulator::new();
	acc.record(fp(1), "q1", 100, 0, 0, true);
	acc.record(fp(2), "q2", 200, 0, 0, true);
	acc.record(fp(3), "q3", 300, 0, 0, true);
	let snap = acc.snapshot();
	assert_eq!(snap.len(), 3);
}

#[test]
fn concurrent_accumulation() {
	let acc = std::sync::Arc::new(StatementStatsAccumulator::new());
	let threads: usize = 4;
	let ops_per_thread: usize = 1000;

	let handles: Vec<_> = (0..threads)
		.map(|t| {
			let acc = std::sync::Arc::clone(&acc);
			thread::spawn(move || {
				for _ in 0..ops_per_thread {
					// All threads record against the same fingerprint
					acc.record(fp(42), "shared_query", 10, 5, 1, true);
					// Each thread also records against its own fingerprint
					acc.record(fp(100 + t as u128), "own_query", 10, 5, 1, true);
				}
			})
		})
		.collect();

	for h in handles {
		h.join().unwrap();
	}

	let snap = acc.snapshot();
	// 1 shared + 4 per-thread = 5 entries
	assert_eq!(snap.len(), 5);

	let shared = snap.iter().find(|(k, _)| *k == fp(42)).unwrap();
	assert_eq!(shared.1.calls(), (threads * ops_per_thread) as u64);

	for t in 0..threads {
		let own = snap.iter().find(|(k, _)| *k == fp(100 + t as u128)).unwrap();
		assert_eq!(own.1.calls(), ops_per_thread as u64);
	}
}
