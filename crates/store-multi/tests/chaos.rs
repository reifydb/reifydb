// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

//! Model-based chaos for store-multi: a seeded op sequence runs against a full-history oracle and three
//! store configurations, and every read is differentially checked. Flush collapses MVCC history below its
//! cutoff, so the oracle is only queried at versions >= the high watermark W (the max flush cutoff so far).

#[path = "chaos/concurrency.rs"]
mod concurrency;
#[path = "chaos/fixtures.rs"]
mod fixtures;
#[path = "chaos/lifecycle.rs"]
mod lifecycle;
#[path = "chaos/multiobject.rs"]
mod multiobject;
#[path = "chaos/oracle.rs"]
mod oracle;
#[path = "chaos/snapshot.rs"]
mod snapshot;
#[path = "chaos/workload.rs"]
mod workload;

use reifydb_core::interface::catalog::{id::TableId, storage::StorageId};
use reifydb_testing_macro::chaos_test;

use crate::workload::{Params, drive};

pub const STORAGE: StorageId = StorageId::Table(TableId(1));

chaos_test!(multi_store_chaos, |seed| {
	// The keyspace is wide enough to span many cache pages, so reads exercise warming and eviction.
	drive(
		seed,
		Params {
			keyspace: 96,
			min_steps: 60,
			max_steps: 160,
			commit_pct: 45,
			flush_pct: 20,
			remove_pct: 25,
			max_deltas: 6,
			max_batch: 40,
		},
	);
});

chaos_test!(multi_store_flush_heavy_chaos, |seed| {
	// Frequent partial flushes over a small keyspace leave a sparse commit buffer over a dense
	// persistent tier across batch boundaries, which is where the cold-merge horizon bites.
	drive(
		seed,
		Params {
			keyspace: 64,
			min_steps: 80,
			max_steps: 200,
			commit_pct: 40,
			flush_pct: 40,
			remove_pct: 30,
			max_deltas: 5,
			max_batch: 24,
		},
	);
});

chaos_test!(multi_store_lifecycle_chaos, |seed| {
	// Tombstones, flushes, TTL sweeps, physical deletes and historical GC interleaved: no ghost, no
	// premature loss, and cross-config agreement at the current version.
	lifecycle::drive(
		seed,
		lifecycle::Params {
			keyspace: 600,
			min_steps: 120,
			max_steps: 240,
			commit_pct: 40,
			flush_pct: 16,
			ttl_pct: 12,
			delete_pct: 9,
			histgc_pct: 8,
			remove_pct: 22,
			max_deltas: 14,
			max_batch: 32,
		},
	);
});

chaos_test!(multi_object_isolation_chaos, |seed| {
	// A sweep or delete scoped to one object must leave the others byte-for-byte intact; catches
	// cross-table bleed in object-scoped scan, delete and TTL bounds.
	multiobject::drive(
		seed,
		multiobject::Params {
			keyspace: 240,
			min_steps: 120,
			max_steps: 240,
			commit_pct: 40,
			flush_pct: 16,
			ttl_pct: 14,
			delete_pct: 10,
			remove_pct: 22,
			max_deltas: 12,
			max_batch: 32,
		},
	);
});

chaos_test!(multi_store_snapshot_chaos, |seed| {
	// A paginated AsOf{V} scan drained one item at a time must hold the exact snapshot even while
	// commits above V and flushes at or below V land between batch pulls.
	snapshot::drive(
		seed,
		snapshot::Params {
			keyspace: 220,
			seed_commits: 60,
			max_deltas: 12,
			remove_pct: 22,
			interleave_pct: 70,
			commit_vs_flush_pct: 60,
		},
	);
});

chaos_test!(multi_store_random_chaos, |seed| {
	// The sweeps above pin fixed configurations; this one draws its parameters from the seed. A failure
	// reports the RESOLVED parameters, which are what a regression pins - the master seed stops meaning
	// the same thing the moment the parameter generator changes.
	workload::drive_random(seed);
});

#[test]
fn multi_store_concurrency_stress() {
	// A fixed seed keeps each thread's INTENDED op stream reproducible; thread scheduling is not. The run
	// loop varies CONC_SEED across invocations for broader coverage; on failure the seed is in the message.
	let seed = std::env::var("CONC_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0xC0FFEE);
	concurrency::run(seed, concurrency::Config::default());
}
