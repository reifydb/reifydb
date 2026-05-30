// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

//! Model-based chaos test for store-multi.
//!
//! A seeded operation sequence (commit / flush / get / get_many / range / range_rev) is applied both to
//! an in-memory full-history oracle ([`oracle`]) and to three `StandardMultiStore` configurations
//! ([`fixtures`]), and the [`workload`] runner differentially checks every read against the oracle. The
//! configurations:
//!   - `memory`     - commit buffer only (no persistent tier, no read cache).
//!   - `persistent` - commit buffer + SQLite persistent + read cache at the default page size.
//!   - `tiny_cache` - same, with a deliberately small read cache (seed-chosen) so a keyspace that spans many pages
//!     forces warming and mid-scan eviction.
//!
//! Determinism / replay: all stores use sync_only pools, so the timer-driven flush/drop actors never
//! fire on their own; every commit-to-persistent movement runs through the synchronous flush stand-in.
//! A run is therefore a pure function of the seed and any failure replays via `CHAOS_SEED` (the shared
//! chaos runner prints `reproduce: make test-chaos SEED=.. N=..`).
//!
//! Soundness: flush collapses MVCC history below its cutoff (the SQLite persistent tier is current-only),
//! so the oracle is only queried at versions >= the high watermark `W` = max flush cutoff so far. For any
//! `read >= W` the latest-version-<=read is always retained - either still in the commit buffer (versions
//! > W are never flushed) or as the persisted base (latest-<=W) - so the full-history oracle stays exact.
//! This mirrors the real contract that a reader never reads below the eviction watermark.

#[path = "chaos/concurrency.rs"]
mod concurrency;
#[path = "chaos/fixtures.rs"]
mod fixtures;
#[path = "chaos/lifecycle.rs"]
mod lifecycle;
#[path = "chaos/multishape.rs"]
mod multishape;
#[path = "chaos/operator.rs"]
mod operator;
#[path = "chaos/oracle.rs"]
mod oracle;
#[path = "chaos/snapshot.rs"]
mod snapshot;
#[path = "chaos/workload.rs"]
mod workload;

use reifydb_core::interface::catalog::{id::TableId, shape::ShapeId};
use reifydb_testing::chaos_test;

use crate::workload::{Params, drive};

pub const SHAPE: ShapeId = ShapeId::Table(TableId(1));

// Broad mixed workload: commits dominate, partial flushes, and reads spread across get/get_many/range
// (forward + reverse, AsOf + Between) over a keyspace that spans many cache pages.
chaos_test!(multi_store_chaos, |seed| {
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

// Flush-heavy variant: frequent partial flushes over a smaller keyspace push the commit buffer into the
// sparse-over-dense-persistent shape across batch boundaries - the family the cold-merge-horizon defect
// lived in.
chaos_test!(multi_store_flush_heavy_chaos, |seed| {
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

// Delete / physical-removal / row-TTL / historical-GC lifecycle: interleaves tombstones, flushes, TTL
// sweeps (header-timestamp driven), direct physical deletes, and historical-version GC; asserts no ghost
// / no premature loss / cross-config agreement at the current version.
chaos_test!(multi_store_lifecycle_chaos, |seed| {
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
			max_time_step: 400,
			max_ttl: 400,
		},
	);
});

// Operator-state (FlowNodeState) lifecycle: single-version get/range, Delta::Drop (sync
// evict_operator_state), flush, and operator TTL; differential across memory vs commit+persistent.
chaos_test!(operator_state_lifecycle_chaos, |seed| {
	operator::drive(
		seed,
		operator::Params {
			keyspace: 48,
			min_steps: 80,
			max_steps: 200,
			commit_pct: 40,
			flush_pct: 15,
			ttl_pct: 15,
			drop_pct: 12,
			max_deltas: 5,
			max_batch: 24,
			max_time_step: 400,
			max_ttl: 400,
		},
	);
});

// Multi-shape isolation: commit / flush / row-TTL / physical-delete spread across several tables; a sweep
// or delete scoped to one shape must leave the others byte-for-byte intact, and a shape's full-scan must
// return exactly that shape's rows. Catches cross-table bleed in shape-scoped scan/delete/TTL bounds.
chaos_test!(multi_shape_isolation_chaos, |seed| {
	multishape::drive(
		seed,
		multishape::Params {
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
			max_time_step: 400,
			max_ttl: 400,
		},
	);
});

// Mid-scan snapshot stability: a paginated AsOf{V} scan, drained one item at a time, must return the exact
// snapshot as-of V even when commits (version > V) and bounded flushes (cutoff <= V) are interleaved between
// batch pulls. Targets the merge/cursor/horizon under live tier migration mid-scan.
chaos_test!(multi_store_snapshot_chaos, |seed| {
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

// Multi-threaded concurrency stress (NON-deterministic; #[ignore]d so it never runs in the deterministic
// suite). Real threads + background flush/drop actors under default pools; disjoint key ownership makes the
// final per-key state deterministic while readers assert structural invariants under live churn. Run on
// demand: `cargo test -p reifydb-store-multi --features chaos -- --ignored` (or `make test-chaos-concurrency`).
#[test]
#[ignore = "non-deterministic multi-threaded stress; run explicitly with --ignored"]
fn multi_store_concurrency_stress() {
	// A fixed seed keeps each thread's INTENDED op stream reproducible; thread scheduling is not. The run
	// loop varies CONC_SEED across invocations for broader coverage; on failure the seed is in the message.
	let seed = std::env::var("CONC_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0xC0FFEE);
	concurrency::run(seed, concurrency::Config::default());
}
