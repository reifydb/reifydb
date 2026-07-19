// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The rolling window operator keeps its engine (running accumulators + group meta caches) on the
// operator instance across batches instead of rebuilding it cold on every apply. Each INSERT below
// is its own commit, so each is a separate batch through the flow: the totals after batch N+1 are
// only correct if the state carried over from batch N (warm cache or store) agrees exactly with
// what batch N committed. A double-merge (event applied to both the cached running accumulator and
// re-scanned from the store), a stale meta high_water (event silently dropped as sealed), or a
// missed eviction would all surface here as a wrong total or a wrong row count.

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

#[test]
fn rolling_sum_accumulates_correctly_across_separate_commits() {
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8, ts: datetime }");
	db.admin(r#"CREATE DEFERRED VIEW app::r { g: int4, total: float8 } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m", ts: "ts", state_cache_size: 8192, internal_state_cache_size: 8192 }
					by { g }
		}"#);

	let insert = |g: i32, v: f64, ts: &str| {
		db.command(&format!("INSERT app::t [{{ g: {g}, v: {v}, ts: \"{ts}\" }}]"));
	};
	let await_total = |g: i32, total: f64| {
		let rql = format!("FROM app::r | filter {{ g == {g} and total == {total} }}");
		let got = db.await_row_count(&rql, 1, StdDuration::from_secs(5));
		assert_eq!(
			got,
			1,
			"group {g} must roll up to {total} from state carried across commits; view now: {:?}",
			db.query_as_root("FROM app::r", ())
		);
	};

	// Batch 1 creates the group's running state from scratch.
	insert(1, 10.0, "2026-01-01T00:00:00Z");
	await_total(1, 10.0);

	// Batch 2 must fold into the state left behind by batch 1, not restart from zero
	// (missed carry-over) and not count batch 1 twice (double-merge on reload).
	insert(1, 5.0, "2026-01-01T00:01:00Z");
	await_total(1, 15.0);

	// An unrelated group gets its own accumulator without disturbing group 1.
	insert(2, 7.0, "2026-01-01T00:01:00Z");
	await_total(2, 7.0);
	await_total(1, 15.0);

	// A third batch for group 1 keeps compounding on the twice-carried state.
	insert(1, 3.0, "2026-01-01T00:02:00Z");
	await_total(1, 18.0);

	// One materialized row per group: updates must rewrite the group's row, not append.
	let rows = db.row_count("FROM app::r");
	assert_eq!(rows, 2, "rolling view must hold exactly one row per group, got {rows}");
}
