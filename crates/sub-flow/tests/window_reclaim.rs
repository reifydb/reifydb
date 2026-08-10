// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The window operator interns every (partition, window) pair as a substrate group, so an idle
//! window can be erased by the flow tick rather than retained forever. Every link in that chain
//! fails the same way - a healthy report over state that grows - hence driving it end to end.

use std::time::Duration as StdDuration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f)
			// The per-operator compaction counters are the only surface that reports what the tick pass
			// reclaimed; a short sample cadence keeps the polls inside their timeouts.
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

const RECLAIMED_A_GROUP: &str = "from system::metrics::runtime::operators::current filter { metric == 'state_compaction_dropped' and value > 0.0 }";

#[test]
fn a_deferred_flow_drains_a_window_group_left_behind_by_retraction() {
	drains_a_stranded_window_group("DEFERRED");
}

#[test]
fn a_rolling_partition_that_wakes_after_reclamation_publishes_one_row_not_two() {
	// A rolling group is coord-less - the group IS the partition - so it can go idle, be reclaimed,
	// and receive events again under the same key, which a tumbling window never does. A woken
	// group that mints a fresh row number while the old sink row survives duplicates the partition.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::r { g: int4, total: int8 } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1s", grace: "1s" }
					by { g }
		}"#);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::r FILTER { g == 1 }", 1, TIMEOUT);

	// A second partition carries the event watermark far past partition 1's horizon, so
	// partition 1 goes idle without being touched itself.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, v: 7, ts: "2026-01-01T00:05:00Z" }]"#);
	db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT);

	// Partition 1 wakes under the same key.
	db.command(r#"INSERT app::t [{ id: 3, g: 1, v: 9, ts: "2026-01-01T00:05:01Z" }]"#);

	// The flow must have applied that insert before a count means anything: awaiting a count of 1
	// returns on the first poll, since g == 1 already holds a row from the first insert.
	assert!(db.await_all_flows(TIMEOUT), "the flow must settle before the row count is evidence");

	assert_eq!(
		db.row_count("FROM app::r FILTER { g == 1 }"),
		1,
		"a woken partition must own exactly one row; view now: {:?}",
		db.query_as_root("FROM app::r", ())
	);
}

fn drains_a_stranded_window_group(view_kind: &str) {
	// A window emptied by retraction drops its expiry index entry, so nothing in the operator ever
	// revisits the empty accumulator row it left behind. Reclamation is the only mechanism that
	// can reach that row, which makes it the sharpest probe that a due group really is drained.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(&format!(r#"CREATE {view_kind} VIEW app::w {{ g: int4, total: int8 }} AS {{
			FROM app::t
				| window tumbling {{ total: math::sum(v) }}
					with {{ interval: "1s", grace: "1s" }}
					by {{ g }}
		}}"#));

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::w", 1, TIMEOUT);

	// Emptying the window is what strands its accumulator: the operator withdraws the published row
	// and un-indexes the window, so no later expiry pass will revisit it.
	db.command("DELETE app::t FILTER { id == 1 }");
	db.await_exact_row_count("FROM app::w", 0, TIMEOUT);

	// A second window, emptied like the first, to arm a seal timer one window later than the
	// stranded group's own. Reclamation is bounded by the seal ledger, and the first timer alone
	// leaves the frontier inside the stranded group's own bucket, so nothing would be reclaimable.
	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 5, ts: "2026-01-01T00:00:01Z" }]"#);
	db.await_row_count("FROM app::w", 1, TIMEOUT);
	db.command("DELETE app::t FILTER { id == 2 }");
	db.await_exact_row_count("FROM app::w", 0, TIMEOUT);

	// Push the event watermark well past both windows' horizon (interval + grace = 2s), which fires
	// their seal timers and leaves the stranded groups unambiguously idle while the new one is not.
	db.command(r#"INSERT app::t [{ id: 3, g: 1, v: 7, ts: "2026-01-01T00:05:00Z" }]"#);
	db.await_row_count("FROM app::w", 1, TIMEOUT);

	let reclaimed = db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT);
	assert_eq!(
		reclaimed,
		1,
		"no window group was ever reclaimed: the operator's state is either still addressed under the root \
		 group (nothing inside a group range to erase), stamped in a domain the seal cutoff cannot read, or the \
		 node was skipped for want of the Reclaim capability; ledger now: {:?}",
		db.query_as_root(
			"from system::metrics::runtime::operators::current filter { metric == 'state_compaction_dropped' }",
			()
		)
	);

	// Reclamation must take only the idle group. The live window still owns its state and must keep
	// accumulating onto it, or the pass has erased something a batch was still using.
	db.command(r#"INSERT app::t [{ id: 4, g: 1, v: 4, ts: "2026-01-01T00:05:00.500Z" }]"#);
	let rows = db.await_row_count("FROM app::w FILTER { total == 11 }", 1, TIMEOUT);
	assert_eq!(
		rows,
		1,
		"the surviving window lost its accumulator: reclamation reached a group that was still live; view \
		 now: {:?}",
		db.query_as_root("FROM app::w", ())
	);
}
