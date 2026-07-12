// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A ring-buffer-backed view announces capacity evictions downstream as remove diffs, so a view
// derived FROM the ring buffer (the canonical case: a keyed aggregate) retracts the evicted rows'
// contribution instead of accumulating one group per key forever. The keyed aggregate operator has
// no state eviction of its own and row TTL GC purges storage below the flow's CDC, so ring-buffer
// eviction propagation is the ONLY mechanism that bounds such a chain. Propagation is silenced only
// when the ring buffer's row TTL is explicitly configured with `mode: drop` (the same "no diff"
// semantic TTL GC already applies for that mode) - no TTL at all, or `mode: delete`, still propagates.
// These tests observe the chain end to end through queries on the downstream aggregate, covering the
// two distinct eviction code paths separately: the global head-counter path (non-partitioned) and
// the per-partition marker path.

use std::{
	thread,
	time::{Duration as StdDuration, Instant},
};

use reifydb::{Database, Params, WithSubsystem, embedded};

fn setup() -> Database {
	embedded::memory().with_flow(|c| c).build().expect("build memory db with flow")
}

fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"));
}

fn command(db: &Database, rql: &str) {
	db.command_as_root(rql, Params::None).unwrap_or_else(|e| panic!("command failed: {e:?}\nrql: {rql}"));
}

fn err_code(db: &Database, rql: &str) -> String {
	match db.command_as_root(rql, Params::None) {
		Ok(_) => panic!("expected command to fail, but it succeeded\nrql: {rql}"),
		Err(e) => e.diagnostic().code.clone(),
	}
}

// The aggregate row for one region: (count, sum). None when the group has no row at all - after a
// full retraction the group must DISAPPEAR, which is observably different from a lingering zero row.
fn agg_group(db: &Database, region: &str) -> Option<(i64, i32)> {
	let rql = format!("FROM test::agg FILTER region == \"{region}\"");
	let frames = db.query_as_root(&rql, Params::None).unwrap_or_else(|e| panic!("query failed: {e:?}\nrql: {rql}"));
	for f in &frames {
		if f.row_count() > 0 {
			let c = f.get::<i64>("c", 0).expect("get c").expect("c defined");
			let s = f.get::<i32>("s", 0).expect("get s").expect("s defined");
			return Some((c, s));
		}
	}
	None
}

fn await_agg_group(db: &Database, region: &str, want: Option<(i64, i32)>) -> Option<(i64, i32)> {
	let deadline = Instant::now() + StdDuration::from_secs(5);
	loop {
		let got = agg_group(db, region);
		if got == want || Instant::now() >= deadline {
			return got;
		}
		thread::sleep(StdDuration::from_millis(20));
	}
}

fn create_events_table(db: &Database) {
	admin(db, "CREATE NAMESPACE test");
	admin(db, "CREATE TABLE test::events { region: utf8, n: int4 }");
}

fn create_agg_over_rb(db: &Database) {
	admin(
		db,
		"CREATE DEFERRED VIEW test::agg { region: utf8, c: int8, s: int4 } \
		 AS { FROM test::rb AGGREGATE { c: math::count(region), s: math::sum(n) } BY { region } }",
	);
}

// Global (non-partitioned) eviction path: once every row of a group has been evicted from the ring
// buffer, the group's aggregate row must be retracted entirely. Rows are inserted one statement at a
// time so each eviction removes a PRIOR batch's row, exercising the storage read-back path.
#[test]
fn global_eviction_retracts_the_downstream_aggregate() {
	let db = setup();
	create_events_table(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2 } AS { FROM test::events }",
	);
	create_agg_over_rb(&db);

	command(&db, "INSERT test::events [{ region: \"us\", n: 1 }]");
	command(&db, "INSERT test::events [{ region: \"us\", n: 2 }]");
	assert_eq!(
		await_agg_group(&db, "us", Some((2, 3))),
		Some((2, 3)),
		"both us rows fit the buffer, so the aggregate sees both"
	);

	command(&db, "INSERT test::events [{ region: \"eu\", n: 3 }]");
	command(&db, "INSERT test::events [{ region: \"eu\", n: 4 }]");

	assert_eq!(
		await_agg_group(&db, "us", None),
		None,
		"every us row was evicted, so the us group must be retracted, not left stale or zeroed"
	);
	assert_eq!(
		await_agg_group(&db, "eu", Some((2, 7))),
		Some((2, 7)),
		"the surviving eu rows must aggregate normally"
	);
}

// Per-partition eviction path: a busy partition evicting its own oldest rows must retract exactly
// those rows' contribution downstream, leaving a quiet partition's aggregate untouched.
#[test]
fn partitioned_eviction_retracts_only_that_partitions_contribution() {
	let db = setup();
	create_events_table(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } \
		 AS { FROM test::events }",
	);
	create_agg_over_rb(&db);

	command(&db, "INSERT test::events [{ region: \"us\", n: 1 }]");
	command(&db, "INSERT test::events [{ region: \"us\", n: 2 }]");
	command(&db, "INSERT test::events [{ region: \"eu\", n: 3 }]");
	assert_eq!(await_agg_group(&db, "us", Some((2, 3))), Some((2, 3)), "us starts with both rows");

	command(&db, "INSERT test::events [{ region: \"us\", n: 4 }]");
	command(&db, "INSERT test::events [{ region: \"us\", n: 5 }]");

	assert_eq!(
		await_agg_group(&db, "us", Some((2, 9))),
		Some((2, 9)),
		"us evicted n=1 and n=2, so its aggregate must reflect only n=4 and n=5"
	);
	assert_eq!(
		await_agg_group(&db, "eu", Some((1, 3))),
		Some((1, 3)),
		"eu never evicted, so its aggregate must be untouched by us's evictions"
	);
}

// TTL cleanup_mode: drop on the global path - with the ring buffer's row TTL explicitly configured to
// drop silently, capacity eviction still removes its own stored rows, but nothing is announced
// downstream, so the aggregate keeps the evicted contribution. This pins that silencing eviction
// propagation requires an explicit `mode: drop`, not merely an absent TTL.
#[test]
fn global_ttl_drop_keeps_the_stale_downstream_aggregate() {
	let db = setup();
	create_events_table(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, row: { ttl: { duration: '1h', mode: drop } } } AS { FROM test::events }",
	);
	create_agg_over_rb(&db);

	command(&db, "INSERT test::events [{ region: \"us\", n: 1 }]");
	command(&db, "INSERT test::events [{ region: \"us\", n: 2 }]");
	assert_eq!(await_agg_group(&db, "us", Some((2, 3))), Some((2, 3)), "us starts with both rows");

	command(&db, "INSERT test::events [{ region: \"eu\", n: 3 }]");
	command(&db, "INSERT test::events [{ region: \"eu\", n: 4 }]");
	assert_eq!(await_agg_group(&db, "eu", Some((2, 7))), Some((2, 7)), "eu inserts still flow downstream");

	assert_eq!(
		agg_group(&db, "us"),
		Some((2, 3)),
		"with cleanup_mode: drop the evicted us rows must remain in the aggregate, stale by design"
	);
}

// TTL cleanup_mode: drop on the per-partition path - evictions in a busy partition accumulate
// downstream instead of retracting.
#[test]
fn partitioned_ttl_drop_keeps_the_stale_downstream_aggregate() {
	let db = setup();
	create_events_table(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, row: { ttl: { duration: '1h', mode: drop } }, partition: { by: { region } } } \
		 AS { FROM test::events }",
	);
	create_agg_over_rb(&db);

	command(&db, "INSERT test::events [{ region: \"us\", n: 1 }]");
	command(&db, "INSERT test::events [{ region: \"us\", n: 2 }]");
	command(&db, "INSERT test::events [{ region: \"us\", n: 4 }]");
	command(&db, "INSERT test::events [{ region: \"us\", n: 5 }]");

	assert_eq!(
		await_agg_group(&db, "us", Some((4, 12))),
		Some((4, 12)),
		"with cleanup_mode: drop every insert accumulates; evictions of n=1 and n=2 are never retracted"
	);
}

// TTL present but with cleanup_mode: delete (not drop) - eviction must still propagate. Pins that
// silencing propagation requires `mode: drop` specifically, not merely the presence of a TTL.
#[test]
fn global_ttl_delete_mode_still_propagates() {
	let db = setup();
	create_events_table(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, row: { ttl: { duration: '1h', mode: delete } } } AS { FROM test::events }",
	);
	create_agg_over_rb(&db);

	command(&db, "INSERT test::events [{ region: \"us\", n: 1 }]");
	command(&db, "INSERT test::events [{ region: \"us\", n: 2 }]");
	assert_eq!(
		await_agg_group(&db, "us", Some((2, 3))),
		Some((2, 3)),
		"both us rows fit the buffer, so the aggregate sees both"
	);

	command(&db, "INSERT test::events [{ region: \"eu\", n: 3 }]");
	command(&db, "INSERT test::events [{ region: \"eu\", n: 4 }]");

	assert_eq!(
		await_agg_group(&db, "us", None),
		None,
		"cleanup_mode: delete is not drop, so every evicted us row must still be retracted"
	);
}

// Within-batch overflow on the global path: one insert batch larger than capacity evicts rows that
// were assigned earlier in the SAME batch and never stored. The insert diff carries the full batch,
// so the eviction remove (emitted after it) must net those rows out - the aggregate ends at exactly
// the surviving rows. The ring buffer here has no row TTL configured at all, pinning that the default
// (no cleanup_mode: drop) is propagate-on.
#[test]
fn global_within_batch_overflow_nets_to_capacity() {
	let db = setup();
	create_events_table(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2 } AS { FROM test::events }",
	);
	create_agg_over_rb(&db);

	command(
		&db,
		"INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }, { region: \"us\", n: 3 }]",
	);

	assert_eq!(
		await_agg_group(&db, "us", Some((2, 5))),
		Some((2, 5)),
		"n=1 was inserted and evicted within one batch; downstream must net to the surviving n=2 and n=3"
	);
}

// Within-batch overflow on the per-partition path, with a quiet partition in the same batch as a
// control. Also runs with no row TTL configured, pinning the default propagate-on behavior.
#[test]
fn partitioned_within_batch_overflow_nets_to_capacity() {
	let db = setup();
	create_events_table(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);
	create_agg_over_rb(&db);

	command(
		&db,
		"INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }, \
		 { region: \"us\", n: 3 }, { region: \"eu\", n: 4 }]",
	);

	assert_eq!(
		await_agg_group(&db, "us", Some((2, 5))),
		Some((2, 5)),
		"us overflowed within the batch and must net to its surviving n=2 and n=3"
	);
	assert_eq!(
		await_agg_group(&db, "eu", Some((1, 4))),
		Some((1, 4)),
		"eu stayed under capacity and must be unaffected by us's within-batch eviction"
	);
}

// An UPDATE that would move a row across partitions is rejected outright at compile time, so it
// never reaches the ring buffer's eviction path at all: neither partition's membership, nor the
// downstream aggregate derived from them, changes.
#[test]
fn update_driven_partition_move_is_rejected_and_downstream_is_unaffected() {
	let db = setup();
	create_events_table(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);
	create_agg_over_rb(&db);

	command(
		&db,
		"INSERT test::events [{ region: \"eu\", n: 10 }, { region: \"eu\", n: 20 }, \
		 { region: \"us\", n: 1 }, { region: \"us\", n: 2 }]",
	);
	assert_eq!(await_agg_group(&db, "eu", Some((2, 30))), Some((2, 30)), "eu starts with both its rows");
	assert_eq!(await_agg_group(&db, "us", Some((2, 3))), Some((2, 3)), "us starts with both its rows");

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"a cross-partition move must be rejected"
	);

	assert_eq!(
		await_agg_group(&db, "eu", Some((2, 30))),
		Some((2, 30)),
		"eu must be unaffected by the rejected update"
	);
	assert_eq!(
		await_agg_group(&db, "us", Some((2, 3))),
		Some((2, 3)),
		"us must be unaffected by the rejected update"
	);
}
