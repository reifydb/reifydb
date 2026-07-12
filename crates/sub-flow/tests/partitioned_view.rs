// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A view can declare `partition by <cols>` so its materialized rows are physically partitioned in the
// underlying storage primitive (table / ring buffer / series), reusing the base-primitive partition
// stack (PartitionedRowKey + Partition::of + partition pruning). These tests drive the deferred flow
// engine end to end: they only observe the view through queries, so a broken write path (rows landing
// under the wrong keyspace) or a broken read path (scan not reading the partitioned keyspace) surfaces
// as a wrong row count - a non-partitioned scan of a partitioned view returns zero rows.

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

// Runs a command expected to fail at compile time (before any row is touched) and returns its
// diagnostic code, so callers can assert on PART_002 (own-partition-column, runtime, value-based)
// vs PART_004 (downstream-view partition-column, compile-time, column-identity-based) precisely.
fn err_code(db: &Database, rql: &str) -> String {
	match db.command_as_root(rql, Params::None) {
		Ok(_) => panic!("expected command to fail, but it succeeded\nrql: {rql}"),
		Err(e) => e.diagnostic().code.clone(),
	}
}

fn row_count(db: &Database, rql: &str) -> usize {
	let frames = db.query_as_root(rql, Params::None).unwrap_or_else(|e| panic!("query failed: {e:?}\nrql: {rql}"));
	frames.iter().map(|f| f.row_count()).sum()
}

fn await_row_count(db: &Database, rql: &str, want: usize) -> usize {
	let deadline = Instant::now() + StdDuration::from_secs(5);
	loop {
		let got = row_count(db, rql);
		if got >= want || Instant::now() >= deadline {
			return got;
		}
		thread::sleep(StdDuration::from_millis(20));
	}
}

// `await_row_count` waits for a count to reach *at least* `want`, which is a no-op wait for a count
// that is expected to DECREASE (e.g. after a delete) - it returns instantly since the still-stale
// higher count already satisfies `>= want`. Use this instead when waiting for a decrease.
fn await_exact_row_count(db: &Database, rql: &str, want: usize) -> usize {
	let deadline = Instant::now() + StdDuration::from_secs(5);
	loop {
		let got = row_count(db, rql);
		if got == want || Instant::now() >= deadline {
			return got;
		}
		thread::sleep(StdDuration::from_millis(20));
	}
}

fn collect_n(db: &Database, rql: &str) -> Vec<i32> {
	let frames = db.query_as_root(rql, Params::None).unwrap_or_else(|e| panic!("query failed: {e:?}\nrql: {rql}"));
	let mut out = Vec::new();
	for f in &frames {
		for r in 0..f.row_count() {
			out.push(f.get::<i32>("n", r).expect("get n").expect("n defined"));
		}
	}
	out
}

fn seed_events(db: &Database) {
	admin(db, "CREATE NAMESPACE test");
	admin(db, "CREATE TABLE test::events { region: utf8, n: int4 }");
	command(
		db,
		"INSERT test::events [{ region: \"us\", n: 1 }, { region: \"eu\", n: 2 }, { region: \"us\", n: 3 }]",
	);
}

// The materialized rows must be stored under the underlying table's PartitionedRow keyspace and read
// back through it. If the sink wrote plain Row keys (or the scan read the Row keyspace) the full scan
// would see zero rows; if partition pruning hashed inconsistently with the write, the FILTER subset
// would be wrong.
#[test]
fn table_backed_partitioned_view_stores_and_prunes() {
	let db = setup();
	seed_events(&db);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::by_region { region: utf8, n: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM test::events }",
	);

	assert_eq!(
		await_row_count(&db, "FROM test::by_region", 3),
		3,
		"all rows must materialize in the partitioned keyspace"
	);

	let us = collect_n(&db, "FROM test::by_region FILTER region == \"us\"");
	let mut us_sorted = us.clone();
	us_sorted.sort();
	assert_eq!(us_sorted, vec![1, 3], "pruned us partition scan must return exactly the us rows");

	let eu = collect_n(&db, "FROM test::by_region FILTER region == \"eu\"");
	assert_eq!(eu, vec![2], "pruned eu partition scan must return exactly the eu rows");
}

fn is_monotonic(v: &[i32]) -> bool {
	v.windows(2).all(|w| w[0] <= w[1]) || v.windows(2).all(|w| w[0] >= w[1])
}

// A partitioned view that is ALSO clustered-sorted (terminal SORT) stores rows as
// [PartitionedRow][shape][partition][sort-values][row]. Partitioning must not disturb the clustered
// sort order: each partition's rows must come back in the SAME order as an equivalent non-partitioned
// sorted view (the control), every row must survive the full partitioned scan, and the order must be
// an actual sort (monotonic), not insertion order.
#[test]
fn partitioned_view_with_terminal_sort() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, n: int4 }");
	command(
		&db,
		"INSERT test::events [{ region: \"us\", n: 3 }, { region: \"us\", n: 1 }, \
		 { region: \"eu\", n: 5 }, { region: \"us\", n: 2 }]",
	);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::sorted_plain { region: utf8, n: int4 } \
		 AS { FROM test::events SORT { n } }",
	);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::sorted_by_region { region: utf8, n: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM test::events SORT { n } }",
	);

	assert_eq!(
		await_row_count(&db, "FROM test::sorted_by_region", 4),
		4,
		"sorted+partitioned rows must all materialize"
	);
	await_row_count(&db, "FROM test::sorted_plain", 4);

	let control_us = collect_n(&db, "FROM test::sorted_plain FILTER region == \"us\"");
	let partitioned_us = collect_n(&db, "FROM test::sorted_by_region FILTER region == \"us\"");
	assert_eq!(partitioned_us, control_us, "partitioning must preserve the non-partitioned clustered sort order");
	assert!(is_monotonic(&partitioned_us), "us rows must be in clustered sort order, got {:?}", partitioned_us);
	let mut present = partitioned_us.clone();
	present.sort();
	assert_eq!(present, vec![1, 2, 3], "every us row must survive the partitioned scan");
}

// Ring-buffer-backed partitioned view: rows are partitioned AND capacity-bounded PER PARTITION. Eviction
// must rebuild the evicted row's PartitionedRow key from the persisted partition map; a wrong key would
// delete the wrong row (leaving stale rows) or fail to delete (over-capacity).
#[test]
fn ringbuffer_backed_partitioned_view_evicts() {
	let db = setup();
	seed_events(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);

	// capacity 2 per partition, three rows inserted (n=1 us, n=2 eu, n=3 us): us never exceeds
	// capacity (only 2 us rows total) and eu never exceeds capacity (only 1 eu row), so nothing is
	// evicted and all three rows survive.
	await_row_count(&db, "FROM test::rb", 3);
	let mut all = collect_n(&db, "FROM test::rb");
	all.sort();
	assert_eq!(all, vec![1, 2, 3], "capacity per partition must not be exceeded, so no eviction fires here");
	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(us, vec![1, 3], "us partition keeps both of its rows, under its own capacity");
	assert_eq!(collect_n(&db, "FROM test::rb FILTER region == \"eu\""), vec![2], "eu partition keeps its row");
}

// Capacity must be tracked independently per partition value: a partition that receives more rows than
// `capacity` must evict only its OWN oldest rows, and must never evict or be starved by another
// partition's activity. Before the per-partition fix, capacity was a single counter shared across all
// partitions, so a busy partition could evict a quiet partition's rows entirely.
#[test]
fn ringbuffer_backed_partitioned_view_evicts_independently_per_partition() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, n: int4 }");
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);

	// us receives 4 rows (over its capacity of 2), eu receives only 1 (well under capacity).
	command(
		&db,
		"INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }, \
		 { region: \"us\", n: 3 }, { region: \"us\", n: 4 }, { region: \"eu\", n: 5 }]",
	);

	await_row_count(&db, "FROM test::rb", 3);
	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(us, vec![3, 4], "us must keep only its own newest `capacity` rows, evicting n=1 and n=2");
	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"eu\""),
		vec![5],
		"eu's single row must survive untouched by us's eviction, proving capacity is per-partition"
	);
}

// A ring buffer without `partition: { by: ... }` uses the single global capacity counter, not the
// per-partition marker/metadata index. Eviction must still work correctly on that path.
#[test]
fn ringbuffer_backed_non_partitioned_view_evicts() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { n: int4 }");
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { n: int4 } WITH { capacity: 2 } AS { FROM test::events }",
	);
	command(&db, "INSERT test::events [{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }]");

	await_row_count(&db, "FROM test::rb", 2);
	let mut all = collect_n(&db, "FROM test::rb");
	all.sort();
	assert_eq!(all, vec![3, 4], "non-partitioned ring buffer must evict down to the newest `capacity` rows");
}

// A partitioned ring buffer assigns storage row numbers from a PER-PARTITION counter, independent of
// the upstream source table's row numbering - so a row's storage row number commonly differs from its
// source row number. An update on such a row must correctly resolve through the forward/row-entry
// remap, not just the (much rarer) case where they happen to coincide.
#[test]
fn ringbuffer_backed_view_update_remaps_row_number() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, n: int4 }");
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 10, partition: { by: { region } } } AS { FROM test::events }",
	);
	command(
		&db,
		"INSERT test::events [{ region: \"eu\", n: 1 }, { region: \"us\", n: 2 }, \
		 { region: \"us\", n: 3 }]",
	);
	await_row_count(&db, "FROM test::rb", 3);

	// The us partition's second row (n=3) has a partition-local storage row number that differs
	// from its source row number (3) - exercising the forward-index remap on update.
	command(&db, "UPDATE test::events { n: 999 } FILTER n == 3");
	await_row_count(&db, "FROM test::rb FILTER n == 999", 1);

	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(
		us,
		vec![2, 999],
		"update must round-trip correctly even when the row's storage row-number differs from its \
		 source row-number"
	);
}

// Partition columns are immutable: updating a downstream view's partition-by column on its source
// must be rejected outright at compile time (PART_004), not silently relocate the row.
#[test]
fn ringbuffer_backed_partitioned_view_update_of_partition_column_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, n: int4 }");
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);
	command(&db, "INSERT test::events [{ region: \"us\", n: 1 }]");
	await_row_count(&db, "FROM test::rb", 1);

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"updating a column that feeds a downstream partitioned view's partition key must be rejected"
	);

	// Rejected at compile time: nothing changed.
	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"us\""),
		vec![1],
		"row must remain under its original partition after the rejected update"
	);
	assert!(collect_n(&db, "FROM test::rb FILTER region == \"eu\"").is_empty(), "row must not have moved");
}

// A partition-changing UPDATE that would ALSO require evicting the destination partition's oldest
// row must be rejected just like any other partition-column update - not attempt the move-and-evict
// dance. Rejection must be atomic: neither partition is touched.
#[test]
fn ringbuffer_backed_partitioned_view_update_into_full_partition_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, n: int4 }");
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);
	command(
		&db,
		"INSERT test::events [{ region: \"eu\", n: 10 }, { region: \"eu\", n: 20 }, \
		 { region: \"us\", n: 1 }, { region: \"us\", n: 2 }]",
	);
	await_row_count(&db, "FROM test::rb", 4);

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"a move into an already-full destination partition must be rejected outright"
	);

	// Nothing evicted, nothing moved: both partitions remain exactly as they were.
	let mut eu = collect_n(&db, "FROM test::rb FILTER region == \"eu\"");
	eu.sort();
	assert_eq!(eu, vec![10, 20], "eu partition must be untouched by the rejected update");
	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(us, vec![1, 2], "us partition must be untouched by the rejected update");
}

// An explicit remove (not self-eviction) must free the vacated row's marker/count so a subsequent
// eviction in that partition targets a real, still-present row.
#[test]
fn ringbuffer_backed_partitioned_view_explicit_remove_then_evicts_correctly() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, n: int4 }");
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);
	command(&db, "INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }]");
	await_row_count(&db, "FROM test::rb", 2);

	command(&db, "DELETE test::events FILTER { n == 1 }");
	await_exact_row_count(&db, "FROM test::rb", 1);
	assert_eq!(
		collect_n(&db, "FROM test::rb"),
		vec![2],
		"explicit remove must delete the row from the ring buffer"
	);

	command(&db, "INSERT test::events [{ region: \"us\", n: 3 }, { region: \"us\", n: 4 }]");
	await_row_count(&db, "FROM test::rb", 2);
	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(
		us,
		vec![3, 4],
		"eviction after an explicit remove must evict real rows and leave the correct newest \
		 `capacity` survivors"
	);
}

// Once a partition's row count drops to zero (all rows removed/evicted), its metadata must be cleaned
// up so state does not accumulate forever for partitions that go quiet (e.g. a token that stops
// trading). A fresh insert into that partition value again must behave like a brand-new partition.
#[test]
fn ringbuffer_backed_partitioned_view_resets_after_partition_empties() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, n: int4 }");
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);
	command(&db, "INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }]");
	await_row_count(&db, "FROM test::rb", 2);

	command(&db, "DELETE test::events FILTER { region == \"us\" }");
	await_exact_row_count(&db, "FROM test::rb", 0);

	command(
		&db,
		"INSERT test::events [{ region: \"us\", n: 3 }, { region: \"us\", n: 4 }, \
		 { region: \"us\", n: 5 }]",
	);
	await_row_count(&db, "FROM test::rb", 2);
	let mut us = collect_n(&db, "FROM test::rb");
	us.sort();
	assert_eq!(
		us,
		vec![4, 5],
		"partition must behave as freshly created after emptying out: capacity 2 enforced correctly \
		 (evicting n=3), not corrupted by leftover metadata from before it emptied"
	);
}

// Series-backed partitioned view: rows are stored under PartitionedRow with a Series locator
// (sequence = row number). Exercises the series write path and the ViewScanNode Series-locator decode
// branch that the table/ring-buffer backends do not.
#[test]
fn series_backed_partitioned_view_stores_and_prunes() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::ticks { ts: int8, region: utf8, n: int4 }");
	command(
		&db,
		"INSERT test::ticks [{ ts: 1, region: \"us\", n: 1 }, { ts: 2, region: \"eu\", n: 2 }, \
		 { ts: 3, region: \"us\", n: 3 }]",
	);
	admin(
		&db,
		"CREATE DEFERRED SERIES VIEW test::s { ts: int8, region: utf8, n: int4 } \
		 WITH { key: ts, partition: { by: { region } } } AS { FROM test::ticks }",
	);

	assert_eq!(
		await_row_count(&db, "FROM test::s", 3),
		3,
		"series view rows must materialize in the partitioned keyspace"
	);

	let mut us = collect_n(&db, "FROM test::s FILTER region == \"us\"");
	us.sort();
	assert_eq!(us, vec![1, 3], "series partition pruning must return exactly the us rows");
}

// The partition columns must reference the view's declared output columns; an unknown column is a
// planning error, not a silent no-op.
#[test]
fn partition_column_must_exist() {
	let db = setup();
	seed_events(&db);
	let err = db
		.admin_as_root(
			"CREATE DEFERRED VIEW test::bad { region: utf8, n: int4 } \
			 WITH { partition: { by: { nope } } } AS { FROM test::events }",
			Params::None,
		)
		.expect_err("partition by an unknown column must be rejected");
	let diag = err.diagnostic();
	assert!(
		diag.message.contains("nope") || diag.message.to_lowercase().contains("column"),
		"expected a column-not-found diagnostic, got {:?}: {}",
		diag.code,
		diag.message
	);
}

// Two independent guards make partition columns immutable via UPDATE:
//  - PART_002 (engine, runtime, value-based): an UPDATE that would change the row's OWN computed partition on the
//    object being updated directly. Same-value reassignment is fine, since the computed partition doesn't actually
//    change.
//  - PART_004 (rql, compile time, column-identity-based): an UPDATE that assigns a column which feeds a downstream
//    (possibly multi-hop) partitioned view's partition key. This has no row values to compare at compile time, so it
//    rejects by column name alone, regardless of whether the value would actually change.
#[test]
fn table_own_partition_column_update_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::t { region: utf8, n: int4 } WITH { partition: { by: { region } } }");
	command(&db, "INSERT test::t [{ region: \"us\", n: 1 }]");

	assert_eq!(
		err_code(&db, "UPDATE test::t { region: \"eu\" } FILTER n == 1"),
		"PART_002",
		"changing a table's own partition column must be rejected"
	);
	assert_eq!(
		collect_n(&db, "FROM test::t FILTER region == \"us\""),
		vec![1],
		"row must remain under its original partition"
	);
}

#[test]
fn table_own_partition_column_same_value_reassignment_allowed() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::t { region: utf8, n: int4 } WITH { partition: { by: { region } } }");
	command(&db, "INSERT test::t [{ region: \"us\", n: 1 }]");

	command(&db, "UPDATE test::t { region: region, n: 2 } FILTER n == 1");
	assert_eq!(
		collect_n(&db, "FROM test::t FILTER region == \"us\""),
		vec![2],
		"same-value partition reassignment must still succeed - Part A is value-based"
	);
}

#[test]
fn series_own_partition_column_update_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(
		&db,
		"CREATE SERIES test::s { ts: int8, region: utf8, n: int4 } WITH { key: ts, partition: { by: { region } } }",
	);
	command(&db, "INSERT test::s [{ ts: 1, region: \"us\", n: 1 }]");

	assert_eq!(
		err_code(&db, "UPDATE test::s { region: \"eu\" } FILTER n == 1"),
		"PART_002",
		"changing a series' own partition column must be rejected"
	);
}

#[test]
fn series_own_partition_column_same_value_reassignment_allowed() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(
		&db,
		"CREATE SERIES test::s { ts: int8, region: utf8, n: int4 } WITH { key: ts, partition: { by: { region } } }",
	);
	command(&db, "INSERT test::s [{ ts: 1, region: \"us\", n: 1 }]");

	command(&db, "UPDATE test::s { region: region, n: 2 } FILTER n == 1");
	assert_eq!(
		collect_n(&db, "FROM test::s FILTER region == \"us\""),
		vec![2],
		"same-value partition reassignment must still succeed on a series too"
	);
}

#[test]
fn ringbuffer_own_partition_column_update_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(
		&db,
		"CREATE RINGBUFFER test::rb { region: utf8, n: int4 } WITH { capacity: 4, partition: { by: { region } } }",
	);
	command(&db, "INSERT test::rb [{ region: \"us\", n: 1 }]");

	assert_eq!(
		err_code(&db, "UPDATE test::rb { region: \"eu\" } FILTER n == 1"),
		"PART_002",
		"changing a ring buffer's own partition column must be rejected"
	);
	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"us\""),
		vec![1],
		"row must remain under its original partition"
	);
}

#[test]
fn ringbuffer_own_partition_column_same_value_reassignment_allowed() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(
		&db,
		"CREATE RINGBUFFER test::rb { region: utf8, n: int4 } WITH { capacity: 4, partition: { by: { region } } }",
	);
	command(&db, "INSERT test::rb [{ region: \"us\", n: 1 }]");

	command(&db, "UPDATE test::rb { region: region, n: 2 } FILTER n == 1");
	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"us\""),
		vec![2],
		"same-value partition reassignment must still succeed on a base ring buffer too"
	);
}

#[test]
fn table_source_feeds_table_view_partition_column_update_rejected() {
	let db = setup();
	seed_events(&db);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::events }",
	);
	await_row_count(&db, "FROM test::v", 3);

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"north\" } FILTER n == 1"),
		"PART_004",
		"updating an unpartitioned table's column that feeds a downstream table-backed partitioned \
		 view must be rejected"
	);
}

#[test]
fn table_source_feeds_ringbuffer_view_partition_column_update_rejected() {
	let db = setup();
	seed_events(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rv { region: utf8, n: int4 } \
		 WITH { capacity: 8, partition: { by: { region } } } AS { FROM test::events }",
	);
	await_row_count(&db, "FROM test::rv", 3);

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"north\" } FILTER n == 1"),
		"PART_004",
		"updating an unpartitioned table's column that feeds a downstream ring-buffer-backed \
		 partitioned view must be rejected"
	);
}

#[test]
fn table_source_feeds_series_view_partition_column_update_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::ticks { ts: int8, region: utf8, n: int4 }");
	command(&db, "INSERT test::ticks [{ ts: 1, region: \"us\", n: 1 }]");
	admin(
		&db,
		"CREATE DEFERRED SERIES VIEW test::sv { ts: int8, region: utf8, n: int4 } \
		 WITH { key: ts, partition: { by: { region } } } AS { FROM test::ticks }",
	);
	await_row_count(&db, "FROM test::sv", 1);

	assert_eq!(
		err_code(&db, "UPDATE test::ticks { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"updating an unpartitioned table's column that feeds a downstream series-backed partitioned \
		 view must be rejected"
	);
}

#[test]
fn series_source_feeds_table_view_partition_column_update_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE SERIES test::s { ts: int8, region: utf8, n: int4 } WITH { key: ts }");
	command(&db, "INSERT test::s [{ ts: 1, region: \"us\", n: 1 }]");
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v { ts: int8, region: utf8, n: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM test::s }",
	);
	await_row_count(&db, "FROM test::v", 1);

	assert_eq!(
		err_code(&db, "UPDATE test::s { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"updating an unpartitioned series' column that feeds a downstream partitioned view must be \
		 rejected"
	);
}

#[test]
fn ringbuffer_source_feeds_table_view_partition_column_update_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE RINGBUFFER test::rb { region: utf8, n: int4 } WITH { capacity: 8 }");
	command(&db, "INSERT test::rb [{ region: \"us\", n: 1 }]");
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::rb }",
	);
	await_row_count(&db, "FROM test::v", 1);

	assert_eq!(
		err_code(&db, "UPDATE test::rb { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"updating an unpartitioned ring buffer's column that feeds a downstream partitioned view must \
		 be rejected"
	);
}

// The dependency scan must be transitive: a plain, unpartitioned intermediate view sits between the
// table and the partitioned view, so a one-hop-only scan would miss this.
#[test]
fn nested_view_chain_partition_column_update_rejected_transitively() {
	let db = setup();
	seed_events(&db);
	admin(&db, "CREATE DEFERRED VIEW test::v1 { region: utf8, n: int4 } AS { FROM test::events }");
	await_row_count(&db, "FROM test::v1", 3);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v2 { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::v1 }",
	);
	await_row_count(&db, "FROM test::v2", 3);

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"north\" } FILTER n == 1"),
		"PART_004",
		"rejection must propagate transitively through an intermediate unpartitioned view"
	);
}

#[test]
fn downstream_view_zero_partition_columns_update_allowed() {
	let db = setup();
	seed_events(&db);
	admin(&db, "CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } AS { FROM test::events }");
	await_row_count(&db, "FROM test::v", 3);

	command(&db, "UPDATE test::events { region: \"north\" } FILTER n == 1");
	await_row_count(&db, "FROM test::v FILTER region == \"north\"", 1);
}

#[test]
fn downstream_view_two_partition_columns_update_either_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, tier: utf8, n: int4 }");
	command(
		&db,
		"INSERT test::events [{ region: \"us\", tier: \"gold\", n: 1 }, \
		 { region: \"us\", tier: \"gold\", n: 2 }]",
	);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v { region: utf8, tier: utf8, n: int4 } \
		 WITH { partition: { by: { region, tier } } } AS { FROM test::events }",
	);
	await_row_count(&db, "FROM test::v", 2);

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"updating the first of two partition columns must be rejected"
	);
	assert_eq!(
		err_code(&db, "UPDATE test::events { tier: \"silver\" } FILTER n == 2"),
		"PART_004",
		"updating the second of two partition columns must be rejected"
	);
}

#[test]
fn downstream_view_four_partition_columns_update_any_rejected() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { a: utf8, b: utf8, c: utf8, d: utf8, n: int4 }");
	command(
		&db,
		"INSERT test::events [{ a: \"1\", b: \"1\", c: \"1\", d: \"1\", n: 1 }, \
		 { a: \"1\", b: \"1\", c: \"1\", d: \"1\", n: 2 }]",
	);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v { a: utf8, b: utf8, c: utf8, d: utf8, n: int4 } \
		 WITH { partition: { by: { a, b, c, d } } } AS { FROM test::events }",
	);
	await_row_count(&db, "FROM test::v", 2);

	// `c` is neither the first nor the last partition column - proves the check scans every
	// assignment, not just the first or last.
	assert_eq!(
		err_code(&db, "UPDATE test::events { c: \"2\" } FILTER n == 1"),
		"PART_004",
		"updating a middle partition column (of four) must be rejected"
	);

	// A non-partition column update must still succeed normally.
	command(&db, "UPDATE test::events { n: 99 } FILTER n == 2");
	await_row_count(&db, "FROM test::v FILTER n == 99", 1);
}

#[test]
fn downstream_view_update_mixed_columns_rejected_when_any_is_partition_key() {
	let db = setup();
	seed_events(&db);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::events }",
	);
	await_row_count(&db, "FROM test::v", 3);

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"north\", n: 100 } FILTER n == 1"),
		"PART_004",
		"a mixed SET clause must be rejected if ANY assignment touches a partition column, even \
		 alongside unrelated columns"
	);
	assert!(
		collect_n(&db, "FROM test::v FILTER n == 100").is_empty(),
		"nothing in the rejected statement must apply, including the non-partition column"
	);
}

// Unlike Part A's value-based own-column guard, Part B has no row values to compare at compile
// time: it is column-identity-based, so even reassigning a downstream view's partition column to
// its current value is rejected.
#[test]
fn downstream_view_same_value_reassignment_still_rejected() {
	let db = setup();
	seed_events(&db);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::events }",
	);
	await_row_count(&db, "FROM test::v", 3);

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: region } FILTER n == 1"),
		"PART_004",
		"same-value reassignment of a downstream view's partition column must still be rejected - \
		 test::events itself is unpartitioned, so only Part B (identity-based) applies here"
	);
}

#[test]
fn two_downstream_views_different_partition_columns_both_enforced() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::events { region: utf8, tier: utf8, n: int4 }");
	command(&db, "INSERT test::events [{ region: \"us\", tier: \"gold\", n: 1 }]");
	admin(
		&db,
		"CREATE DEFERRED VIEW test::by_region { region: utf8, tier: utf8, n: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM test::events }",
	);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::by_tier { region: utf8, tier: utf8, n: int4 } \
		 WITH { partition: { by: { tier } } } AS { FROM test::events }",
	);
	await_row_count(&db, "FROM test::by_region", 1);
	await_row_count(&db, "FROM test::by_tier", 1);

	assert_eq!(
		err_code(&db, "UPDATE test::events { tier: \"silver\" } FILTER n == 1"),
		"PART_004",
		"`tier` is not by_region's partition key, but IS by_tier's - the second view alone must still \
		 block the update"
	);
	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"symmetric check: `region` is only by_region's partition key"
	);
}

#[test]
fn downstream_view_update_non_partition_column_allowed() {
	let db = setup();
	seed_events(&db);
	admin(
		&db,
		"CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::events }",
	);
	await_row_count(&db, "FROM test::v", 3);

	command(&db, "UPDATE test::events { n: 42 } FILTER n == 1");
	await_row_count(&db, "FROM test::v FILTER n == 42", 1);
}
