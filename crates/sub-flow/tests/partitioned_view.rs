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

// Ring-buffer-backed partitioned view: rows are partitioned AND capacity-bounded. Eviction must rebuild
// the evicted row's PartitionedRow key from the persisted partition map; a wrong key would delete the
// wrong row (leaving stale rows) or fail to delete (over-capacity).
#[test]
fn ringbuffer_backed_partitioned_view_evicts() {
	let db = setup();
	seed_events(&db);
	admin(
		&db,
		"CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }",
	);

	// capacity 2, three rows inserted (n=1 us, n=2 eu, n=3 us): the oldest (n=1) is evicted.
	await_row_count(&db, "FROM test::rb", 2);
	let mut all = collect_n(&db, "FROM test::rb");
	all.sort();
	assert_eq!(all, vec![2, 3], "ring buffer must keep the newest `capacity` rows across partitions");
	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"us\""),
		vec![3],
		"us partition keeps only the surviving us row"
	);
	assert_eq!(collect_n(&db, "FROM test::rb FILTER region == \"eu\""), vec![2], "eu partition keeps its row");
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
