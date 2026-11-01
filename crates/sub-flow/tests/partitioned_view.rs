// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A view can declare `partition by <cols>` so its materialized rows are physically partitioned in
// the underlying storage. These tests observe the view only through queries, so a broken write or
// read path surfaces as a wrong count: a non-partitioned scan of a partitioned view returns zero.

use reifydb::{Params, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_value::value::{duration::Duration, system_columns::row_numbers};

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|c| c).build().expect("build memory db with flow"))
}

fn err_code(db: &TestDb, rql: &str) -> String {
	// Callers need the exact code, since only a table's own partition column refuses an update (PART_002).
	match db.try_command(rql) {
		Ok(_) => panic!("expected command to fail, but it succeeded\nrql: {rql}"),
		Err(e) => e.diagnostic().code.clone(),
	}
}

fn collect_n(db: &TestDb, rql: &str) -> Vec<i32> {
	let frames = db.query(rql);
	let mut out = Vec::new();
	for f in &frames {
		for r in 0..f.row_count() {
			out.push(f.get::<i32>("n", r).expect("get n").expect("n defined"));
		}
	}
	out
}

fn seed_events(db: &TestDb) {
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
	db.command(
		"INSERT test::events [{ region: \"us\", n: 1 }, { region: \"eu\", n: 2 }, { region: \"us\", n: 3 }]",
	);
}

#[test]
fn table_backed_partitioned_view_stores_and_prunes() {
	// A sink writing plain Row keys, or a scan reading the Row keyspace, sees zero rows; pruning
	// that hashes inconsistently with the write returns the wrong FILTER subset.
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED VIEW test::by_region { region: utf8, n: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM test::events }");

	assert_eq!(
		db.await_row_count("FROM test::by_region", 3, Duration::from_seconds_const(5)),
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

#[test]
fn partitioned_view_with_terminal_sort() {
	// Partitioning must not disturb a terminal SORT: each partition comes back in the same order
	// as the non-partitioned control, every row survives the scan, and the order is an actual
	// sort rather than insertion order.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
	db.command(
		"INSERT test::events [{ region: \"us\", n: 3 }, { region: \"us\", n: 1 }, \
		 { region: \"eu\", n: 5 }, { region: \"us\", n: 2 }]",
	);
	db.admin("CREATE DEFERRED VIEW test::sorted_plain { region: utf8, n: int4 } \
		 AS { FROM test::events SORT { n } }");
	db.admin("CREATE DEFERRED VIEW test::sorted_by_region { region: utf8, n: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM test::events SORT { n } }");

	assert_eq!(
		db.await_row_count("FROM test::sorted_by_region", 4, Duration::from_seconds_const(5)),
		4,
		"sorted+partitioned rows must all materialize"
	);
	db.await_row_count("FROM test::sorted_plain", 4, Duration::from_seconds_const(5));

	let control_us = collect_n(&db, "FROM test::sorted_plain FILTER region == \"us\"");
	let partitioned_us = collect_n(&db, "FROM test::sorted_by_region FILTER region == \"us\"");
	assert_eq!(partitioned_us, control_us, "partitioning must preserve the non-partitioned sorted view order");
	assert!(is_monotonic(&partitioned_us), "us rows must be in sorted view order, got {:?}", partitioned_us);
	let mut present = partitioned_us.clone();
	present.sort();
	assert_eq!(present, vec![1, 2, 3], "every us row must survive the partitioned scan");
}

#[test]
fn ringbuffer_backed_partitioned_view_evicts() {
	// Eviction resolves the row to evict through the per-partition row-entry index; a wrong key
	// deletes the wrong row or fails to delete at all.
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }");

	// Capacity 2 per partition and neither partition exceeds it (2 us, 1 eu), so nothing evicts.
	db.await_row_count("FROM test::rb", 3, Duration::from_seconds_const(5));
	let mut all = collect_n(&db, "FROM test::rb");
	all.sort();
	assert_eq!(all, vec![1, 2, 3], "capacity per partition must not be exceeded, so no eviction fires here");
	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(us, vec![1, 3], "us partition keeps both of its rows, under its own capacity");
	assert_eq!(collect_n(&db, "FROM test::rb FILTER region == \"eu\""), vec![2], "eu partition keeps its row");
}

#[test]
fn ringbuffer_backed_partitioned_view_evicts_independently_per_partition() {
	// Capacity is tracked per partition value: a busy partition evicts only its own oldest rows
	// and must never starve a quiet one, which a single shared counter would.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }");

	// us receives 4 rows (over its capacity of 2), eu receives only 1 (well under capacity).
	db.command(
		"INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }, \
		 { region: \"us\", n: 3 }, { region: \"us\", n: 4 }, { region: \"eu\", n: 5 }]",
	);

	db.await_row_count("FROM test::rb", 3, Duration::from_seconds_const(5));
	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(us, vec![3, 4], "us must keep only its own newest `capacity` rows, evicting n=1 and n=2");
	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"eu\""),
		vec![5],
		"eu's single row must survive untouched by us's eviction, proving capacity is per-partition"
	);
}

#[test]
fn ringbuffer_backed_non_partitioned_view_evicts() {
	// Without `partition: { by: ... }` eviction runs off the single global ring-buffer metadata
	// rather than the per-partition metadata, which is a separate path.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { n: int4 }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { n: int4 } WITH { capacity: 2 } AS { FROM test::events }");
	db.command("INSERT test::events [{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }]");

	db.await_row_count("FROM test::rb", 2, Duration::from_seconds_const(5));
	let mut all = collect_n(&db, "FROM test::rb");
	all.sort();
	assert_eq!(all, vec![3, 4], "non-partitioned ring buffer must evict down to the newest `capacity` rows");
}

#[test]
fn ringbuffer_backed_view_update_remaps_row_number() {
	// A partitioned ring buffer numbers storage rows from a per-partition counter, so a row's
	// storage row number commonly differs from its source row number and the update has to
	// resolve through the remap rather than only in the rarer coinciding case.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 10, partition: { by: { region } } } AS { FROM test::events }");
	db.command(
		"INSERT test::events [{ region: \"eu\", n: 1 }, { region: \"us\", n: 2 }, \
		 { region: \"us\", n: 3 }]",
	);
	db.await_row_count("FROM test::rb", 3, Duration::from_seconds_const(5));

	// The us partition's second row (n=3) has a partition-local storage row number that differs
	// from its source row number (3) - exercising the forward-index remap on update.
	db.command("UPDATE test::events { n: 999 } FILTER n == 3");
	db.await_row_count("FROM test::rb FILTER n == 999", 1, Duration::from_seconds_const(5));

	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(
		us,
		vec![2, 999],
		"update must round-trip correctly even when the row's storage row-number differs from its \
		 source row-number"
	);
}

#[test]
fn ringbuffer_backed_partitioned_view_update_of_partition_column_moves_row() {
	// A row left under its old partition after its partition column changed is read in the wrong partition.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }");
	db.command("INSERT test::events [{ region: \"us\", n: 1 }]");
	db.await_row_count("FROM test::rb", 1, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: \"eu\" } FILTER n == 1");
	db.await_row_count("FROM test::rb FILTER region == \"eu\"", 1, Duration::from_seconds_const(5));

	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"eu\""),
		vec![1],
		"the row must sit in its new partition"
	);
	assert!(
		collect_n(&db, "FROM test::rb FILTER region == \"us\"").is_empty(),
		"the old partition must not keep it"
	);
}

#[test]
fn ringbuffer_backed_partitioned_view_update_into_full_partition_evicts_its_oldest() {
	// A row moved into a full partition counts against its capacity, so the destination's oldest row must go.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }");
	db.command(
		"INSERT test::events [{ region: \"eu\", n: 10 }, { region: \"eu\", n: 20 }, \
		 { region: \"us\", n: 1 }, { region: \"us\", n: 2 }]",
	);
	db.await_row_count("FROM test::rb", 4, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: \"eu\" } FILTER n == 1");
	db.await_row_count("FROM test::rb FILTER region == \"eu\" and n == 1", 1, Duration::from_seconds_const(5));

	assert_eq!(
		{
			let mut rows = collect_n(&db, "FROM test::rb FILTER region == \"eu\"");
			rows.sort();
			rows
		},
		vec![1, 20],
		"eu must evict its oldest row, n 10, for the moved one"
	);
	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"us\""),
		vec![2],
		"us must keep only the row that stayed"
	);
}

#[test]
fn ringbuffer_backed_partitioned_view_explicit_remove_then_evicts_correctly() {
	// An explicit remove, unlike self-eviction, must free the vacated row's entry so the next
	// eviction in that partition targets a real, still-present row.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }");
	db.command("INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }]");
	db.await_row_count("FROM test::rb", 2, Duration::from_seconds_const(5));

	db.command("DELETE test::events FILTER { n == 1 }");
	// `await_row_count`'s `>= want` returns instantly on a stale higher count, so waiting for a
	// decrease has to match exactly.
	db.await_exact_row_count("FROM test::rb", 1, Duration::from_seconds_const(5));
	assert_eq!(
		collect_n(&db, "FROM test::rb"),
		vec![2],
		"explicit remove must delete the row from the ring buffer"
	);

	db.command("INSERT test::events [{ region: \"us\", n: 3 }, { region: \"us\", n: 4 }]");
	db.await_row_count("FROM test::rb", 2, Duration::from_seconds_const(5));
	let mut us = collect_n(&db, "FROM test::rb FILTER region == \"us\"");
	us.sort();
	assert_eq!(
		us,
		vec![3, 4],
		"eviction after an explicit remove must evict real rows and leave the correct newest \
		 `capacity` survivors"
	);
}

#[test]
fn ringbuffer_backed_partitioned_view_resets_after_partition_empties() {
	// A partition that empties must drop its metadata, or state accumulates forever for partition
	// values that go quiet; inserting into that value again must behave like a fresh partition.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }");
	db.command("INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }]");
	db.await_row_count("FROM test::rb", 2, Duration::from_seconds_const(5));

	db.command("DELETE test::events FILTER { region == \"us\" }");
	// The count must decrease to 0, which `await_row_count`'s `>= want` cannot observe.
	db.await_exact_row_count("FROM test::rb", 0, Duration::from_seconds_const(5));

	db.command(
		"INSERT test::events [{ region: \"us\", n: 3 }, { region: \"us\", n: 4 }, \
		 { region: \"us\", n: 5 }]",
	);
	db.await_row_count("FROM test::rb", 2, Duration::from_seconds_const(5));
	let mut us = collect_n(&db, "FROM test::rb");
	us.sort();
	assert_eq!(
		us,
		vec![4, 5],
		"partition must behave as freshly created after emptying out: capacity 2 enforced correctly \
		 (evicting n=3), not corrupted by leftover metadata from before it emptied"
	);
}

#[test]
fn series_backed_partitioned_view_stores_and_prunes() {
	// A partitioned series view must round-trip through the partitioned series keyspace, never the plain row one.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::ticks { ts: int8, region: utf8, n: int4 }");
	db.command(
		"INSERT test::ticks [{ ts: 1, region: \"us\", n: 1 }, { ts: 2, region: \"eu\", n: 2 }, \
		 { ts: 3, region: \"us\", n: 3 }]",
	);
	db.admin("CREATE DEFERRED SERIES VIEW test::s { ts: int8, region: utf8, n: int4 } \
		 WITH { key: ts, partition: { by: { region } } } AS { FROM test::ticks }");

	assert_eq!(
		db.await_row_count("FROM test::s", 3, Duration::from_seconds_const(5)),
		3,
		"series view rows must materialize in the partitioned keyspace"
	);

	let mut us = collect_n(&db, "FROM test::s FILTER region == \"us\"");
	us.sort();
	assert_eq!(us, vec![1, 3], "series partition pruning must return exactly the us rows");
}

#[test]
fn series_backed_partitioned_view_rows_keep_distinct_row_numbers() {
	// The sink puts the flow row number in the key's sequence field, so the scan has to read it
	// back from there. Decoding a partitioned series key as a plain partitioned row key yields
	// RowNumber(0) for every row, and rows that share a row number collapse onto each other in
	// every downstream consumer.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::ticks { ts: int8, region: utf8, n: int4 }");
	db.command(
		"INSERT test::ticks [{ ts: 1, region: \"us\", n: 1 }, { ts: 2, region: \"eu\", n: 2 }, \
		 { ts: 3, region: \"us\", n: 3 }]",
	);
	db.admin("CREATE DEFERRED SERIES VIEW test::s { ts: int8, region: utf8, n: int4 } \
		 WITH { key: ts, partition: { by: { region } } } AS { FROM test::ticks }");

	assert_eq!(db.await_row_count("FROM test::s", 3, Duration::from_seconds_const(5)), 3);

	let mut seen: Vec<u64> = db
		.query("FROM test::s")
		.iter()
		.flat_map(|frame| row_numbers(&frame.batch).expect("#rownum").iter().map(|rn| rn.0).collect::<Vec<_>>())
		.collect();
	seen.sort();
	let mut distinct = seen.clone();
	distinct.dedup();

	assert_eq!(seen.len(), 3, "every materialized row must reach the reader");
	assert_eq!(distinct, seen, "no two series view rows may share a row number: {seen:?}");
	assert!(!seen.contains(&0), "a row number of zero means the sequence was never decoded: {seen:?}");
}

#[test]
fn partition_column_must_exist() {
	// Partition columns reference the view's declared output columns, so an unknown one is a
	// planning error rather than a silent no-op.
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

#[test]
fn table_own_partition_column_update_rejected() {
	// A table's own partition column is immutable: PART_002 fails only when the computed partition changes.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { region: utf8, n: int4 } WITH { partition: { by: { region } } }");
	db.command("INSERT test::t [{ region: \"us\", n: 1 }]");

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
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { region: utf8, n: int4 } WITH { partition: { by: { region } } }");
	db.command("INSERT test::t [{ region: \"us\", n: 1 }]");

	db.command("UPDATE test::t { region: region, n: 2 } FILTER n == 1");
	assert_eq!(
		collect_n(&db, "FROM test::t FILTER region == \"us\""),
		vec![2],
		"same-value partition reassignment must still succeed - Part A is value-based"
	);
}

#[test]
fn series_own_partition_column_update_rejected() {
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin(
		"CREATE SERIES test::s { ts: int8, region: utf8, n: int4 } WITH { key: ts, partition: { by: { region } } }",
	);
	db.command("INSERT test::s [{ ts: 1, region: \"us\", n: 1 }]");

	assert_eq!(
		err_code(&db, "UPDATE test::s { region: \"eu\" } FILTER n == 1"),
		"PART_002",
		"changing a series' own partition column must be rejected"
	);
}

#[test]
fn series_own_partition_column_same_value_reassignment_allowed() {
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin(
		"CREATE SERIES test::s { ts: int8, region: utf8, n: int4 } WITH { key: ts, partition: { by: { region } } }",
	);
	db.command("INSERT test::s [{ ts: 1, region: \"us\", n: 1 }]");

	db.command("UPDATE test::s { region: region, n: 2 } FILTER n == 1");
	assert_eq!(
		collect_n(&db, "FROM test::s FILTER region == \"us\""),
		vec![2],
		"same-value partition reassignment must still succeed on a series too"
	);
}

#[test]
fn ringbuffer_own_partition_column_update_rejected() {
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin(
		"CREATE RINGBUFFER test::rb { region: utf8, n: int4 } WITH { capacity: 4, partition: { by: { region } } }",
	);
	db.command("INSERT test::rb [{ region: \"us\", n: 1 }]");

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
	db.admin("CREATE NAMESPACE test");
	db.admin(
		"CREATE RINGBUFFER test::rb { region: utf8, n: int4 } WITH { capacity: 4, partition: { by: { region } } }",
	);
	db.command("INSERT test::rb [{ region: \"us\", n: 1 }]");

	db.command("UPDATE test::rb { region: region, n: 2 } FILTER n == 1");
	assert_eq!(
		collect_n(&db, "FROM test::rb FILTER region == \"us\""),
		vec![2],
		"same-value partition reassignment must still succeed on a base ring buffer too"
	);
}

#[test]
fn table_source_feeds_table_view_partition_column_update_moves_row() {
	// A downstream partitioned view must move the row with its source, or it files the row under a stale value.
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::events }");
	db.await_row_count("FROM test::v", 3, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: \"north\" } FILTER n == 1");
	db.await_row_count("FROM test::v FILTER region == \"north\"", 1, Duration::from_seconds_const(5));

	assert_eq!(
		collect_n(&db, "FROM test::v FILTER region == \"north\""),
		vec![1],
		"the row must sit in its new partition"
	);
	assert_eq!(
		collect_n(&db, "FROM test::v FILTER region == \"us\""),
		vec![3],
		"the old partition must keep only the rows that stayed"
	);
}

#[test]
fn table_source_feeds_ringbuffer_view_partition_column_update_moves_row() {
	// A downstream partitioned view must move the row with its source, or it files the row under a stale value.
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rv { region: utf8, n: int4 } \
		 WITH { capacity: 8, partition: { by: { region } } } AS { FROM test::events }");
	db.await_row_count("FROM test::rv", 3, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: \"north\" } FILTER n == 1");
	db.await_row_count("FROM test::rv FILTER region == \"north\"", 1, Duration::from_seconds_const(5));

	assert_eq!(
		collect_n(&db, "FROM test::rv FILTER region == \"north\""),
		vec![1],
		"the row must sit in its new partition"
	);
	assert_eq!(
		collect_n(&db, "FROM test::rv FILTER region == \"us\""),
		vec![3],
		"the old partition must keep only the rows that stayed"
	);
}

#[test]
fn table_source_feeds_series_view_partition_column_update_moves_row() {
	// A downstream partitioned view must move the row with its source, or it files the row under a stale value.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::ticks { ts: int8, region: utf8, n: int4 }");
	db.command("INSERT test::ticks [{ ts: 1, region: \"us\", n: 1 }]");
	db.admin("CREATE DEFERRED SERIES VIEW test::sv { ts: int8, region: utf8, n: int4 } \
		 WITH { key: ts, partition: { by: { region } } } AS { FROM test::ticks }");
	db.await_row_count("FROM test::sv", 1, Duration::from_seconds_const(5));

	db.command("UPDATE test::ticks { region: \"eu\" } FILTER n == 1");
	db.await_row_count("FROM test::sv FILTER region == \"eu\"", 1, Duration::from_seconds_const(5));

	assert_eq!(
		collect_n(&db, "FROM test::sv FILTER region == \"eu\""),
		vec![1],
		"the row must sit in its new partition"
	);
	assert_eq!(
		collect_n(&db, "FROM test::sv FILTER region == \"us\""),
		Vec::<i32>::new(),
		"the old partition must keep only the rows that stayed"
	);
}

#[test]
fn series_source_feeds_table_view_partition_column_update_moves_row() {
	// A downstream partitioned view must move the row with its source, or it files the row under a stale value.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE SERIES test::s { ts: int8, region: utf8, n: int4 } WITH { key: ts }");
	db.command("INSERT test::s [{ ts: 1, region: \"us\", n: 1 }]");
	db.admin("CREATE DEFERRED VIEW test::v { ts: int8, region: utf8, n: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM test::s }");
	db.await_row_count("FROM test::v", 1, Duration::from_seconds_const(5));

	db.command("UPDATE test::s { region: \"eu\" } FILTER n == 1");
	db.await_row_count("FROM test::v FILTER region == \"eu\"", 1, Duration::from_seconds_const(5));

	assert_eq!(
		collect_n(&db, "FROM test::v FILTER region == \"eu\""),
		vec![1],
		"the row must sit in its new partition"
	);
	assert_eq!(
		collect_n(&db, "FROM test::v FILTER region == \"us\""),
		Vec::<i32>::new(),
		"the old partition must keep only the rows that stayed"
	);
}

#[test]
fn ringbuffer_source_feeds_table_view_partition_column_update_moves_row() {
	// A downstream partitioned view must move the row with its source, or it files the row under a stale value.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE RINGBUFFER test::rb { region: utf8, n: int4 } WITH { capacity: 8 }");
	db.command("INSERT test::rb [{ region: \"us\", n: 1 }]");
	db.admin("CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::rb }");
	db.await_row_count("FROM test::v", 1, Duration::from_seconds_const(5));

	db.command("UPDATE test::rb { region: \"eu\" } FILTER n == 1");
	db.await_row_count("FROM test::v FILTER region == \"eu\"", 1, Duration::from_seconds_const(5));

	assert_eq!(
		collect_n(&db, "FROM test::v FILTER region == \"eu\""),
		vec![1],
		"the row must sit in its new partition"
	);
	assert_eq!(
		collect_n(&db, "FROM test::v FILTER region == \"us\""),
		Vec::<i32>::new(),
		"the old partition must keep only the rows that stayed"
	);
}

#[test]
fn nested_view_chain_partition_column_update_moves_the_row_through_both_views() {
	// The move must pass through an unpartitioned intermediate view, or the partitioned one keeps the old row.
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED VIEW test::v1 { region: utf8, n: int4 } AS { FROM test::events }");
	db.await_row_count("FROM test::v1", 3, Duration::from_seconds_const(5));
	db.admin("CREATE DEFERRED VIEW test::v2 { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::v1 }");
	db.await_row_count("FROM test::v2", 3, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: \"north\" } FILTER n == 1");
	db.await_row_count("FROM test::v2 FILTER region == \"north\"", 1, Duration::from_seconds_const(5));

	assert_eq!(
		collect_n(&db, "FROM test::v2 FILTER region == \"north\""),
		vec![1],
		"the row must reach its new partition"
	);
	assert_eq!(
		collect_n(&db, "FROM test::v2 FILTER region == \"us\""),
		vec![3],
		"the old partition must keep only n 3"
	);
}

#[test]
fn downstream_view_zero_partition_columns_update_allowed() {
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } AS { FROM test::events }");
	db.await_row_count("FROM test::v", 3, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: \"north\" } FILTER n == 1");
	db.await_row_count("FROM test::v FILTER region == \"north\"", 1, Duration::from_seconds_const(5));
}

#[test]
fn downstream_view_two_partition_columns_update_either_moves_the_row() {
	// A change to either partition column changes the partition, so both must move the row.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, tier: utf8, n: int4 }");
	db.command(
		"INSERT test::events [{ region: \"us\", tier: \"gold\", n: 1 }, \
		 { region: \"us\", tier: \"gold\", n: 2 }]",
	);
	db.admin("CREATE DEFERRED VIEW test::v { region: utf8, tier: utf8, n: int4 } \
		 WITH { partition: { by: { region, tier } } } AS { FROM test::events }");
	db.await_row_count("FROM test::v", 2, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: \"eu\" } FILTER n == 1");
	db.command("UPDATE test::events { tier: \"silver\" } FILTER n == 2");
	db.await_row_count("FROM test::v FILTER tier == \"silver\"", 1, Duration::from_seconds_const(5));

	assert_eq!(collect_n(&db, "FROM test::v FILTER region == \"eu\" and tier == \"gold\""), vec![1]);
	assert_eq!(collect_n(&db, "FROM test::v FILTER region == \"us\" and tier == \"silver\""), vec![2]);
	assert!(
		collect_n(&db, "FROM test::v FILTER region == \"us\" and tier == \"gold\"").is_empty(),
		"the old partition must be empty"
	);
}

#[test]
fn downstream_view_four_partition_columns_update_any_moves_the_row() {
	// `c` is neither the first nor the last partition column, so a move must read every partition column.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { a: utf8, b: utf8, c: utf8, d: utf8, n: int4 }");
	db.command(
		"INSERT test::events [{ a: \"1\", b: \"1\", c: \"1\", d: \"1\", n: 1 }, \
		 { a: \"1\", b: \"1\", c: \"1\", d: \"1\", n: 2 }]",
	);
	db.admin("CREATE DEFERRED VIEW test::v { a: utf8, b: utf8, c: utf8, d: utf8, n: int4 } \
		 WITH { partition: { by: { a, b, c, d } } } AS { FROM test::events }");
	db.await_row_count("FROM test::v", 2, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { c: \"2\" } FILTER n == 1");
	db.await_row_count("FROM test::v FILTER c == \"2\"", 1, Duration::from_seconds_const(5));
	assert_eq!(collect_n(&db, "FROM test::v FILTER c == \"2\""), vec![1], "the row must sit in its new partition");
	assert_eq!(collect_n(&db, "FROM test::v FILTER c == \"1\""), vec![2], "the old partition must keep only n 2");

	db.command("UPDATE test::events { n: 99 } FILTER n == 2");
	db.await_row_count("FROM test::v FILTER n == 99", 1, Duration::from_seconds_const(5));
}

#[test]
fn downstream_view_update_mixed_columns_moves_the_row_with_every_assignment_applied() {
	// A move that dropped the non-partition assignment would publish the moved row with a stale value.
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::events }");
	db.await_row_count("FROM test::v", 3, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: \"north\", n: 100 } FILTER n == 1");
	db.await_row_count("FROM test::v FILTER region == \"north\"", 1, Duration::from_seconds_const(5));

	assert_eq!(collect_n(&db, "FROM test::v FILTER region == \"north\""), vec![100]);
	assert!(collect_n(&db, "FROM test::v FILTER n == 1").is_empty(), "the old row must be gone");
}

#[test]
fn downstream_view_same_value_reassignment_leaves_the_row_in_its_partition() {
	// Reassigning the partition column to its own value must update in place, not move or duplicate the row.
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::events }");
	db.await_row_count("FROM test::v", 3, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { region: region, n: 7 } FILTER n == 1");
	db.await_row_count("FROM test::v FILTER n == 7", 1, Duration::from_seconds_const(5));

	assert_eq!(
		{
			let mut rows = collect_n(&db, "FROM test::v FILTER region == \"us\"");
			rows.sort();
			rows
		},
		vec![3, 7],
		"the row must stay in us with its new value"
	);
	assert_eq!(
		db.await_exact_row_count("FROM test::v", 3, Duration::from_seconds_const(5)),
		3,
		"no row may be duplicated"
	);
}

#[test]
fn two_downstream_views_different_partition_columns_each_move_the_row_by_their_own_key() {
	// Each view partitions by its own column, so each must move the row only when its own key changes.
	let db = setup();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, tier: utf8, n: int4 }");
	db.command("INSERT test::events [{ region: \"us\", tier: \"gold\", n: 1 }]");
	db.admin("CREATE DEFERRED VIEW test::by_region { region: utf8, tier: utf8, n: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM test::events }");
	db.admin("CREATE DEFERRED VIEW test::by_tier { region: utf8, tier: utf8, n: int4 } \
		 WITH { partition: { by: { tier } } } AS { FROM test::events }");
	db.await_row_count("FROM test::by_region", 1, Duration::from_seconds_const(5));
	db.await_row_count("FROM test::by_tier", 1, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { tier: \"silver\" } FILTER n == 1");
	db.await_row_count("FROM test::by_tier FILTER tier == \"silver\"", 1, Duration::from_seconds_const(5));
	assert_eq!(
		collect_n(&db, "FROM test::by_region FILTER region == \"us\""),
		vec![1],
		"by_region keeps the row in us"
	);
	assert!(
		collect_n(&db, "FROM test::by_tier FILTER tier == \"gold\"").is_empty(),
		"by_tier must move it out of gold"
	);

	db.command("UPDATE test::events { region: \"eu\" } FILTER n == 1");
	db.await_row_count("FROM test::by_region FILTER region == \"eu\"", 1, Duration::from_seconds_const(5));
	assert!(
		collect_n(&db, "FROM test::by_region FILTER region == \"us\"").is_empty(),
		"by_region must move it out of us"
	);
	assert_eq!(
		collect_n(&db, "FROM test::by_tier FILTER tier == \"silver\""),
		vec![1],
		"by_tier keeps the row in silver"
	);
}

#[test]
fn downstream_view_update_non_partition_column_allowed() {
	let db = setup();
	seed_events(&db);
	db.admin("CREATE DEFERRED VIEW test::v { region: utf8, n: int4 } WITH { partition: { by: { region } } } \
		 AS { FROM test::events }");
	db.await_row_count("FROM test::v", 3, Duration::from_seconds_const(5));

	db.command("UPDATE test::events { n: 42 } FILTER n == 1");
	db.await_row_count("FROM test::v FILTER n == 42", 1, Duration::from_seconds_const(5));
}
