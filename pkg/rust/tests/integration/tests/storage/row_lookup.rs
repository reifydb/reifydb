// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};

const ROWS: &str = "insert test::src [{ ts: 10, region: 'us', n: 1 }, { ts: 20, region: 'us', n: 2 }, \
                    { ts: 30, region: 'eu', n: 3 }]";

fn db_with(ddl: &[&str]) -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().unwrap());
	db.admin("create namespace test");
	db.admin("create table test::src { ts: int8, region: utf8, n: int4 }");
	for statement in ddl {
		db.admin(statement);
	}
	db.command(ROWS);
	assert!(db.await_all_flows(Duration::from_secs(10)), "the views must materialize before anything is asserted");
	db
}

fn plan_kinds(db: &TestDb, rql: &str) -> Vec<String> {
	let mut kinds = Vec::new();
	for frame in &db.query(&format!("CALL rql::explain('{rql}')")) {
		for r in 0..frame.row_count() {
			kinds.push(frame.get::<String>("kind", r).expect("kind").expect("kind defined"));
		}
	}
	kinds
}

fn rows_of(db: &TestDb, object: &str) -> Vec<(u64, i32)> {
	let mut out = Vec::new();
	for frame in &db.query(&format!("from {object} map {{ #rownum, n }}")) {
		for r in 0..frame.row_count() {
			let rownum = frame.get::<u64>("rownum", r).expect("rownum").expect("rownum defined");
			let n = frame.get::<i32>("n", r).expect("n").expect("n defined");
			out.push((rownum, n));
		}
	}
	out.sort();
	out
}

fn n_at_rownum(db: &TestDb, object: &str, rownum: u64) -> Vec<i32> {
	let mut out = Vec::new();
	for frame in &db.query(&format!("from {object} filter #rownum == {rownum}")) {
		for r in 0..frame.row_count() {
			out.push(frame.get::<i32>("n", r).expect("n").expect("n defined"));
		}
	}
	out
}

fn assert_every_row_is_addressable(db: &TestDb, object: &str) {
	let all = rows_of(db, object);
	assert_eq!(all.len(), 3, "precondition: {object} must hold all three rows");

	for (rownum, n) in all {
		assert_eq!(
			n_at_rownum(db, object, rownum),
			vec![n],
			"{object}: a #rownum the full scan just reported must address exactly that row; an empty \
			 answer means the plan probed a keyspace the row was never written to"
		);
	}
}

#[test]
fn a_table_backed_view_folds_rownum_into_a_point_lookup() {
	// The control. An unpartitioned table-backed view is the one shape whose rows really do live
	// under the plain row key, so the rewrite is allowed to skip the scan entirely. If this stops
	// being a lookup the guards below have been widened too far and the optimisation is gone.
	let db = db_with(&["create deferred view test::v { ts: int8, region: utf8, n: int4 } as { from test::src }"]);

	assert_eq!(
		plan_kinds(&db, "from test::v filter #rownum == 1"),
		vec!["RowPointLookup".to_string()],
		"the plain keyspace must still collapse to a single point lookup, with no scan and no filter left"
	);

	assert_every_row_is_addressable(&db, "test::v");
}

#[test]
fn a_series_backed_view_answers_a_rownum_lookup() {
	// A series row carries its row number in the key's trailing sequence field, behind the series
	// key, so no row key can be built from a row number alone. The rewrite has to decline and let
	// the scan plus filter answer, which is what the full scan already does correctly.
	let db = db_with(&[
		"create deferred series view test::v { ts: int8, region: utf8, n: int4 } with { key: ts } as { from test::src }",
	]);

	assert_every_row_is_addressable(&db, "test::v");

	let kinds = plan_kinds(&db, "from test::v filter #rownum == 3");
	assert!(
		!kinds.contains(&"RowPointLookup".to_string()),
		"a series keyspace cannot be point-looked-up by row number: {kinds:?}"
	);
	assert!(kinds.contains(&"Filter".to_string()), "the declined predicate must survive as a filter: {kinds:?}");
}

#[test]
fn a_partitioned_series_backed_view_answers_a_rownum_lookup() {
	// Both reasons to decline at once: the partition sits ahead of the series key, and the row
	// number sits behind it.
	let db = db_with(&[
		"create deferred series view test::v { ts: int8, region: utf8, n: int4 } with { key: ts, partition: { by: { region } } } as { from test::src }",
	]);

	assert_every_row_is_addressable(&db, "test::v");

	let kinds = plan_kinds(&db, "from test::v filter #rownum == 3");
	assert!(
		!kinds.contains(&"RowPointLookup".to_string()),
		"a partitioned series keyspace cannot be point-looked-up by row number: {kinds:?}"
	);
}

#[test]
fn a_partitioned_view_answers_a_rownum_lookup() {
	// A partitioned key prefixes the row number with the partition value, which a row number does
	// not carry, so the one key the lookup could build is the wrong one.
	let db = db_with(&[
		"create deferred view test::v { ts: int8, region: utf8, n: int4 } with { partition: { by: { region } } } as { from test::src }",
	]);

	assert_every_row_is_addressable(&db, "test::v");

	let kinds = plan_kinds(&db, "from test::v filter #rownum == 1");
	assert!(
		!kinds.contains(&"RowPointLookup".to_string()),
		"a partitioned keyspace cannot be point-looked-up by row number: {kinds:?}"
	);
}

#[test]
fn a_partitioned_table_answers_a_rownum_lookup() {
	// Same partition prefix, reached through the table arm, which had no guard at all.
	let db = db_with(&[]);
	db.admin("create table test::pt { ts: int8, region: utf8, n: int4 } with { partition: { by: { region } } }");
	db.command(
		"insert test::pt [{ ts: 10, region: 'us', n: 1 }, { ts: 20, region: 'us', n: 2 }, \
		 { ts: 30, region: 'eu', n: 3 }]",
	);

	assert_every_row_is_addressable(&db, "test::pt");

	let kinds = plan_kinds(&db, "from test::pt filter #rownum == 1");
	assert!(
		!kinds.contains(&"RowPointLookup".to_string()),
		"a partitioned table cannot be point-looked-up by row number: {kinds:?}"
	);
}

#[test]
fn a_series_backed_view_answers_a_rownum_list_lookup() {
	// The list rewrite shares the same source guard as the point rewrite, so it has to decline on
	// the same keyspaces; a list that silently returns nothing is the identical defect.
	let db = db_with(&[
		"create deferred series view test::v { ts: int8, region: utf8, n: int4 } with { key: ts } as { from test::src }",
	]);

	let mut got = Vec::new();
	for frame in &db.query("from test::v filter #rownum in [1, 3]") {
		for r in 0..frame.row_count() {
			got.push(frame.get::<i32>("n", r).expect("n").expect("n defined"));
		}
	}
	got.sort();
	assert_eq!(got, vec![1, 3], "both listed row numbers must resolve, not silently drop to nothing");

	let kinds = plan_kinds(&db, "from test::v filter #rownum in [1, 3]");
	assert!(
		!kinds.contains(&"RowListLookup".to_string()),
		"a series keyspace cannot be list-looked-up by row number: {kinds:?}"
	);
}
