// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;

fn plan(db: &TestDb, rql: &str) -> Vec<(String, String)> {
	let mut rows = Vec::new();
	for frame in &db.query(&format!("CALL rql::explain('{rql}')")) {
		for r in 0..frame.row_count() {
			let kind = frame.get::<String>("kind", r).expect("kind").expect("kind defined");
			let detail = frame.get::<String>("detail", r).expect("detail").expect("detail defined");
			rows.push((kind, detail));
		}
	}
	rows
}

fn filter_detail(db: &TestDb, rql: &str) -> Option<String> {
	plan(db, rql).into_iter().find(|(kind, _)| kind == "Filter").map(|(_, detail)| detail)
}

fn collect_n(db: &TestDb, rql: &str) -> Vec<i32> {
	let mut out = Vec::new();
	for frame in &db.query(rql) {
		for r in 0..frame.row_count() {
			out.push(frame.get::<i32>("n", r).expect("get n").expect("n defined"));
		}
	}
	out.sort();
	out
}

fn partitioned_db() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin(
		"create series test::s { ts: int8, region: utf8, n: int4 } with { key: ts, partition: { by: { region } } }",
	);
	db.command(
		"insert test::s [{ ts: 10, region: \"us\", n: 1 }, { ts: 20, region: \"us\", n: 2 }, \
		 { ts: 30, region: \"us\", n: 3 }, { ts: 20, region: \"eu\", n: 4 }]",
	);
	db
}

#[test]
fn a_partitioned_series_reads_only_the_matching_key_span() {
	// The partitioned rewrite used to discard the key bounds it had just extracted, so a pruned
	// partition was still swept end to end and every key comparison was redone row by row. The
	// bounds only genuinely reach the scan range if the surviving filter stops mentioning them.
	let db = partitioned_db();
	let rql = "from test::s filter region == \"us\" and ts >= 20";

	let detail = filter_detail(&db, rql).expect("the partition predicate must survive as a filter");
	assert!(!detail.contains("ts >="), "the key bound belongs in the scan range, not in the filter: {detail}");
	assert!(detail.contains("region"), "the partition predicate must stay as a filter: {detail}");

	assert_eq!(collect_n(&db, rql), vec![2, 3], "the pushed key span must keep exactly the in-range us rows");
}

#[test]
fn a_pushed_key_span_never_crosses_into_another_partition() {
	// The span is built inside one partition prefix; a span bounded on the key alone would sweep
	// the neighbouring partition's row at the same key into the answer.
	let db = partitioned_db();

	assert_eq!(
		collect_n(&db, "from test::s filter region == \"us\" and ts >= 20 and ts <= 20"),
		vec![2],
		"only the us row at ts 20 may come back, never the eu row at the same key"
	);
}

#[test]
fn a_partitioned_series_without_a_partition_predicate_still_answers_every_partition() {
	// The partition sits ahead of the key in the encoded layout, so key bounds cannot be pushed
	// without one. Refusing must fall back to a full scan plus filter, not to one partition.
	let db = partitioned_db();
	let rql = "from test::s filter ts >= 20";

	let detail = filter_detail(&db, rql).expect("without a partition the key bound has to stay a filter");
	assert!(detail.contains("ts >="), "the key comparison must still be evaluated per row: {detail}");

	assert_eq!(collect_n(&db, rql), vec![2, 3, 4], "every partition must contribute its in-range rows");
}

fn tagged_db() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin("create enum test::kind { Alpha, Beta }");
	db.admin("create series test::t { ts: int8, n: int4 } with { key: ts, tag: test::kind }");
	db.command(
		"insert test::t [{ ts: 10, tag: 0, n: 1 }, { ts: 20, tag: 0, n: 2 }, \
		 { ts: 20, tag: 1, n: 3 }, { ts: 30, tag: 1, n: 4 }]",
	);
	db
}

#[test]
fn a_tagged_series_refuses_key_pushdown_without_a_tag_predicate() {
	// A key-bounded range covers exactly one tag class, so pushing key bounds with no tag pinned
	// would silently drop every other tag's rows. The planner must decline and leave the
	// comparison to the filter.
	let db = tagged_db();
	let rql = "from test::t filter ts >= 20";

	let detail = filter_detail(&db, rql).expect("the refused key bound must remain a filter");
	assert!(detail.contains("ts >="), "a tagged series with no tag predicate keeps the comparison: {detail}");

	assert_eq!(collect_n(&db, rql), vec![2, 3, 4], "rows of every tag inside the key window must survive");
}

#[test]
fn a_tagged_series_pushes_the_key_span_once_a_tag_is_pinned() {
	// The refusal is about the missing tag, not about tagged series as such: with a tag pinned
	// the span covers a single tag class and nothing is left for a filter to re-check.
	let db = tagged_db();
	let rql = "from test::t filter tag == 1 and ts >= 20";

	assert_eq!(filter_detail(&db, rql), None, "a tag-pinned key bound leaves no predicate behind");

	assert_eq!(collect_n(&db, rql), vec![3, 4], "only the pinned tag's in-range rows may come back");
}
