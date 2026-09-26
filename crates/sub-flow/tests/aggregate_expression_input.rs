// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;
use reifydb_value::value::duration::Duration;

const TIMEOUT: Duration = Duration::from_seconds_const(5);

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, a: int4, b: Option(int4), c: Option(int4) }");
	db
}

fn text(db: &TestDb, rql: &str, name: &str) -> Vec<String> {
	let frames = db.query(rql);
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	column_values(&frames[0], name).iter().map(|v| v.to_string()).collect()
}

fn groups(db: &TestDb, view: &str) -> Vec<Vec<String>> {
	let rql = format!("FROM {view} | sort {{ g: ASC }}");
	let columns: Vec<Vec<String>> = ["g", "x", "n"].iter().map(|name| text(db, &rql, name)).collect();
	(0..columns[0].len()).map(|i| columns.iter().map(|column| column[i].clone()).collect()).collect()
}

#[test]
fn a_view_sum_over_an_expression_whose_operand_is_none_in_every_row_matches_a_view_over_a_none_column() {
	// An all-none operand that breaks the expression stalls the flow, so populated group 2 never lands.
	let db = setup();
	db.admin(
		"CREATE DEFERRED VIEW app::by_expr { g: int4, x: Option(int8), n: int8 } AS { FROM app::t | aggregate { x: math::sum(a - b), n: math::count(a - b) } by { g } }",
	);
	db.admin(
		"CREATE DEFERRED VIEW app::by_col { g: int4, x: Option(int4), n: int8 } AS { FROM app::t | aggregate { x: math::sum(c), n: math::count(c) } by { g } }",
	);

	db.command("INSERT app::t [{ g: 1, a: 10, b: none, c: none }, { g: 1, a: 5, b: none, c: none }]");
	db.command("INSERT app::t [{ g: 2, a: 10, b: 3, c: 7 }]");
	assert!(db.await_all_flows(TIMEOUT), "both flows must process every insert");

	let by_col = groups(&db, "app::by_col");
	assert!(by_col.contains(&vec!["2".to_string(), "7".to_string(), "1".to_string()]), "got: {by_col:?}");
	assert_eq!(groups(&db, "app::by_expr"), by_col, "sum and count over a - b must match sum and count over c");
}

#[test]
fn a_view_sum_over_an_expression_after_a_filter_that_removes_every_row_keeps_serving_later_rows() {
	// A flow that fails on the emptied batch never applies the later passing row, so the view stays empty.
	let db = setup();
	db.admin(
		"CREATE DEFERRED VIEW app::v { g: int4, x: Option(int8) } AS { FROM app::t | filter { a > 1000 } | aggregate { x: math::sum(a - b) } by { g } }",
	);

	db.command("INSERT app::t [{ g: 1, a: 10, b: 3, c: none }, { g: 2, a: 5, b: none, c: none }]");
	assert!(db.await_all_flows(TIMEOUT), "the flow must process the batch the filter empties");
	assert_eq!(db.row_count("FROM app::v"), 0, "no row passes the filter, so the view must have no group");

	db.command("INSERT app::t [{ g: 3, a: 2000, b: 1, c: none }]");
	db.await_exact_row_count("FROM app::v", 1, TIMEOUT);

	assert_eq!(text(&db, "FROM app::v", "x"), vec!["1999"], "the later passing row must be summed as a - b");
}
