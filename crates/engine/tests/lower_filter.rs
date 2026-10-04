// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	fragment::{StatementColumn, StatementLine},
	params::Params,
};

fn query_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
	}
}

#[test]
fn a_type_mismatch_on_an_all_none_column_points_at_the_compare() {
	// The error must carry the compare's own text, or the user cannot find the bad compare in the query.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: Option(int4) }");
	t.command("INSERT test::t [{ a: none }, { a: none }]");

	let diagnostic = query_err(&t, "FROM test::t FILTER a == \"x\"");

	assert_eq!(diagnostic.code, "OPERATOR_022", "got: {diagnostic:?}");
	assert_eq!(diagnostic.fragment.text(), "a==x", "got: {diagnostic:?}");
	assert_eq!(diagnostic.fragment.line(), StatementLine(1), "got: {diagnostic:?}");
	assert_eq!(diagnostic.fragment.column(), StatementColumn(21), "got: {diagnostic:?}");
}

#[test]
fn a_deciding_left_side_no_longer_hides_a_type_error_on_the_right() {
	// The right side is type checked before any row runs, so an all false left side cannot skip it.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: bool }");
	t.command("INSERT test::t [{ a: 1, b: false }, { a: 2, b: false }]");

	let diagnostic = query_err(&t, "FROM test::t FILTER b and a == \"x\"");

	assert_eq!(diagnostic.code, "OPERATOR_022", "got: {diagnostic:?}");
}

#[test]
fn between_over_a_nullable_column_keeps_the_rows_in_range() {
	// A nullable column used to make between fail; now each row is kept or dropped on its own value.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: Option(int4) }");
	t.command("INSERT test::t [{ a: 1 }, { a: 5 }, { a: none }]");

	let frames = t.query("FROM test::t FILTER a between 2 and 10");

	assert_eq!(TestEngine::row_count(&frames), 1);
	let values: Vec<Option<i32>> = frames[0].rows().map(|r| r.get::<i32>("a").unwrap()).collect();
	assert_eq!(values, vec![Some(5)]);
}
