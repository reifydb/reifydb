// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE TABLE test::t { id: int4, a: Option(int4), b: Option(bool), g: Option(float8), f: Option(float8) }",
	);
	t.command(
		"INSERT test::t [{ id: 1, a: 1, b: none, g: 1.5, f: none }, { id: 2, a: 5, b: none, g: none, f: none }, { id: 3, a: none, b: none, g: -2.0, f: none }]",
	);
	t
}

fn stored_b(t: &TestEngine) -> Vec<(i32, Option<bool>)> {
	let frames = t.query("FROM test::t");
	let mut rows: Vec<(i32, Option<bool>)> =
		frames[0].rows().map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<bool>("b").unwrap())).collect();
	rows.sort();
	rows
}

#[test]
fn between_over_a_nullable_column_answers_per_row_in_a_patch() {
	// A patch must take the lowered path, where between answers per row while the old path errors.
	let t = engine();

	let frames = t.query("FROM test::t | patch { b: a between 2 and 10 }");

	let mut rows: Vec<(i32, Option<bool>)> =
		frames[0].rows().map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<bool>("b").unwrap())).collect();
	rows.sort();
	assert_eq!(rows, vec![(1, Some(false)), (2, Some(true)), (3, None)]);
}

#[test]
fn an_update_stores_a_between_over_a_nullable_column_per_row() {
	// An update computes its values through the patch, so the lowered answer must be what lands in storage.
	let t = engine();

	t.command("UPDATE test::t { b: a between 2 and 10 } FILTER id > 0");

	assert_eq!(stored_b(&t), vec![(1, Some(false)), (2, Some(true)), (3, None)]);
}

#[test]
fn an_update_stores_a_negated_float_column() {
	// A wrong negation would be written to storage and survive every later read.
	let t = engine();

	t.command("UPDATE test::t { f: -g } FILTER id > 0");

	let frames = t.query("FROM test::t");
	let mut rows: Vec<(i32, Option<f64>)> =
		frames[0].rows().map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<f64>("f").unwrap())).collect();
	rows.sort_by_key(|(id, _)| *id);
	assert_eq!(rows, vec![(1, Some(-1.5)), (2, None), (3, Some(2.0))]);
}
