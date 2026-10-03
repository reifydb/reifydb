// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

#[test]
fn between_over_a_nullable_column_answers_per_row_in_a_map() {
	// A map over a table must take the lowered path, where between answers per row while the old path errors.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: Option(int4) }");
	t.command("INSERT test::t [{ a: 1 }, { a: 5 }, { a: none }]");

	let frames = t.query("FROM test::t | map { a, b: a between 2 and 10 }");

	let mut rows: Vec<(Option<i32>, Option<bool>)> =
		frames[0].rows().map(|r| (r.get::<i32>("a").unwrap(), r.get::<bool>("b").unwrap())).collect();
	rows.sort();
	assert_eq!(rows, vec![(None, None), (Some(1), Some(false)), (Some(5), Some(true))]);
}

#[test]
fn between_with_a_none_bound_follows_kleene_in_a_map_with_no_input() {
	// A map with no input must take the lowered path too, where a none bound gives none instead of an error.
	let t = TestEngine::new();

	let frames = t.query("MAP { b: 1 between none and 3 }");

	let values: Vec<Option<bool>> = frames[0].rows().map(|r| r.get::<bool>("b").unwrap()).collect();
	assert_eq!(values, vec![None]);
}
