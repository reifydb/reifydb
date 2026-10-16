// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

#[test]
fn between_over_a_nullable_column_answers_per_row_in_an_extend() {
	// An extend over a table must answer between per row, otherwise a none row fails the whole batch.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: Option(int4) }");
	t.command("INSERT test::t [{ a: 1 }, { a: 5 }, { a: none }]");

	let frames = t.query("FROM test::t | extend { b: a between 2 and 10 }");

	let mut rows: Vec<(Option<i32>, Option<bool>)> =
		frames[0].rows().map(|r| (r.get::<i32>("a").unwrap(), r.get::<bool>("b").unwrap())).collect();
	rows.sort();
	assert_eq!(rows, vec![(None, None), (Some(1), Some(false)), (Some(5), Some(true))]);
}

#[test]
fn an_extend_column_reads_an_earlier_new_column_on_the_lowered_path() {
	// A later extend column must see the earlier one, otherwise chained extends break.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: Option(int4) }");
	t.command("INSERT test::t [{ a: 1 }, { a: 5 }, { a: none }]");

	let frames = t.query("FROM test::t | extend { b: a + 1, c: b between 2 and 10 }");

	let mut rows: Vec<(Option<i32>, Option<bool>)> =
		frames[0].rows().map(|r| (r.get::<i32>("a").unwrap(), r.get::<bool>("c").unwrap())).collect();
	rows.sort();
	assert_eq!(rows, vec![(None, None), (Some(1), Some(true)), (Some(5), Some(true))]);
}

#[test]
fn between_with_a_none_bound_follows_kleene_in_an_extend_with_no_input() {
	// An extend with no input must give none for a none bound, never an error.
	let t = TestEngine::new();

	let frames = t.query("EXTEND { b: 1 between none and 3 }");

	let values: Vec<Option<bool>> = frames[0].rows().map(|r| r.get::<bool>("b").unwrap()).collect();
	assert_eq!(values, vec![None]);
}
