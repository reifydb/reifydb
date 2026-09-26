// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, a: int4, b: Option(int4), c: Option(int4), d: Option(int4) }");
	t.command(
		"INSERT test::t [
			{ g: 1, a: 10, b: 3, c: 7, d: 7 },
			{ g: 1, a: 5, b: 8, c: -3, d: 3 },
			{ g: 2, a: 4, b: 1, c: 3, d: 3 },
			{ g: 2, a: 6, b: none, c: none, d: none }
		]",
	);
	t
}

fn empty_engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, a: int4, b: int4 }");
	t
}

fn column_text(frames: &[Frame], name: &str) -> Vec<String> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	(0..column.data.len()).map(|i| column.data.as_string(i)).collect()
}

fn rows(frames: &[Frame], names: &[&str]) -> Vec<Vec<String>> {
	let columns: Vec<Vec<String>> = names.iter().map(|name| column_text(frames, name)).collect();
	let row_count = columns.first().map_or(0, Vec::len);
	let mut rows: Vec<Vec<String>> =
		(0..row_count).map(|i| columns.iter().map(|column| column[i].clone()).collect()).collect();
	rows.sort();
	rows
}

fn strings(values: &[&str]) -> Vec<String> {
	values.iter().map(|v| v.to_string()).collect()
}

#[test]
fn sum_over_an_expression_equals_sum_over_a_column_holding_it_per_group() {
	// An expression input read from the wrong rows, or with none counted, drifts from the stored column.
	let t = engine();

	let frames = t.query(
		"FROM test::t | aggregate { x: math::sum(a - b), y: math::sum(c), n: math::count(a - b), m: math::count(c) } by { g }",
	);

	assert_eq!(
		rows(&frames, &["g", "x", "y", "n", "m"]),
		vec![strings(&["1", "4", "4", "2", "2"]), strings(&["2", "3", "3", "1", "1"])],
		"each group must sum and count a - b exactly as it sums and counts c, skipping the none row"
	);
}

#[test]
fn sum_over_an_expression_equals_sum_over_a_column_holding_it_without_by() {
	// Without by every row is one group, so a batch-local evaluation that loses rows shows up here.
	let t = engine();

	let frames = t.query("FROM test::t | aggregate { x: math::sum(a - b), y: math::sum(c) }");

	assert_eq!(
		rows(&frames, &["x", "y"]),
		vec![strings(&["7", "7"])],
		"sum(a - b) must equal sum(c) over all rows"
	);
}

#[test]
fn an_expression_input_that_calls_a_builtin_function_is_evaluated_per_row() {
	// Applying abs after summing gives |4| = 4 for group 1 instead of 10, a plausible wrong answer.
	let t = engine();

	let frames = t.query("FROM test::t | aggregate { x: math::sum(math::abs(a - b)), y: math::sum(d) } by { g }");

	assert_eq!(
		rows(&frames, &["g", "x", "y"]),
		vec![strings(&["1", "10", "10"]), strings(&["2", "3", "3"])],
		"sum(abs(a - b)) must equal sum(d) where d holds abs(a - b)"
	);
}

#[test]
fn an_expression_input_that_calls_a_udf_is_hoisted_and_evaluated_per_row() {
	// A UDF call left in the argument reaches the builtin evaluator, which asserts it was hoisted and panics.
	let t = engine();

	let frames = t.query(r#"
			UDF twice ($x: int4) { RETURN $x * 2 };
			FROM test::t | aggregate { x: math::sum(twice(a)), y: math::sum(a) } by { g }
		"#);

	assert_eq!(
		rows(&frames, &["g", "x", "y"]),
		vec![strings(&["1", "30", "15"]), strings(&["2", "20", "10"])],
		"sum(twice(a)) must be twice sum(a) in every group"
	);
}

#[test]
fn each_expression_input_feeds_its_own_accumulator() {
	// Swapped input indexes hand one accumulator another call's values with no error to notice.
	let t = engine();

	let frames = t.query(
		"FROM test::t | aggregate { x: math::sum(a - b), top: math::max(a + 100), s: math::sum(a) - math::sum(b * 2) } by { g }",
	);

	assert_eq!(
		rows(&frames, &["g", "x", "top", "s"]),
		vec![strings(&["1", "4", "110", "-7"]), strings(&["2", "3", "106", "8"])],
		"every aggregate must read the values of its own argument"
	);
}

#[test]
fn an_expression_input_after_a_filter_that_removes_every_row_gives_no_groups() {
	// A filter that empties the input still sends an empty batch, which the expression must survive.
	let t = engine();

	let frames = t.query("FROM test::t | filter { a > 1000 } | aggregate { x: math::sum(a - b) } by { g }");

	assert_eq!(TestEngine::row_count(&frames), 0, "no row passes the filter, so there must be no group");
}

#[test]
fn a_udf_expression_input_after_a_filter_that_removes_every_row_gives_no_groups() {
	// The UDF node skips empty batches, so its result column is missing when the aggregate evaluates the input.
	let t = engine();

	let frames = t.query(r#"
			UDF twice ($x: int4) { RETURN $x * 2 };
			FROM test::t | filter { a > 1000 } | aggregate { x: math::sum(twice(a)) } by { g }
		"#);

	assert_eq!(TestEngine::row_count(&frames), 0, "no row passes the filter, so there must be no group");
}

#[test]
fn an_expression_over_an_operand_that_is_none_in_every_row_matches_a_column_that_is_none_in_every_row() {
	// An all-none batch must still carry the expression's type, or sum rejects it while sum over the column works.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::n { g: int4, a: int4, b: Option(int4), c: Option(int4) }");
	t.command("INSERT test::n [{ g: 1, a: 10, b: none, c: none }, { g: 1, a: 5, b: none, c: none }]");

	let frames = t.query(
		"FROM test::n | aggregate { x: math::sum(a - b), y: math::sum(c), n: math::count(a - b), m: math::count(c) } by { g }",
	);

	let result = rows(&frames, &["g", "x", "y", "n", "m"]);
	assert_eq!(result.len(), 1, "one group must come back, got: {result:?}");
	assert_eq!(result[0][1], result[0][2], "sum(a - b) must equal sum(c) when every b and c is none");
	assert_eq!(result[0][3], "0", "count(a - b) must skip every none row");
	assert_eq!(result[0][4], "0", "count(c) must skip every none row");
}

#[test]
fn a_column_extra_argument_fails_before_any_row_is_read() {
	// On an empty table no row is ever read, so this error must come from the call shape alone.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { s: math::sum(a, b) } by { g }");

	assert!(err.contains("FUNCTION_003"), "math::sum(a, b) must report FUNCTION_003 on an empty table, got: {err}");
}

#[test]
fn a_literal_extra_argument_to_an_aggregate_without_literal_slots_fails_before_any_row_is_read() {
	// Passing the literal through would silently return sum(a) and drop the 1 the caller wrote.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { s: math::sum(a, 1) } by { g }");

	assert!(err.contains("FUNCTION_003"), "math::sum(a, 1) must report FUNCTION_003 on an empty table, got: {err}");
}
