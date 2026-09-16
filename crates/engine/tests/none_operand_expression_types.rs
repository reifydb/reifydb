// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{frame::frame::Frame, value_type::ValueType};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, a: int4, b: Option(int4) }");
	t.command("INSERT test::t [{ g: 1, a: 10, b: 3 }, { g: 2, a: 5, b: none }, { g: 2, a: 7, b: none }]");
	t
}

fn column_type(frames: &[Frame], name: &str) -> ValueType {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	column.data.get_type()
}

fn assert_same_type_as_populated(t: &TestEngine, populated: &str, other: &str, rows: usize) {
	// A Boolean on both sides would pass the equality, so the populated type itself must be the real one.
	let expected = column_type(&t.query(populated), "x");
	assert_ne!(expected, ValueType::Option(Box::new(ValueType::Boolean)), "populated query: {populated}");
	assert_ne!(expected, ValueType::Boolean, "populated query: {populated}");

	let frames = t.query(other);
	assert_eq!(TestEngine::row_count(&frames), rows, "query: {other}");
	assert_eq!(column_type(&frames, "x"), expected, "query: {other}");
}

#[test]
fn a_difference_over_an_empty_result_has_the_type_a_populated_result_has() {
	// An empty batch reports every optional operand as none, which used to type the result as Boolean.
	let t = engine();

	assert_same_type_as_populated(
		&t,
		"FROM test::t | filter { g == 1 } | map { x: a - b }",
		"FROM test::t | filter { a > 1000 } | map { x: a - b }",
		0,
	);
}

#[test]
fn a_difference_whose_operand_is_none_in_every_row_has_the_type_a_populated_result_has() {
	// A batch that happens to hold only none must not change the column type a client sees for the same query.
	let t = engine();

	assert_same_type_as_populated(
		&t,
		"FROM test::t | filter { g == 1 } | map { x: a - b }",
		"FROM test::t | filter { g == 2 } | map { x: a - b }",
		2,
	);
}

#[test]
fn a_negation_whose_operand_is_none_in_every_row_has_the_type_a_populated_result_has() {
	// The unary path has its own all-none shortcut and must derive the type the same way the binary path does.
	let t = engine();

	assert_same_type_as_populated(
		&t,
		"FROM test::t | filter { g == 1 } | map { x: -b }",
		"FROM test::t | filter { g == 2 } | map { x: -b }",
		2,
	);
}

#[test]
fn a_vectorized_udf_over_an_empty_result_has_the_type_a_populated_result_has() {
	// A UDF node that skips the empty batch leaves its result column missing, which the lookup turns into Boolean.
	let t = engine();

	assert_same_type_as_populated(
		&t,
		"UDF twice ($x: int) { RETURN $x * 2 }; FROM test::t | filter { g == 1 } | map { x: twice(a) }",
		"UDF twice ($x: int) { RETURN $x * 2 }; FROM test::t | filter { a > 1000 } | map { x: twice(a) }",
		0,
	);
}

#[test]
fn an_operand_that_is_a_literal_none_still_gives_none() {
	// A literal none has no type to derive from, so it must keep yielding none rather than a type error.
	let t = engine();

	let frames = t.query("FROM test::t | filter { g == 1 } | map { x: a - none }");

	assert_eq!(TestEngine::row_count(&frames), 1);
	assert_eq!(frames[0].columns[0].data.as_string(0), "none");
}
