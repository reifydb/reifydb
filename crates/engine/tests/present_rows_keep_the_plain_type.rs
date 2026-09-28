// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{frame::frame::Frame, value_type::ValueType};

fn column_type(frames: &[Frame], name: &str) -> ValueType {
	let frame = frames.last().expect("at least one frame");
	let column = frame
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frame.columns));
	column.data.get_type()
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: int4 }");
	t.command("INSERT test::t [{ a: 1, b: 10 }, { a: 2, b: 20 }]");
	t
}

#[test]
fn inline_rows_inserted_into_a_table_keep_the_declared_plain_type() {
	// Every row gives a value, so the column must stay int4, never widen to Option(int4).
	let t = engine();

	let frames = t.command("INSERT test::t [{ a: 3, b: 30 }] RETURNING { a }");

	assert_eq!(column_type(&frames, "a"), ValueType::Int4);
}

#[test]
fn a_loop_row_over_present_values_keeps_the_plain_type() {
	// A row variable built from a present value must carry the value's type, never Option of it.
	let t = engine();

	let frames = t.query("LET $rows = FROM test::t; FOR $r IN $rows { $r }");

	assert_eq!(column_type(&frames, "a"), ValueType::Int4);
}

#[test]
fn aggregate_keys_with_no_none_keep_the_plain_type() {
	// No group key is none, so the key column must stay int4, never widen to Option(int4).
	let t = engine();

	let frames = t.query("FROM test::t | aggregate { total: math::sum(b) } by { a }");

	assert_eq!(column_type(&frames, "a"), ValueType::Int4);
}

#[test]
fn an_untyped_udf_result_with_no_input_keeps_the_plain_type() {
	// A udf with no declared return type must give its value's type, never Option of it.
	let t = engine();

	let frames = t.query("UDF m ($x) { RETURN $x }; map { v: m(cast(7, int4)) }");

	assert_eq!(column_type(&frames, "v"), ValueType::Int4);
}
