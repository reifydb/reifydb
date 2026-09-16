// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{frame::frame::Frame, value_type::ValueType};

const LOOPY: &str =
	"UDF loopy ($x: int4): int4 { LET $i = 0; WHILE $i < 100 { IF $i >= $x { BREAK }; $i = $i + 1 }; RETURN $i }; ";
const TWICE: &str = "UDF twice ($x: int4): int2 { RETURN $x * 2 }; ";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, a: int4 }");
	t.command("INSERT test::t [{ g: 1, a: 10 }, { g: 2, a: 5 }]");
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

#[test]
fn a_udf_result_in_a_query_map_has_the_declared_return_type() {
	// A script call already casts to the declared type, so a map over rows must not hand back a different type.
	let t = engine();
	let script = column_type(&t.query(&format!("{LOOPY}let $v = loopy(3); map {{ v: $v }}")), "v");
	assert_eq!(script, ValueType::Int4, "the script path must honor the declared return type");

	let per_row = column_type(&t.query(&format!("{LOOPY}FROM test::t | map {{ x: loopy(a) }}")), "x");
	let vectorized = column_type(&t.query(&format!("{TWICE}FROM test::t | map {{ x: twice(a) }}")), "x");

	assert_eq!((per_row, vectorized), (ValueType::Int4, ValueType::Int2), "(per-row udf, vectorized udf)");
}

#[test]
fn a_non_vectorized_udf_over_an_empty_result_has_its_declared_return_type() {
	// A filter that removes every row must not turn the column into Any, which breaks every aggregate over it.
	let t = engine();

	let frames = t.query(&format!("{LOOPY}FROM test::t | filter {{ a > 1000 }} | map {{ x: loopy(a) }}"));

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "x"), ValueType::Int4);
}
