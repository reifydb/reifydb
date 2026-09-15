// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame},
};

fn engine(schema: &str, rows: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(&format!("CREATE TABLE test::s {schema}"));
	t.command(&format!("INSERT test::s {rows}"));
	t
}

fn row_count(frames: &[Frame]) -> usize {
	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	frames[0].columns.first().map(|c| c.data.len()).unwrap_or(0)
}

fn column(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

fn query_error(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!(
			"expected an error, got:\n{}",
			r.frames.iter().map(|f| f.to_string()).collect::<String>()
		),
	}
}

#[test]
fn distinct_keeps_int_rows_whose_digits_only_match_when_concatenated() {
	// (1, 23) and (12, 3) render to the same digits once joined, but are different rows.
	let t = engine("{ a: int4, b: int4 }", "[{ a: 1, b: 23 }, { a: 12, b: 3 }]");

	assert_eq!(row_count(&t.query("FROM test::s | distinct {}")), 2, "all-columns key merged two rows");
	assert_eq!(row_count(&t.query("FROM test::s | distinct { a, b }")), 2, "named key merged two rows");
}

#[test]
fn distinct_keeps_mixed_type_rows_whose_text_only_matches_when_concatenated() {
	// An int next to a text renders "1" + "23" and "12" + "3" alike, so the key must carry each value's bounds.
	let t = engine("{ a: int4, b: utf8 }", r#"[{ a: 1, b: "23" }, { a: 12, b: "3" }]"#);

	assert_eq!(row_count(&t.query("FROM test::s | distinct { a, b }")), 2);
}

#[test]
fn distinct_merges_nones_into_one_row_exactly_as_group_by_groups_them() {
	// Distinct and `by` must agree on key identity, or a none splits into several rows in one and not the other.
	let t = engine(
		"{ id: int4, x: Option(utf8) }",
		r#"[{ id: 1, x: none }, { id: 2, x: none }, { id: 3, x: "none" }]"#,
	);

	let distinct = row_count(&t.query("FROM test::s | distinct { x }"));
	let groups = row_count(&t.query("FROM test::s | aggregate { n: math::count(id) } by { x }"));

	assert_eq!(distinct, 2, "two nones are one key and the text none is another");
	assert_eq!(distinct, groups, "distinct and group by disagree on key identity");
}

#[test]
fn distinct_keeps_the_first_row_of_each_key_in_input_order() {
	// Keying by typed values must not change which duplicate survives or the order rows come out in.
	let t = engine(
		"{ x: utf8, y: int4 }",
		r#"[{ x: "a", y: 1 }, { x: "b", y: 2 }, { x: "a", y: 3 }, { x: "c", y: 4 }, { x: "b", y: 5 }]"#,
	);
	let input = t.query("FROM test::s");
	let (xs, ys) = (column(&input, "x"), column(&input, "y"));
	let mut seen = Vec::new();
	let mut expected = Vec::new();
	for (x, y) in xs.iter().zip(ys.iter()) {
		if !seen.contains(x) {
			seen.push(x.clone());
			expected.push(y.clone());
		}
	}

	let kept = column(&t.query("FROM test::s | distinct { x }"), "y");

	assert_eq!(kept, expected, "distinct must keep the first row of each key, in input order");
}

#[test]
fn distinct_on_a_tuple_column_is_an_error_not_a_panic() {
	// The typed key encoding has no form for an any value, so reaching it would panic the query.
	let t = engine("{ x: utf8, y: utf8 }", r#"[{ x: "ab", y: "c" }, { x: "a", y: "bc" }]"#);

	let named = query_error(&t, "FROM test::s | map { t: (x, y) } | distinct { t }");
	let all = query_error(&t, "FROM test::s | map { t: [x, y] } | distinct {}");

	assert_eq!(named.code, "DISTINCT_001", "got: {named:?}");
	assert_eq!(all.code, "DISTINCT_001", "got: {all:?}");
}
