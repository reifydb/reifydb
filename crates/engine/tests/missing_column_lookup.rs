// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{fragment::Fragment, params::Params, value::value_type::ValueType};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, a: int4, b: Option(int4) }");
	t.command("INSERT test::t [{ g: 1, a: 10, b: 3 }, { g: 2, a: 5, b: none }, { g: 2, a: 7, b: none }]");
	t
}

fn assert_column_not_found(t: &TestEngine, rql: &str, column: &str) {
	// A Fragment::None or internal fragment would lose the position a client needs to point at the typo.
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	let Some(err) = result.error else {
		panic!("a missing column must be an error, not a result of none or dropped rows\nrql: {rql}");
	};

	assert_eq!(err.code, "QUERY_001", "rql: {rql}, got: {err:?}");
	assert_eq!(err.fragment.text(), column, "the error must name the missing column\nrql: {rql}");
	assert!(matches!(err.fragment, Fragment::Statement { .. }), "rql: {rql}, got: {:?}", err.fragment);

	let offset = rql.rfind(column).unwrap();
	assert_eq!(*err.fragment.column() as usize, offset + 1, "the fragment must point at the column\nrql: {rql}");
}

#[test]
fn a_typo_in_map_reports_column_not_found() {
	// A none column for the typo would pass as a valid result, so the lookup must fail instead.
	let t = engine();

	assert_column_not_found(&t, "FROM test::t | map { x: typo }", "typo");
}

#[test]
fn a_typo_in_extend_reports_column_not_found() {
	// Extend keeps the input columns, so a none column for the typo would pass as a valid extra column.
	let t = engine();

	assert_column_not_found(&t, "FROM test::t | extend { y: typo }", "typo");
}

#[test]
fn a_typo_in_filter_reports_column_not_found() {
	// A none predicate matches no row, so a typo must fail rather than silently drop every row.
	let t = engine();

	assert_column_not_found(&t, "FROM test::t | filter { typo == 1 }", "typo");
}

#[test]
fn a_column_dropped_by_an_earlier_map_reports_column_not_found() {
	// The second map only sees x, so reading a must fail rather than reach back to a column of none.
	let t = engine();

	assert_column_not_found(&t, "FROM test::t | map { x: a - b } | map { y: a }", "a");
}

#[test]
fn a_typo_inside_a_function_argument_reports_column_not_found() {
	// A function argument goes through the same lookup, so a typo there must fail before the function runs.
	let t = engine();

	assert_column_not_found(&t, "FROM test::t | map { x: math::abs(typo) }", "typo");
}

#[test]
fn a_typo_inside_an_aggregate_argument_expression_reports_column_not_found() {
	// Only a bare column argument is resolved by the aggregate node, so an expression argument must fail too.
	let t = engine();

	assert_column_not_found(&t, "FROM test::t | aggregate { n: math::sum(a + typo) } by { g }", "typo");
}

#[test]
fn a_typo_in_a_map_without_input_reports_column_not_found() {
	// Without an input there are no columns at all, so every bare name must fail rather than yield one none row.
	let t = engine();

	assert_column_not_found(&t, "MAP { x: typo }", "typo");
}

#[test]
fn an_insert_returning_a_pre_image_column_gives_none_typed_like_the_stored_column() {
	// An insert has no pre-image, so pre_ columns must resolve to none of the column type rather than not found.
	let t = engine();

	let frames = t.command("INSERT test::t [{ g: 9, a: 1, b: 2 }] RETURNING { g, pre_a }");

	assert_eq!(TestEngine::row_count(&frames), 1);
	let pre_a = frames[0].columns.iter().find(|c| c.name == "pre_a").expect("pre_a column");
	assert_eq!(pre_a.data.as_string(0), "none");
	assert_eq!(pre_a.data.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
}

#[test]
fn a_dictionary_insert_that_stores_nothing_still_returns_typed_columns() {
	// An all-none input interns nothing, and the empty result must still carry id and value for RETURNING.
	let t = engine();
	t.admin("CREATE DICTIONARY test::codes FOR utf8 AS uint4");

	let frames = t.command("INSERT test::codes [{ value: none }] RETURNING { id, value }");

	assert_eq!(TestEngine::row_count(&frames), 0);
	let types: Vec<(String, ValueType)> =
		frames[0].columns.iter().map(|c| (c.name.clone(), c.data.get_type())).collect();
	assert_eq!(types, vec![("id".to_string(), ValueType::Uint4), ("value".to_string(), ValueType::Utf8)]);
}
