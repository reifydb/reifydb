// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const UDF: &str = "UDF m ($x) { RETURN not $x };";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, b: bool }");
	t.command("INSERT test::t [{ id: 1, b: true }, { id: 2, b: false }]");
	t
}

fn columns(t: &TestEngine, rql: &str) -> Vec<(String, Vec<String>)> {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		panic!("{rql}: {}", err.diagnostic().message);
	}
	let frame = result.frames.last().expect("a frame");
	frame.batch
		.schema()
		.fields()
		.iter()
		.zip(frame.batch.columns())
		.filter(|(field, _)| !field.name().starts_with('#'))
		.map(|(field, _)| {
			let column = frame.column(field.name()).unwrap().expect("column");
			(field.name().clone(), (0..column.len()).map(|i| column.get_value(i).to_string()).collect())
		})
		.collect()
}

#[test]
fn a_function_call_without_alias_in_map_keeps_its_column_named_after_the_call() {
	// A call with no alias must stay in the output under its own name, otherwise its column silently vanishes.
	let t = engine();
	assert_eq!(
		columns(&t, &format!("{UDF} FROM test::t | sort {{ id: ASC }} | map {{ id, m(b) }}")),
		vec![
			("id".to_string(), vec!["1".to_string(), "2".to_string()]),
			("m(b)".to_string(), vec!["false".to_string(), "true".to_string()]),
		]
	);
}

#[test]
fn a_function_call_without_alias_in_extend_adds_its_column_named_after_the_call() {
	// Extend must add the call under its own name, never clash with the internal column that holds its result.
	let t = engine();
	assert_eq!(
		columns(&t, &format!("{UDF} FROM test::t | sort {{ id: ASC }} | extend {{ m(b) }}")),
		vec![
			("id".to_string(), vec!["1".to_string(), "2".to_string()]),
			("b".to_string(), vec!["true".to_string(), "false".to_string()]),
			("m(b)".to_string(), vec!["false".to_string(), "true".to_string()]),
		]
	);
}

#[test]
fn a_function_call_without_alias_and_without_input_is_named_after_the_call() {
	// With no input rows the call must still carry its own name, never the internal result column name.
	let t = engine();
	assert_eq!(
		columns(&t, &format!("{UDF} map {{ m(true) }}")),
		vec![("m(true)".to_string(), vec!["false".to_string()])]
	);
}

#[test]
fn a_function_call_inside_a_larger_expression_is_named_after_the_whole_expression() {
	// The header must spell the expression the user wrote, otherwise the internal column name leaks into it.
	let t = engine();
	let names: Vec<String> = columns(&t, &format!("{UDF} FROM test::t | map {{ not m(b) }}"))
		.into_iter()
		.map(|(name, _)| name)
		.collect();
	let builtin: Vec<String> = columns(&t, "FROM test::t | map { not b }")
		.into_iter()
		.map(|(name, _)| name.replace('b', "m(b)"))
		.collect();
	assert_eq!(names, builtin);
}
