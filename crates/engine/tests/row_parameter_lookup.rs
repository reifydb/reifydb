// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::Value};

const SOURCES: [&str; 2] = ["test::t", "test::r"];

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");
	t.admin("CREATE RINGBUFFER test::r { id: int4 } WITH { capacity: 10 }");
	for source in SOURCES {
		t.command(&format!("INSERT {source} [{{ id: 10 }}, {{ id: 20 }}, {{ id: 30 }}, {{ id: 40 }}]"));
	}
	t
}

#[test]
fn a_rownum_parameter_answers_the_rows_its_value_names() {
	// The run-time lookup must read exactly the rows the parameter names, never another row or all rows.
	let t = engine();
	for source in SOURCES {
		let rows = format!("FROM {source} | filter {{ #rownum == $1 }} | sort {{ id: ASC }} | map {{ v: id }}");
		assert_eq!(run(&t, &rows, positional(vec![Value::Int4(2)])), ids(&["20"]), "{source}");
		assert_eq!(run(&t, &rows, positional(vec![Value::Uint8(4)])), ids(&["40"]), "{source}");
		assert_eq!(run(&t, &rows, positional(vec![Value::Uint8(9)])), ids(&[]), "{source}");

		let list = format!(
			"FROM {source} | filter {{ #rownum in [$1, 3] }} | sort {{ id: ASC }} | map {{ v: id }}"
		);
		assert_eq!(run(&t, &list, positional(vec![Value::Int8(1)])), ids(&["10", "30"]), "{source}");

		let range = format!(
			"FROM {source} | filter {{ #rownum between $1 and $2 }} | sort {{ id: ASC }} | map {{ v: id }}"
		);
		assert_eq!(
			run(&t, &range, positional(vec![Value::Uint1(2), Value::Uint1(3)])),
			ids(&["20", "30"]),
			"{source}"
		);
	}
}

#[test]
fn a_named_rownum_parameter_answers_its_own_value() {
	// A named parameter must resolve by name, otherwise it reads the wrong value or none.
	let t = engine();
	let rows = "FROM test::t | filter { #rownum == $id } | sort { id: ASC } | map { v: id }";
	let named = Params::Named(Arc::new(HashMap::from([("id".to_string(), Value::Uint2(3))])));
	assert_eq!(run(&t, rows, named), ids(&["30"]));
}

#[test]
fn a_rownum_parameter_answers_exactly_what_the_plain_filter_answers() {
	// The wrapped filter is never a row predicate, so any difference means the lookup changed an answer or an
	// error.
	let t = engine();
	let cases: [Value; 7] = [
		Value::Int4(2),
		Value::Int4(-1),
		Value::Int4(0),
		Value::Utf8("abc".to_string()),
		Value::none(),
		Value::float8(2.0),
		Value::Uint8(u64::MAX),
	];
	for source in SOURCES {
		for value in &cases {
			for filter in [
				"#rownum == $1",
				"#rownum in [$1, 3]",
				"#rownum between $1 and 3",
				"#rownum between 3 and $1",
			] {
				let lookup = run(
					&t,
					&format!(
						"FROM {source} | filter {{ {filter} }} | sort {{ id: ASC }} | map {{ v: id }}"
					),
					positional(vec![value.clone()]),
				);
				let plain = run(
					&t,
					&format!(
						"FROM {source} | filter {{ ({filter}) == true }} | sort {{ id: ASC }} | map {{ v: id }}"
					),
					positional(vec![value.clone()]),
				);
				assert_eq!(lookup, plain, "{source} {filter} with {value:?}");
			}
		}
	}
}

#[test]
fn a_missing_rownum_parameter_raises_the_same_error_as_a_plain_filter() {
	// The fallback must surface the missing parameter, never answer no rows.
	let t = engine();
	let lookup = run(&t, "FROM test::t | filter { #rownum == $1 } | map { v: id }", Params::None);
	let plain = run(&t, "FROM test::t | filter { id == $1 } | map { v: id }", Params::None);
	assert!(matches!(lookup, Answer::Error(..)), "{lookup:?}");
	assert_eq!(lookup, plain);
}

#[test]
fn a_negated_rownum_list_excludes_the_listed_rows() {
	// A row lookup of the listed rows would return exactly the rows not in must drop.
	let t = engine();
	for source in SOURCES {
		let constants = format!(
			"FROM {source} | filter {{ #rownum not in [1, 2] }} | sort {{ id: ASC }} | map {{ v: id }}"
		);
		let parameters = format!(
			"FROM {source} | filter {{ #rownum not in [$1, 2] }} | sort {{ id: ASC }} | map {{ v: id }}"
		);
		assert_eq!(run(&t, &constants, Params::None), ids(&["30", "40"]), "{source}");
		assert_eq!(run(&t, &parameters, positional(vec![Value::Int4(1)])), ids(&["30", "40"]), "{source}");
	}
}

#[test]
fn a_rownum_range_to_the_largest_row_number_answers_the_rows_that_exist() {
	// A range end far past the last row must stop at the last row, never walk every empty row number.
	let t = engine();
	for source in SOURCES {
		let rows = format!(
			"FROM {source} | filter {{ #rownum between 3 and 18446744073709551615 }} | sort {{ id: ASC }} | map {{ v: id }}"
		);
		assert_eq!(run(&t, &rows, Params::None), ids(&["30", "40"]), "{source}");
	}
}

#[derive(Debug, PartialEq)]
enum Answer {
	Rows(Vec<String>),
	Error(String, String),
}

fn positional(values: Vec<Value>) -> Params {
	Params::Positional(Arc::new(values))
}

fn ids(values: &[&str]) -> Answer {
	Answer::Rows(values.iter().map(|v| v.to_string()).collect())
}

fn run(t: &TestEngine, rql: &str, params: Params) -> Answer {
	let result = t.inner().query_as(TestEngine::identity(), rql, params);
	if let Some(err) = result.error {
		let diagnostic = err.diagnostic();
		return Answer::Error(diagnostic.code.clone(), diagnostic.message.clone());
	}
	let column = result.frames.last().expect("a frame").column("v").unwrap().expect("column v");
	Answer::Rows((0..column.len()).map(|i| column.get_value(i).to_string()).collect())
}
