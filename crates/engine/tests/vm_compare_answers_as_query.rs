// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const OPERATORS: [&str; 6] = ["==", "!=", "<", "<=", ">", ">="];

const OPERANDS: [&str; 14] = [
	"2",
	"300",
	"cast(2, int8)",
	"cast(2, uint4)",
	"2.5",
	"cast(-0.0, float8)",
	"cast(0.0, float8)",
	"cast(2, float4)",
	"cast(1.5, decimal)",
	"\"a\"",
	"\"b\"",
	"true",
	"none",
	"cast(none, int4)",
];

#[test]
fn a_script_comparison_answers_exactly_what_the_same_comparison_answers_in_a_query() {
	// A script compare must give exactly the query's type, values or error, otherwise a LET and a map drift.
	let t = TestEngine::new();
	for op in OPERATORS {
		for left in OPERANDS {
			for right in OPERANDS {
				let operands = format!("LET $a = {left}; LET $b = {right};");
				let script = run(&t, &format!("{operands} LET $r = $a {op} $b; map {{ v: $r }}"));
				let query = run(&t, &format!("{operands} map {{ v: $a {op} $b }}"));
				assert_eq!(script, query, "{left} {op} {right}");
			}
		}
	}
}

#[test]
fn a_column_compared_with_one_value_in_a_function_answers_as_the_query_does_on_every_row() {
	// The single value must be repeated to every row, otherwise the answer covers one row or errors on length.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: Option(int4) }");
	t.command("INSERT test::t [{ id: 1, a: 1 }, { id: 2, a: 5 }, { id: 3, a: none }, { id: 4, a: 9 }]");
	for op in OPERATORS {
		for (left, right) in [("5", "a"), ("a", "5")] {
			let body = format!("{} {op} {}", left.replace('a', "$x"), right.replace('a', "$x"));
			let script = run(
				&t,
				&format!(
					"UDF m ($x) {{ RETURN {body} }}; FROM test::t | sort {{ id: ASC }} | map {{ v: m(a) }}"
				),
			);
			let query = run(
				&t,
				&format!("FROM test::t | sort {{ id: ASC }} | map {{ v: {left} {op} {right} }}"),
			);
			assert!(
				matches!(&script, Answer::Column(_, values) if values.len() == 4),
				"{left} {op} {right}: {script:?}"
			);
			assert_eq!(script, query, "{left} {op} {right}");
		}
	}
}

#[derive(Debug, PartialEq)]
enum Answer {
	Column(String, Vec<String>),
	Error(String, String),
}

fn run(t: &TestEngine, rql: &str) -> Answer {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		let diagnostic = err.diagnostic();
		return Answer::Error(diagnostic.code.clone(), diagnostic.message.clone());
	}
	let column = result.frames.last().expect("a frame").column("v").unwrap().expect("column v");
	let values = (0..column.len()).map(|i| column.get_value(i).to_string()).collect();
	Answer::Column(column.get_type().to_string(), values)
}
