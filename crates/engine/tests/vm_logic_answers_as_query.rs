// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const OPERATORS: [&str; 3] = ["and", "or", "xor"];

const OPERANDS: [&str; 6] = ["true", "false", "none", "cast(none, bool)", "1", "\"a\""];

#[test]
fn a_script_logic_op_answers_exactly_what_the_same_op_answers_in_a_query() {
	// A script logic op must give exactly the query's type, values or error, otherwise a LET and a map drift.
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
	for operand in OPERANDS {
		let script = run(&t, &format!("LET $a = {operand}; LET $r = not $a; map {{ v: $r }}"));
		let query = run(&t, &format!("LET $a = {operand}; map {{ v: not $a }}"));
		assert_eq!(script, query, "not {operand}");
	}
}

#[test]
fn a_logic_op_in_a_function_over_rows_answers_as_the_query_does_on_every_row() {
	// The single value must be repeated to every row, otherwise the answer covers one row or errors on length.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, b: Option(bool) }");
	t.command("INSERT test::t [{ id: 1, b: true }, { id: 2, b: false }, { id: 3, b: none }, { id: 4, b: true }]");
	let rows = "FROM test::t | sort { id: ASC } | map";
	let mut cases = vec![("not $x".to_string(), "not b".to_string())];
	for op in OPERATORS {
		for value in ["true", "false"] {
			cases.push((format!("$x {op} {value}"), format!("b {op} {value}")));
			cases.push((format!("{value} {op} $x"), format!("{value} {op} b")));
		}
	}
	for (body, literal) in cases {
		let script = run(&t, &format!("UDF m ($x) {{ RETURN {body} }}; {rows} {{ v: m(b) }}"));
		let query = run(&t, &format!("{rows} {{ v: {literal} }}"));
		assert!(matches!(&script, Answer::Column(_, values) if values.len() == 4), "{body}: {script:?}");
		assert_eq!(script, query, "{body}");
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
