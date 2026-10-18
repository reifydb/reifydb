// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const OPERATORS: [&str; 3] = ["and", "or", "xor"];

const OPERANDS: [&str; 8] = ["true", "false", "none", "cast(none, bool)", "1", "\"a\"", "(2 > 1)", "(1 > \"a\")"];

#[test]
fn a_literal_logic_op_answers_exactly_what_the_same_op_over_variables_answers() {
	// Folding literals early must never skip a type check, otherwise true and 1 answers 1 instead of an error.
	let t = TestEngine::new();
	for op in OPERATORS {
		for left in OPERANDS {
			for right in OPERANDS {
				let literal = run(&t, &format!("map {{ v: {left} {op} {right} }}"));
				let variables =
					run(&t, &format!("LET $a = {left}; LET $b = {right}; map {{ v: $a {op} $b }}"));
				assert_eq!(literal, variables, "{left} {op} {right}");
			}
		}
	}
}

#[test]
fn a_literal_true_and_a_column_compare_still_answers_the_compare_on_every_row() {
	// The kept fold must still answer the column side exactly, row by row, with its own type.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: Option(int4) }");
	t.command("INSERT test::t [{ id: 1, a: 1 }, { id: 2, a: 5 }, { id: 3, a: none }]");
	let rows = "FROM test::t | sort { id: ASC } | map";
	for (folded, plain) in
		[("true and (a > 2)", "a > 2"), ("(a > 2) and true", "a > 2"), ("false or (a > 2)", "a > 2")]
	{
		assert_eq!(
			run(&t, &format!("{rows} {{ v: {folded} }}")),
			run(&t, &format!("{rows} {{ v: {plain} }}")),
			"{folded}"
		);
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
