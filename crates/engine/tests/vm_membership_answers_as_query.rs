// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const OPERANDS: [&str; 7] = ["2", "300", "cast(2, float8)", "\"a\"", "true", "none", "cast(none, int4)"];

fn lists() -> Vec<String> {
	let mut lists = vec![String::new()];
	for first in OPERANDS {
		lists.push(first.to_string());
		for second in OPERANDS {
			lists.push(format!("{first}, {second}"));
		}
	}
	lists.extend(["2, 300, none", "\"a\", 2, true", "none, cast(none, int4), 300"].map(String::from));
	lists
}

#[test]
fn a_script_between_or_in_answers_exactly_what_the_same_query_answers() {
	// A script between or in must give exactly the query's type, values or error, otherwise a LET and a map drift.
	let t = TestEngine::new();
	for value in OPERANDS {
		for lower in OPERANDS {
			for upper in OPERANDS {
				let operands = format!("LET $a = {value}; LET $l = {lower}; LET $u = {upper};");
				let script =
					run(&t, &format!("{operands} LET $r = $a between $l and $u; map {{ v: $r }}"));
				let query = run(&t, &format!("{operands} map {{ v: $a between $l and $u }}"));
				assert_eq!(script, query, "{value} between {lower} and {upper}");
			}
		}
	}
	for value in OPERANDS {
		for list in lists() {
			for not in ["", "not "] {
				let operands = format!("LET $a = {value};");
				let script =
					run(&t, &format!("{operands} LET $r = $a {not}in [{list}]; map {{ v: $r }}"));
				let query = run(&t, &format!("{operands} map {{ v: $a {not}in [{list}] }}"));
				assert_eq!(script, query, "{value} {not}in [{list}]");
			}
		}
	}
}

#[test]
fn a_between_or_in_in_a_function_over_rows_answers_as_the_query_does_on_every_row() {
	// Every operand must cover every row, otherwise the answer covers one row or errors on length.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: Option(int4) }");
	t.command("INSERT test::t [{ id: 1, a: 1 }, { id: 2, a: 5 }, { id: 3, a: none }, { id: 4, a: 9 }]");
	let rows = "FROM test::t | sort { id: ASC } | map";
	for body in [
		"$x between 2 and 6",
		"4 between $x and 10",
		"5 between 0 and $x",
		"$x in []",
		"$x not in []",
		"$x in [1, 9]",
		"$x not in [1, none]",
		"5 in [$x, 7]",
	] {
		let script = run(&t, &format!("UDF m ($x) {{ RETURN {body} }}; {rows} {{ v: m(a) }}"));
		let query = run(&t, &format!("{rows} {{ v: {} }}", body.replace("$x", "a")));
		assert!(matches!(&script, Answer::Column(_, values) if values.len() == 4), "{body}: {script:?}");
		assert_eq!(script, query, "{body}");
	}
}

#[test]
fn in_and_not_in_with_the_same_count_in_one_script_each_keep_their_own_answer() {
	// One cached plan per count must still tell in from not in, otherwise the second reuses the first's answer.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: int4 }");
	t.command("INSERT test::t [{ id: 1, a: 1 }, { id: 2, a: 5 }]");
	let script = "LET $a = 2; LET $r = $a in [1, 2]; LET $s = $a not in [1, 2]; map { v: [$r, $s] }";
	assert_eq!(run(&t, script), Answer::Column("Any".to_string(), vec!["[true, false]".to_string()]), "{script}");
	let body = "UDF m ($x) { LET $r = $x in [1, 2]; LET $s = $x not in [1, 2]; RETURN $r and not $s };";
	assert_eq!(
		run(&t, &format!("{body} FROM test::t | sort {{ id: ASC }} | map {{ v: m(a) }}")),
		run(&t, "FROM test::t | sort { id: ASC } | map { v: a in [1, 2] }"),
		"{body}"
	);
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
