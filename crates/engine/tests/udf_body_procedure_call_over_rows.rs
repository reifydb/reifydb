// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[derive(Debug, PartialEq)]
enum Answer {
	Column(Vec<String>),
	Error(String, String),
}

fn run(t: &TestEngine, rql: &str) -> Answer {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		let diagnostic = err.diagnostic();
		return Answer::Error(diagnostic.code, diagnostic.message);
	}
	let column = result.frames.last().expect("a frame").column("v").unwrap().expect("column v");
	Answer::Column((0..column.len()).map(|i| column.get_value(i).to_string()).collect())
}

#[test]
fn a_procedure_or_generator_called_in_a_body_over_rows_answers_on_each_row_what_it_answers_on_one_row() {
	// A call that runs one row at a time must run once per row, never surface an internal or record batch error.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: int4 }");
	t.command("INSERT test::t [{ id: 1, a: 1 }, { id: 2, a: 7 }]");
	t.admin("CREATE PROCEDURE test::greet AS { map { message: 'hello' } }");
	for body in ["test::greet()", "gen::series(1, 3)", "series::generate(1, 3)", "graphql::explain(\"{ a }\")"] {
		let udf = format!("UDF m ($x) {{ RETURN {body} }};");
		let one_row = run(&t, &format!("{udf} map {{ v: m(1) }}"));
		let over_rows = run(&t, &format!("{udf} FROM test::t | sort {{ id: ASC }} | map {{ v: m(a) }}"));
		let expected = match one_row {
			Answer::Column(values) => Answer::Column([values.clone(), values].concat()),
			error => error,
		};
		assert_eq!(over_rows, expected, "{body}");
	}
}
