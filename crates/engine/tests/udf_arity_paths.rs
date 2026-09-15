// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const LOOP: &str = "LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 };";

fn engine() -> TestEngine {
	// Two rows, otherwise a udf body calling another udf falls back to the scalar path, never the batch paths.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: 1 }, { a: 2 }]");
	t
}

fn paths(parameters: &str, arguments: &str) -> Vec<(&'static str, &'static str, String)> {
	let vectorized = format!("UDF f ({parameters}) {{ RETURN 1 }}; ");
	let per_row = format!("UDF f ({parameters}) {{ {LOOP} RETURN 1 }}; ");
	let closure = format!("let $f = ({parameters}) {{ 1 }}; ");
	let outer = format!("UDF outer ($y) {{ RETURN f({arguments}) }}; ");
	vec![
		("script call", "f", format!("{vectorized}let $v = f({arguments}); map {{ v: $v }}")),
		("map without input", "f", format!("{vectorized}map {{ v: f({arguments}) }}")),
		("vectorized udf over rows", "f", format!("{vectorized}FROM test::t | map {{ v: f({arguments}) }}")),
		("per-row udf over rows", "f", format!("{per_row}FROM test::t | map {{ v: f({arguments}) }}")),
		("udf in a filter", "f", format!("{vectorized}FROM test::t | filter {{ f({arguments}) == 1 }}")),
		(
			"vectorized udf called from a udf body",
			"f",
			format!("{vectorized}{outer}FROM test::t | map {{ v: outer(a) }}"),
		),
		(
			"per-row udf called from a udf body",
			"f",
			format!("{per_row}{outer}FROM test::t | map {{ v: outer(a) }}"),
		),
		("closure script call", "$f", format!("{closure}let $v = $f({arguments}); map {{ v: $v }}")),
		("closure without input", "$f", format!("{closure}map {{ v: $f({arguments}) }}")),
		("closure over rows", "$f", format!("{closure}FROM test::t | map {{ v: $f({arguments}) }}")),
	]
}

fn assert_arity_error(t: &TestEngine, parameters: &str, arguments: &str, expected: usize, given: usize) {
	for (path, function, rql) in paths(parameters, arguments) {
		let result = t.inner().query_as(TestEngine::identity(), &rql, Params::None);

		let Some(err) = result.error else {
			panic!("{path}: a call with the wrong number of arguments must fail, got {:?}", result.frames);
		};
		let diagnostic = err.diagnostic();
		assert_eq!(
			(diagnostic.code.as_str(), diagnostic.fragment.text(), diagnostic.message.as_str()),
			(
				"FUNCTION_002",
				function,
				format!("function {function} expects {expected} arguments, got {given}").as_str()
			),
			"{path}: the error must name the callee at the call, never the opening parenthesis, got {diagnostic:?}"
		);
	}
}

#[test]
fn a_udf_or_closure_given_too_many_arguments_is_an_arity_error_naming_it_on_every_path() {
	// A dropped extra argument hides a call mistake, so every path must reject it with both counts.
	let t = engine();

	assert_arity_error(&t, "$x", "1, 2", 1, 2);
}

#[test]
fn a_udf_or_closure_given_too_few_arguments_is_an_arity_error_naming_it_on_every_path() {
	// A missing argument must never leave its parameter unbound while the body still runs.
	let t = engine();

	assert_arity_error(&t, "$x, $y", "1", 2, 1);
}

#[test]
fn a_udf_over_rows_with_the_wrong_argument_count_fails_even_when_no_row_reaches_it() {
	// Arity belongs to the call, so an empty input must not turn a wrong call into a silent success.
	let t = engine();

	let result = t.inner().query_as(
		TestEngine::identity(),
		"UDF f ($x) { RETURN $x }; FROM test::t | filter { a == 99 } | map { v: f(1, 2) }",
		Params::None,
	);

	let diagnostic = result.error.expect("the wrong argument count must fail on an empty input").diagnostic();
	assert_eq!((diagnostic.code.as_str(), diagnostic.fragment.text()), ("FUNCTION_002", "f"), "{diagnostic:?}");
}
