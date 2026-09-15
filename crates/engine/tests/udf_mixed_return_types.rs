// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const LOOP: &str = "LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 };";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: 5 }, { a: 10 }]");
	t
}

fn outcome(t: &TestEngine, rql: &str) -> String {
	// A panic must be recorded as an outcome, otherwise one crashing path hides how the other paths behave.
	match catch_unwind(AssertUnwindSafe(|| t.inner().query_as(TestEngine::identity(), rql, Params::None))) {
		Err(_) => "panic".to_string(),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => format!(
				"ok {:?}",
				result.frames[0].columns.iter().find(|c| c.name == "v").map(|c| c.data.get_type())
			),
		},
	}
}

fn outcomes(t: &TestEngine, first: &str, second: &str) -> Vec<(String, String)> {
	let mut out = Vec::new();
	for (then, otherwise) in [(first, second), (second, first)] {
		let body = format!("IF $x > 7 {{ RETURN {then} }}; RETURN {otherwise}");
		let queries = [
			("vectorized", format!("UDF m ($x) {{ {body} }}; FROM test::t | map {{ v: m(a) }}")),
			("per-row", format!("UDF m ($x) {{ {LOOP} {body} }}; FROM test::t | map {{ v: m(a) }}")),
			(
				"per-row called from a udf body",
				format!(
					"UDF m ($x) {{ {LOOP} {body} }}; UDF outer ($y) {{ RETURN m($y) }}; FROM test::t | map {{ v: outer(a) }}"
				),
			),
		];
		for (path, rql) in queries {
			out.push((format!("{path}, {then} first"), outcome(t, &rql)));
		}
	}
	out
}

#[test]
fn an_untyped_udf_returning_text_for_one_row_and_an_int_for_another_fails_like_a_conditional_on_every_path() {
	// An if with mixed branch types is an error, so a udf must never panic, stringify or depend on branch order.
	let t = engine();
	let conditional = outcome(&t, "FROM test::t | map { v: if a > 7 { 'big' } else { 1 } }");
	assert_eq!(conditional, "error RUNTIME_012", "the reference conditional must reject mixed branch types");

	let outcomes = outcomes(&t, "'big'", "1");

	let first_error = outcomes.iter().find(|(_, o)| o.starts_with("error")).map(|(_, o)| o.clone());
	assert!(
		first_error.is_some() && outcomes.iter().all(|(_, o)| Some(o) == first_error.as_ref()),
		"every path and branch order must fail with one and the same error, got {outcomes:#?}"
	);
}

#[test]
fn an_untyped_udf_returning_int1_for_one_row_and_int2_for_another_behaves_the_same_on_every_path() {
	// A width difference must never crash a path, and the result must not depend on the path or on branch order.
	let t = engine();

	let outcomes = outcomes(&t, "1", "300");

	assert!(
		!outcomes.iter().any(|(_, o)| o == "panic") && outcomes.iter().all(|(_, o)| *o == outcomes[0].1),
		"every path and branch order must give the same outcome without a panic, got {outcomes:#?}"
	);
}

#[test]
fn an_untyped_udf_returning_an_int4_on_every_row_gives_an_int4_column_whatever_the_row_count() {
	// The column must stay int4, otherwise the widening fold turns two int4 rows into int8 and three into int16.
	let t = engine();
	t.command("INSERT test::t [{ a: 20 }]");
	let body = "IF $x > 7 { RETURN $x }; RETURN $x";
	let queries = [
		("vectorized", format!("UDF m ($x) {{ {body} }}; FROM test::t | map {{ v: m(a) }}")),
		("per-row", format!("UDF m ($x) {{ {LOOP} {body} }}; FROM test::t | map {{ v: m(a) }}")),
		(
			"per-row called from a udf body",
			format!(
				"UDF m ($x) {{ {LOOP} {body} }}; UDF outer ($y) {{ RETURN m($y) }}; FROM test::t | map {{ v: outer(a) }}"
			),
		),
	];

	let outcomes: Vec<(&str, String)> = queries.iter().map(|(path, rql)| (*path, outcome(&t, rql))).collect();

	assert!(
		outcomes.iter().all(|(_, o)| o == "ok Some(Int4)"),
		"every path must keep the int4 the body returns, got {outcomes:#?}"
	);
}
