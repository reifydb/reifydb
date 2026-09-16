// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, fragment::Fragment, params::Params};

const FUNCTIONS: [&str; 4] = ["math::avg", "math::sum", "math::min", "math::max"];

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, s: utf8 }");
	t.command("INSERT test::t [{ g: 1, s: \"a\" }, { g: 2, s: \"b\" }]");
	t
}

fn query_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
	}
}

fn assert_points_at_function(diagnostic: &Diagnostic, function: &str) {
	assert_eq!(diagnostic.code, "FUNCTION_004", "got: {diagnostic:?}");
	assert!(
		matches!(diagnostic.fragment, Fragment::Statement { .. }),
		"the fragment must carry a position, got: {diagnostic:?}"
	);
	assert_eq!(diagnostic.fragment.text(), function, "got: {diagnostic:?}");
}

#[test]
fn a_text_literal_argument_to_a_numeric_aggregate_reports_the_call_position() {
	// Without the call fragment the error renders with no LOCATION and no RQL, so a user cannot see which aggregate
	// failed.
	let t = engine();
	for function in FUNCTIONS {
		let rql = format!("from test::t aggregate {{ v: {function}('hello') }} by {{ g }}");
		assert_points_at_function(&query_err(&t, &rql), function);
	}
}

#[test]
fn a_text_column_argument_to_a_numeric_aggregate_reports_the_call_position() {
	// A column input reaches the accumulator on a different path than a literal, so both must keep the position.
	let t = engine();
	for function in FUNCTIONS {
		let rql = format!("from test::t aggregate {{ v: {function}(s) }} by {{ g }}");
		assert_points_at_function(&query_err(&t, &rql), function);
	}
}
