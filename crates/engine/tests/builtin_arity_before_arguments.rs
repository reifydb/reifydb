// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, params::Params};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: -1 }, { a: 2 }]");
	t
}

fn query_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
	}
}

fn assert_arity_mismatch(diagnostic: &Diagnostic, function: &str) {
	assert_eq!(diagnostic.code, "FUNCTION_002", "got: {diagnostic:?}");
	assert_eq!(diagnostic.fragment.text(), function, "got: {diagnostic:?}");
}

#[test]
fn an_extra_type_name_argument_reports_the_argument_count() {
	// A type name past the type slots reads as a column, so an argument-first check reports a missing column instead of the wrong count.
	let t = engine();
	assert_arity_mismatch(&query_err(&t, "map {is::type(42, int4, int4)}"), "is::type");
}

#[test]
fn an_extra_argument_naming_a_missing_column_reports_the_argument_count() {
	// The count is known before any row is read, so it must win over an error raised while evaluating the extra argument.
	let t = engine();
	assert_arity_mismatch(&query_err(&t, "from test::t map { v: math::abs(a, missing) }"), "math::abs");
}
