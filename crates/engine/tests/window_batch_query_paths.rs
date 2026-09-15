// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_core::execution::ExecutionResult;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, params::Params};

const WINDOW: &str = "FROM test::t | window rolling { total: math::sum(v) } with { duration: 1h } by { g }";

fn setup() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	t.command("INSERT test::t [{ g: 1, v: 1.0, ts: \"2026-01-01T00:00:00Z\" }]");
	t
}

fn without_panic(rql: &str, run: impl FnOnce() -> ExecutionResult) -> ExecutionResult {
	catch_unwind(AssertUnwindSafe(run)).unwrap_or_else(|payload| {
		let message = payload
			.downcast_ref::<String>()
			.cloned()
			.or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
			.unwrap_or_default();
		panic!("{rql} panicked instead of returning an error: {message}")
	})
}

fn assert_window_rejected(t: &TestEngine, rql: &str) {
	let result = without_panic(rql, || t.inner().admin_as(TestEngine::identity(), rql, Params::None));
	let Some(err) = result.error else {
		panic!("{rql} runs a window outside a deferred view, so it must be an error, got {:?}", result.frames);
	};
	let diagnostic: Diagnostic = err.diagnostic();
	assert_eq!(
		diagnostic.code, "QUERY_007",
		"{rql} must be refused as a window in a batch query, got: {diagnostic:?}"
	);
	assert!(diagnostic.message.contains("window"), "the error must name window, got: {diagnostic:?}");
}

#[test]
fn a_let_bound_window_reports_an_error_instead_of_panicking() {
	// A window bound to a variable still runs as a batch query and must be refused, never panic.
	let t = setup();
	assert_window_rejected(&t, &format!("let $w = {WINDOW}; FROM $w"));
}

#[test]
fn a_window_in_an_if_branch_reports_an_error_instead_of_panicking() {
	// A branch body is compiled as its own batch plan, so a window there must be refused too.
	let t = setup();
	assert_window_rejected(&t, &format!("if true {{ {WINDOW} }}"));
}

#[test]
fn a_window_in_a_for_iterable_reports_an_error_instead_of_panicking() {
	// A for iterable subquery is a batch plan, so a window there must be refused too.
	let t = setup();
	assert_window_rejected(&t, &format!("let $n = 0; for $r in {{ {WINDOW} }} {{ $n = $n + 1 }}; map {{ n: $n }}"));
}

#[test]
fn a_window_as_inner_join_input_reports_an_error_instead_of_panicking() {
	// The join right side is compiled with the batch operators, so a window there must be refused.
	let t = setup();
	assert_window_rejected(&t, &format!("FROM test::t INNER JOIN {{ {WINDOW} }} AS s USING (g, s.g)"));
}

#[test]
fn a_window_as_left_join_input_reports_an_error_instead_of_panicking() {
	// The left join right side is compiled with the batch operators, so a window there must be refused.
	let t = setup();
	assert_window_rejected(&t, &format!("FROM test::t LEFT JOIN {{ {WINDOW} }} AS s USING (g, s.g)"));
}

#[test]
fn a_window_as_natural_join_input_reports_an_error_instead_of_panicking() {
	// The natural join right side is compiled with the batch operators, so a window there must be refused.
	let t = setup();
	assert_window_rejected(&t, &format!("FROM test::t NATURAL JOIN {{ {WINDOW} }} AS s"));
}

#[test]
fn a_window_in_a_procedure_body_reports_an_error_instead_of_panicking() {
	// A procedure body runs as a batch query when called, so its window must be refused, never panic.
	let t = setup();
	t.admin(
		"CREATE PROCEDURE test::windowed AS { FROM test::t window rolling { total: math::sum(v) } with { duration: 1h } by { g } }",
	);
	assert_window_rejected(&t, "CALL test::windowed()");
}

#[test]
fn a_window_in_a_udf_body_reports_an_error_instead_of_panicking() {
	// A udf body runs as a batch query per call, so its window must be refused, never panic.
	let t = setup();
	assert_window_rejected(
		&t,
		&format!(
			"UDF windowed ($x: int4) {{ let $w = {WINDOW}; RETURN $x }}; FROM test::t MAP {{ r: windowed(g) }}"
		),
	);
}

#[test]
fn a_window_without_input_reports_an_error_instead_of_panicking() {
	// A window with no input still reaches the batch operators and must be refused, never panic.
	let t = setup();
	assert_window_rejected(&t, "window rolling { total: math::sum(v) } with { duration: 1h } by { g }");
}

#[test]
fn a_window_in_a_test_body_reports_an_error_outcome_instead_of_panicking() {
	// A test body runs as a batch query, so RUN TESTS must report the window as an error outcome, never panic.
	let t = setup();
	t.admin(&format!("CREATE TEST test::windowed {{ {WINDOW} }}"));
	let result = without_panic("RUN TESTS test", || {
		t.inner().admin_as(TestEngine::identity(), "RUN TESTS test", Params::None)
	});
	assert!(result.error.is_none(), "a failing test body must not fail RUN TESTS itself, got: {:?}", result.error);
	let rows: Vec<_> = result.frames[0].rows().collect();
	assert_eq!(rows.len(), 1, "exactly the one test must run, got: {}", result.frames[0]);
	assert_eq!(rows[0].get::<String>("outcome").unwrap().unwrap(), "error", "got: {}", result.frames[0]);
	let message = rows[0].get::<String>("message").unwrap().unwrap();
	assert!(message.contains("QUERY_007"), "the test outcome must carry the window refusal, got: {message}");
}
