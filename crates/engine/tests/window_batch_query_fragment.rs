// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	fragment::{Fragment, StatementColumn, StatementLine},
	params::Params,
};

fn setup() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	t.command("INSERT test::t [{ g: 1, v: 1.0, ts: \"2026-01-01T00:00:00Z\" }]");
	t
}

fn window_refusal(t: &TestEngine, rql: &str) -> Diagnostic {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	let Some(err) = result.error else {
		panic!("{rql} runs a window outside a deferred view, so it must be an error, got {:?}", result.frames);
	};
	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "QUERY_007", "{rql} must be refused as a window in a batch query: {diagnostic:?}");
	diagnostic
}

fn assert_points_at_window(diagnostic: &Diagnostic, line: u32, column: u32) {
	match &diagnostic.fragment {
		Fragment::Statement {
			text,
			line: at_line,
			column: at_column,
		} => {
			assert_eq!(&**text, "window", "{diagnostic:?}");
			assert_eq!(
				(*at_line, *at_column),
				(StatementLine(line), StatementColumn(column)),
				"{diagnostic:?}"
			);
		}
		other => panic!("the fragment must point at the window keyword in the statement, got {other:?}"),
	}
}

#[test]
fn a_window_refused_in_a_batch_query_points_at_the_window_keyword() {
	// Without a span the author of a long pipeline cannot tell which step the refusal is about.
	let t = setup();
	let diagnostic = window_refusal(
		&t,
		"FROM test::t | window rolling { total: math::sum(v) } with { duration: 1h } by { g }",
	);
	assert_points_at_window(&diagnostic, 1, 16);
}

#[test]
fn a_window_refused_on_a_later_line_points_at_that_line() {
	// The span must carry the real line, otherwise a span fixed to line 1 passes the single-line check.
	let t = setup();
	let diagnostic = window_refusal(
		&t,
		"FROM test::t\n| window rolling { total: math::sum(v) } with { duration: 1h } by { g }",
	);
	assert_points_at_window(&diagnostic, 2, 3);
}
