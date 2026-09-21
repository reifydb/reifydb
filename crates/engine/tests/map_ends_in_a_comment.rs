// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_query_that_ends_where_an_expression_is_expected_reports_an_error_instead_of_panicking() {
	// A query cut short by a trailing '#' comment must be a diagnostic, never a panic.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");
	t.command("INSERT test::t [{ id: 1 }]");

	let rql = "FROM test::t | map { id, #foo }";
	let outcome = catch_unwind(AssertUnwindSafe(|| t.inner().query_as(TestEngine::identity(), rql, Params::None)));

	let result = outcome.unwrap_or_else(|payload| {
		let message = payload
			.downcast_ref::<String>()
			.cloned()
			.or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
			.unwrap_or_default();
		panic!("{rql} panicked instead of returning an error: {message}")
	});
	let Some(err) = result.error else {
		panic!("the query ends after the comma, so it must be an error, got {:?}", result.frames);
	};
	let diagnostic = err.diagnostic();
	assert!(!diagnostic.code.is_empty(), "the error must carry a code, got: {diagnostic:?}");
}
