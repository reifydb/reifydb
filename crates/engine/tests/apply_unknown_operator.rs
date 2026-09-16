// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn applying_an_unknown_operator_reports_an_error_instead_of_panicking() {
	// A user can type any operator name, so an unknown one must be a diagnostic, never a panic.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4 }");
	t.command("INSERT test::t [{ g: 1 }]");

	let rql = "FROM test::t | apply no_such_op {}";
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
		panic!("no_such_op is not a registered operator, so it must be an error, got {:?}", result.frames);
	};
	let diagnostic = err.diagnostic();
	assert!(!diagnostic.code.is_empty(), "the error must carry a code, got: {diagnostic:?}");
	assert!(
		diagnostic.message.contains("no_such_op"),
		"the error must name the unknown operator, got: {diagnostic:?}"
	);
}
