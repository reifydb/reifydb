// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_batch_query_with_window_reports_an_error_instead_of_panicking() {
	// Window runs only in deferred views, so a batch query must refuse it with a diagnostic, never a panic.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	t.command("INSERT test::t [{ g: 1, v: 1.0, ts: \"2026-01-01T00:00:00Z\" }]");

	let outcome = catch_unwind(AssertUnwindSafe(|| {
		t.inner().query_as(
			TestEngine::identity(),
			"FROM test::t | window rolling { total: math::sum(v) } with { duration: 1h } by { g }",
			Params::None,
		)
	}));

	let result = outcome.unwrap_or_else(|payload| {
		let message = payload
			.downcast_ref::<String>()
			.cloned()
			.or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
			.unwrap_or_default();
		panic!("window in a batch query panicked instead of returning an error: {message}")
	});
	let Some(err) = result.error else {
		panic!("window is not supported in a batch query, so it must be an error, got {:?}", result.frames);
	};
	let diagnostic = err.diagnostic();
	assert!(!diagnostic.code.is_empty(), "the error must carry a code, got: {diagnostic:?}");
	assert!(
		diagnostic.message.to_lowercase().contains("window"),
		"the error must name window, got: {diagnostic:?}"
	);
}
