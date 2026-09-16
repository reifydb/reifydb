// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_batch_query_with_append_reports_an_error_instead_of_panicking() {
	// Append runs only in deferred views, so a batch query must refuse it with a diagnostic, never a panic.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::a { id: int4 }");
	t.admin("CREATE TABLE test::b { id: int4 }");
	t.command("INSERT test::a [{ id: 1 }]");
	t.command("INSERT test::b [{ id: 2 }]");

	let outcome = catch_unwind(AssertUnwindSafe(|| {
		t.inner().query_as(TestEngine::identity(), "FROM test::a | append { FROM test::b }", Params::None)
	}));

	let result = outcome.unwrap_or_else(|payload| {
		let message = payload
			.downcast_ref::<String>()
			.cloned()
			.or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
			.unwrap_or_default();
		panic!("append in a batch query panicked instead of returning an error: {message}")
	});
	let Some(err) = result.error else {
		panic!("append is not supported in a batch query, so it must be an error, got {:?}", result.frames);
	};
	let diagnostic = err.diagnostic();
	assert!(!diagnostic.code.is_empty(), "the error must carry a code, got: {diagnostic:?}");
	assert!(
		diagnostic.message.to_lowercase().contains("append"),
		"the error must name append, got: {diagnostic:?}"
	);
}
