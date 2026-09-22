// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn outcome(t: &TestEngine, rql: &str) -> String {
	match catch_unwind(AssertUnwindSafe(|| t.inner().query_as(TestEngine::identity(), rql, Params::None))) {
		Err(panic) => format!(
			"panic {}",
			panic.downcast_ref::<String>()
				.cloned()
				.or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
				.unwrap_or_default()
		),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => format!("ok {}", result.frames.iter().map(|f| f.to_string()).collect::<String>()),
		},
	}
}

#[test]
fn an_assert_result_shorter_than_the_batch_never_panics() {
	// A row with no assert result has nothing proving it true, so it must fail, never index past the end.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");
	t.command("INSERT test::t [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let result = outcome(&t, "FROM test::t | assert { uuid::v4() != uuid::v4() }");

	assert!(!result.starts_with("panic"), "the assert must pass or fail as a statement error, got {result}");
}
