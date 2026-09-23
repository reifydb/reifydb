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
fn an_overflowing_duration_addition_is_an_error_not_a_panic() {
	// A panic on the statement worker takes the process down, so overflow must surface as an error.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { d: duration }");
	t.command("INSERT test::t [{ d: duration::months(2147483647) }]");

	let result = outcome(&t, "FROM test::t | map { v: d + duration::months(1) }");

	assert!(result.starts_with("error "), "months past i32::MAX must be an error, got {result}");
}
