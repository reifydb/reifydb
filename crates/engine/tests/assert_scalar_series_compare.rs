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
			None => "ok".to_string(),
		},
	}
}

#[test]
fn a_true_assert_over_a_one_value_series_passes() {
	// The series and the literal must line up to the same length, or the compare trips its length check.
	let t = TestEngine::new();

	let result = outcome(&t, "assert { gen::series(1, 1) == 1 }");

	assert_eq!(result, "ok", "gen::series(1, 1) holds the single value 1, so the assert holds");
}

#[test]
fn an_assert_comparing_a_series_over_a_multi_row_batch_never_panics() {
	// A series shorter than the batch must never reach the compare with unequal lengths and trip its length check.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");
	t.command("INSERT test::t [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let result = outcome(&t, "FROM test::t | assert { gen::series(1, 1) == 1 }");

	assert!(!result.starts_with("panic"), "the assert must pass or fail as a statement error, got {result}");
}
