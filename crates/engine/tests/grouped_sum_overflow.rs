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
fn an_overflowing_grouped_sum_fails_like_overflowing_addition_instead_of_wrapping() {
	// int16 has no wider fixed type, so under the default error mode an overflowing sum must be an error.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::p { a: int16, b: int16 }");
	t.admin("CREATE TABLE test::t { g: int4, a: int16 }");
	t.command("INSERT test::p [{ a: 170141183460469231731687303715884105727, b: 1 }]");
	t.command("INSERT test::t [{ g: 1, a: 170141183460469231731687303715884105727 }, { g: 1, a: 1 }]");

	let addition = outcome(&t, "FROM test::p | map { s: a + b }");
	let grouped = outcome(&t, "FROM test::t | aggregate { s: math::sum(a) } by { g }");

	assert!(addition.starts_with("error "), "the reference overflow mode must be an error, got {addition}");
	assert!(grouped.starts_with("error "), "grouped sum must fail like addition ({addition}), got {grouped}");
}
