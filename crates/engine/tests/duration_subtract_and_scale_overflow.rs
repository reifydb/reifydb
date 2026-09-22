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

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { hi: duration, lo: duration }");
	t.command("INSERT test::t [{ hi: duration::months(2147483647), lo: duration::months(-2147483647) }]");
	t
}

#[test]
fn an_overflowing_duration_subtraction_scale_or_add_is_an_error_not_a_panic() {
	// A panic on the statement worker takes the process down, so months past the i32 range must be an error.
	let cases = [
		("lo - duration::months(1)", "lo - duration::months(2)"),
		("duration::subtract(lo, duration::months(1))", "duration::subtract(lo, duration::months(2))"),
		("duration::scale(hi, 1)", "duration::scale(hi, 2)"),
		("duration::add(lo, duration::months(1))", "duration::add(hi, duration::months(1))"),
	];

	let mut wrong = Vec::new();
	for (fits, overflows) in cases {
		let fits_result = outcome(&engine(), &format!("FROM test::t | map {{ v: {fits} }}"));
		let overflow_result = outcome(&engine(), &format!("FROM test::t | map {{ v: {overflows} }}"));
		if fits_result != "ok" || !overflow_result.starts_with("error ") {
			wrong.push((fits, fits_result, overflows, overflow_result));
		}
	}

	assert!(
		wrong.is_empty(),
		"each in-range call must succeed and its overflowing twin must be an error, got {wrong:#?}"
	);
}
