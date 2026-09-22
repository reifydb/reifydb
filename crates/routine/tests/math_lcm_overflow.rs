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
			None => {
				let column = result.frames[0]
					.columns
					.iter()
					.find(|c| c.name == "v")
					.unwrap_or_else(|| panic!("no column v in {rql}"));
				let values: Vec<String> =
					(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
				format!("ok {values:?}")
			}
		},
	}
}

#[test]
fn lcm_of_large_coprime_arguments_is_an_error_or_the_exact_multiple() {
	// Coprime inputs whose product passes i64::MAX overflow the unchecked multiply, which panics or wraps.
	let cases = [
		("math::lcm(cast('4000000000', int8), cast('4000000001', int8))", "16000000004000000000"),
		("math::lcm(cast('-4000000000', int8), cast('4000000001', int8))", "16000000004000000000"),
		("math::lcm(cast('4294967291', uint4), cast('4294967279', uint4))", "18446743979220271189"),
	];

	let mut wrong = Vec::new();
	for (call, multiple) in cases {
		let t = TestEngine::new();
		let result = outcome(&t, &format!("map {{ v: {call} }}"));
		if !result.starts_with("error ") && result != format!("ok [\"{multiple}\"]") {
			wrong.push((call, result));
		}
	}

	assert!(wrong.is_empty(), "lcm must be a range error or the exact multiple, got {wrong:#?}");
}
