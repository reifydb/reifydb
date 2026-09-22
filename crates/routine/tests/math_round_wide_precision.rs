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
fn a_precision_beyond_i32_is_not_truncated_to_a_small_digit_count() {
	// Truncating 2^32 + 1 digits to i32 rounds to 1 digit, so 1.25 must stay 1.25 or the call must fail.
	let t = TestEngine::new();
	let mut wrong = Vec::new();
	for width in ["int8", "int16", "uint8", "uint16"] {
		let rql = format!("map {{ v: math::round(cast('1.25', float8), cast('4294967297', {width})) }}");
		let result = outcome(&t, &rql);
		if !result.starts_with("error ") && result != "ok [\"1.25\"]" {
			wrong.push((rql, result));
		}
	}

	assert!(wrong.is_empty(), "a huge precision must keep 1.25 or be an error, got {wrong:#?}");
}

#[test]
fn a_precision_that_is_not_an_integer_is_a_type_error_not_zero_digits() {
	// Reading an unusable precision as 0 silently rounds to a whole number instead of rejecting the argument.
	let t = TestEngine::new();

	let result = outcome(&t, "map { v: math::round(cast('1.25', float8), 'two') }");

	assert!(result.starts_with("error "), "a text precision must be rejected, got {result}");
}
