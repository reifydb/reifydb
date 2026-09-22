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
fn gcd_of_a_wide_argument_is_the_true_divisor_not_the_one_of_a_narrowed_value() {
	// Narrowing to i64 wraps or truncates wide arguments and abs of i64::MIN panics, so gcd comes out wrong.
	let cases = [
		("math::gcd(cast('18446744073709551615', uint8), 3)", "3"),
		("math::gcd(cast('55340232221128654848', int16), 9)", "3"),
		("math::gcd(cast('-9223372036854775808', int8), 2)", "2"),
	];

	let mut observed = Vec::new();
	let mut expected = Vec::new();
	for (call, answer) in cases {
		let t = TestEngine::new();
		observed.push((call, outcome(&t, &format!("map {{ v: {call} }}"))));
		expected.push((call, format!("ok [\"{answer}\"]")));
	}

	assert_eq!(observed, expected);
}

#[test]
fn lcm_of_a_wide_argument_is_an_error_or_the_true_multiple() {
	// The true multiple does not fit the narrowed i64, so a wrapped result or a panic both hide the overflow.
	let cases = [
		("math::lcm(cast('18446744073709551615', uint8), 1)", "18446744073709551615"),
		("math::lcm(cast('-9223372036854775808', int8), 1)", "9223372036854775808"),
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
