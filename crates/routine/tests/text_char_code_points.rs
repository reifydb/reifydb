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
fn an_int8_code_point_past_u32_is_an_error_not_a_truncated_character() {
	// Cutting the int8 with as u32 keeps the low bits, so 2^32 + 65 comes out as 'A'.
	let t = TestEngine::new();

	let result = outcome(&t, "map { v: text::char(cast('4294967361', int8)) }");

	assert!(result.starts_with("error "), "a code point past u32 must be an error, got {result}");
}

#[test]
fn an_invalid_code_point_is_an_error_not_an_empty_string() {
	// An empty string for a surrogate, a negative or a past-max code point hides the bad input as valid text.
	let t = TestEngine::new();
	let cases = [
		"text::char(cast('55296', int4))",
		"text::char(cast('1114112', int4))",
		"text::char(cast('-1', int4))",
	];

	let mut wrong = Vec::new();
	for call in cases {
		let result = outcome(&t, &format!("map {{ v: {call} }}"));
		if !result.starts_with("error ") {
			wrong.push((call, result));
		}
	}

	assert!(wrong.is_empty(), "each invalid code point must be an error, got {wrong:#?}");
}
