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
fn substring_start_and_length_beyond_i32_are_not_truncated() {
	// Truncating an int8 start or length to i32 picks other characters than the ones asked for.
	let t = TestEngine::new();
	let cases = [
		("text::substring('abcdef', cast('4294967296', int8), cast('1', int8))", "ok [\"\"]"),
		("text::substring('abcdef', cast('1', int8), cast('4294967297', int8))", "ok [\"bcdef\"]"),
	];

	let mut wrong = Vec::new();
	for (call, answer) in cases {
		let result = outcome(&t, &format!("map {{ v: {call} }}"));
		if !result.starts_with("error ") && result != answer {
			wrong.push((call, answer, result));
		}
	}

	assert!(wrong.is_empty(), "each call must give the untruncated answer or an error, got {wrong:#?}");
}

#[test]
fn series_bounds_beyond_i32_are_not_truncated() {
	// Truncating int8 bounds to i32 turns a series past 2^32 into a series near zero.
	let t = TestEngine::new();

	let result = outcome(&t, "map { v: gen::series(cast('4294967297', int8), cast('4294967298', int8)) }");

	assert!(
		result.starts_with("error ") || result == "ok [\"4294967297\", \"4294967298\"]",
		"a series from 2^32 + 1 to 2^32 + 2 must hold those two values or be an error, got {result}"
	);
}
