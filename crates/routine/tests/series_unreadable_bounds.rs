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
fn series_bounds_of_an_unread_type_give_the_asked_series_or_an_error_never_one_to_ten() {
	// Falling back to 1..10 when a bound is unreadable returns ten rows nobody asked for.
	let t = TestEngine::new();
	let cases = [
		"gen::series(cast('2', uint8), cast('3', uint8))",
		"gen::series(cast('2', int16), cast('3', int16))",
		"gen::series(cast('2', uint16), cast('3', uint16))",
	];

	let mut wrong = Vec::new();
	for call in cases {
		let result = outcome(&t, &format!("map {{ v: {call} }}"));
		if !result.starts_with("error ") && result != r#"ok ["2", "3"]"# {
			wrong.push((call, result));
		}
	}

	assert!(wrong.is_empty(), "a series from 2 to 3 must hold 2 and 3 or be an error, got {wrong:#?}");
}

#[test]
fn series_bounds_that_are_not_numbers_are_an_error() {
	// A text bound has no integer reading, so a default series in its place hides the bad argument.
	let t = TestEngine::new();

	let result = outcome(&t, "map { v: gen::series('a', 'b') }");

	assert!(result.starts_with("error "), "text bounds must be an error, got {result}");
}
