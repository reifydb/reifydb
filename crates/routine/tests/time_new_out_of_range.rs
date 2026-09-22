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
fn time_new_with_a_part_out_of_range_is_an_error_or_none_never_midnight() {
	// Turning an impossible time into 00:00:00 gives a real looking time for bad input.
	let t = TestEngine::new();
	let cases = [
		"time::new(25, 0, 0)",
		"time::new(-1, 0, 0)",
		"time::new(0, 60, 0)",
		"time::new(0, -1, 0)",
		"time::new(0, 0, 60)",
		"time::new(0, 0, -1)",
		"time::new(0, 0, 0, -1)",
	];

	let mut wrong = Vec::new();
	for call in cases {
		let result = outcome(&t, &format!("map {{ v: {call} }}"));
		if !result.starts_with("error ") && result != r#"ok ["none"]"# {
			wrong.push((call, result));
		}
	}

	assert!(wrong.is_empty(), "an out of range time part must give an error or none, got {wrong:#?}");
}
