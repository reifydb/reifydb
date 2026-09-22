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
fn an_epoch_past_the_datetime_range_is_an_error_like_the_millis_variant_never_1970() {
	// Swallowing the overflow publishes 1970-01-01 as a real row, so it must fail like from_epoch_millis does.
	let t = TestEngine::new();

	let millis = outcome(&t, "map { v: datetime::from_epoch_millis(cast('18446744074000', int8)) }");
	let seconds = outcome(&t, "map { v: datetime::from_epoch(cast('18446744074', int8)) }");

	assert!(millis.starts_with("error "), "the reference variant must reject the overflow, got {millis}");
	assert!(
		seconds.starts_with("error "),
		"from_epoch must reject the overflow like from_epoch_millis ({millis}), got {seconds}"
	);
}
