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
fn abs_of_the_minimum_signed_value_is_an_error_never_a_panic_or_the_minimum_itself() {
	// The positive of MIN does not fit its width, so abs must report that instead of panicking or wrapping to MIN.
	let cases = [
		("int1", "-128", "128"),
		("int2", "-32768", "32768"),
		("int4", "-2147483648", "2147483648"),
		("int8", "-9223372036854775808", "9223372036854775808"),
		("int16", "-170141183460469231731687303715884105728", "170141183460469231731687303715884105728"),
	];

	let mut wrong = Vec::new();
	for (width, min, positive) in cases {
		let t = TestEngine::new();
		let rql = format!("map {{ v: math::abs(cast('{min}', {width})) }}");
		let result = outcome(&t, &rql);
		if !result.starts_with("error ") && result != format!("ok [\"{positive}\"]") {
			wrong.push((rql, result));
		}
	}

	assert!(wrong.is_empty(), "abs of MIN must be a range error or the exact positive value, got {wrong:#?}");
}
