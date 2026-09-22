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

fn plus(width: &str, value: &str) -> String {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(&format!("CREATE TABLE test::t {{ x: {width} }}"));
	t.command(&format!("INSERT test::t [{{ x: cast('{value}', {width}) }}]"));
	outcome(&t, "FROM test::t | map { v: +x }")
}

#[test]
fn unary_plus_on_an_unsigned_value_keeps_the_value() {
	// Retyping to the signed twin with as wraps the top half, so +200 as uint1 came out as -56.
	let cases = [
		("uint1", "200"),
		("uint2", "65535"),
		("uint4", "4294967295"),
		("uint8", "18446744073709551615"),
		("uint16", "340282366920938463463374607431768211455"),
	];

	let mut observed = Vec::new();
	let mut expected = Vec::new();
	for (width, value) in cases {
		observed.push((width, plus(width, value)));
		expected.push((width, format!("ok [\"{value}\"]")));
	}

	assert_eq!(observed, expected);
}
