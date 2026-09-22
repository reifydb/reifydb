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
fn an_unsigned_start_and_length_pick_the_same_characters_as_signed_ones() {
	// Reading an unsigned start or length as 0 turns every unsigned substring into the empty string.
	let t = TestEngine::new();
	let signed = outcome(&t, "map { v: text::substring('abcdef', 2, 3) }");

	let mut observed = Vec::new();
	let mut expected = Vec::new();
	for width in ["uint1", "uint2", "uint4", "uint8", "uint16"] {
		let call = format!("text::substring('abcdef', cast('2', {width}), cast('3', {width}))");
		observed.push((width, outcome(&t, &format!("map {{ v: {call} }}"))));
		expected.push((width, r#"ok ["cde"]"#.to_string()));
	}

	assert_eq!(signed, r#"ok ["cde"]"#, "the signed call is the reference answer");
	assert_eq!(observed, expected);
}
