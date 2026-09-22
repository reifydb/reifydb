// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::frame::frame::Frame};

fn column(frames: &[Frame], name: &str) -> Vec<String> {
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect()
}

fn answers(t: &TestEngine, rql: &str) -> String {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		return format!("error {}", err.diagnostic().code);
	}
	column(&result.frames, "v").join(",")
}

#[test]
fn uint16_above_i128_max_compares_by_value_with_int16() {
	// A failed promotion to i128 makes every ordering false, so u128::MAX would not exceed i128::MAX.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, u: uint16, i: int16 }");
	t.command(
		"INSERT test::t [{ id: 1, u: cast('340282366920938463463374607431768211455', uint16), i: cast('170141183460469231731687303715884105727', int16) }, \
		 { id: 2, u: cast('170141183460469231731687303715884105728', uint16), i: cast('-1', int16) }]",
	);

	let cases = [
		("u > i", "true,true"),
		("u >= i", "true,true"),
		("u < i", "false,false"),
		("u <= i", "false,false"),
		("u == i", "false,false"),
		("u != i", "true,true"),
		("i < u", "true,true"),
		("i > u", "false,false"),
	];
	let mut observed = Vec::new();
	let mut expected = Vec::new();
	for (condition, answer) in cases {
		observed.push((
			condition,
			answers(&t, &format!("FROM test::t | sort {{ id: ASC }} | map {{ v: {condition} }}")),
		));
		expected.push((condition, answer.to_string()));
	}
	let kept = column(&t.query("FROM test::t | filter { u > i } | sort { id: ASC }"), "id");

	assert_eq!(observed, expected);
	assert_eq!(kept, ["1", "2"], "a filter on u > i must keep both rows");
}
