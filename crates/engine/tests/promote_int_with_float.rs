// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::value_type::ValueType};

#[test]
fn clamp_of_an_int_by_a_float_bound_keeps_the_fraction() {
	// Promoting int with float8 to int casts the 0.5 bound down to a whole number before clamping.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int, f: float8 }");
	t.command("INSERT test::t [{ a: 0, f: 0.5 }, { a: 20, f: 0.5 }]");
	let r = t.inner().query_as(
		TestEngine::identity(),
		"FROM test::t | sort { a: asc } | map { v: math::clamp(a, f, 10) }",
		Params::None,
	);
	if let Some(e) = r.error {
		panic!("query failed: {e:?}")
	}
	let column = r.frames[0].columns.iter().find(|c| c.name == "v").expect("column v");
	let values: Vec<String> = (0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
	assert_eq!(column.data.get_type(), ValueType::DECIMAL);
	assert_eq!(values, vec!["0.5000000000", "10.0000000000"]);
}
