// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_vectorized_udf_calling_a_builtin_function_on_its_parameter_gives_one_result_per_row() {
	// The batch body passes the builtin a whole column, so popping a scalar must never become an internal error.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: int4 }");
	t.command("INSERT test::t [{ id: 1, a: -3 }, { id: 2, a: 4 }]");

	let result = t.inner().query_as(
		TestEngine::identity(),
		"UDF f ($x: int4) { RETURN math::abs($x) }; FROM test::t | sort { id: asc } | map { v: f(a) }",
		Params::None,
	);

	if let Some(err) = result.error {
		panic!("the call must succeed, got {:?}", err.diagnostic());
	}
	let column = result.frames[0].columns.iter().find(|c| c.name == "v").expect("column v");
	let values: Vec<String> = (0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
	assert_eq!(values, vec!["3", "4"]);
}
