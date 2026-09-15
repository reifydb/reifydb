// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_udf_called_with_more_arguments_than_parameters_is_an_arity_error_on_every_path() {
	// Procedures reject extra arguments with FUNCTION_002, so a udf must never drop them silently.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: 1 }]");
	let udf = "UDF f ($x: utf8) { RETURN $x }; ";
	let paths = [
		("script call", format!("{udf}let $v = f('a', 'b'); map {{ v: $v }}")),
		("map without input", format!("{udf}map {{ v: f('a', 'b') }}")),
		("udf over rows", format!("{udf}FROM test::t | map {{ v: f('a', 'b') }}")),
	];

	for (path, rql) in paths {
		let result = t.inner().query_as(TestEngine::identity(), &rql, Params::None);

		let Some(err) = result.error else {
			panic!("{path}: two arguments for one parameter must fail, got {:?}", result.frames);
		};
		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "FUNCTION_002", "{path}: {diagnostic:?}");
	}
}
