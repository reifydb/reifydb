// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_udf_called_with_a_list_for_a_utf8_parameter_is_an_error_naming_the_parameter() {
	// A declared parameter type must be enforced, otherwise the body runs on a value of the wrong type.
	let t = TestEngine::new();

	let result = t.inner().query_as(
		TestEngine::identity(),
		"UDF takes_text ($x: utf8) { RETURN 'ok' }; let $v = takes_text([1, 2]); map { v: $v }",
		Params::None,
	);

	let Some(err) = result.error else {
		panic!("a list is not utf8, so the call must fail, got {:?}", result.frames);
	};
	let diagnostic = err.diagnostic();
	assert!(
		matches!(diagnostic.fragment.text(), "x" | "$x"),
		"the error must name the parameter, got: {diagnostic:?}"
	);
}
