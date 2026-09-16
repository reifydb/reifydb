// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{fragment::Fragment, params::Params};

#[test]
fn an_undefined_name_in_a_procedure_call_argument_reports_its_position() {
	// Without the fragment a client cannot underline which argument named an undefined variable.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE PROCEDURE ns::echo { value: utf8, kind: utf8 } AS { map { r: $kind } }");

	let result = t.inner().command_as(
		TestEngine::identity(),
		"let $r = ns::echo('a', typo); map { r: $r }",
		Params::None,
	);

	let Some(err) = result.error else {
		panic!("typo is not defined, so the call must fail, got {:?}", result.frames);
	};
	let diagnostic = err.diagnostic();
	assert!(
		matches!(diagnostic.fragment, Fragment::Statement { .. }),
		"the fragment must carry a position, got: {diagnostic:?}"
	);
	assert_eq!(diagnostic.fragment.text(), "typo", "got: {diagnostic:?}");
}
