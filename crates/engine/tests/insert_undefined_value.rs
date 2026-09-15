// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{fragment::Fragment, params::Params};

#[test]
fn an_insert_value_written_as_undefined_reports_column_not_found_and_stores_nothing() {
	// undefined is not RQL, so it must fail as an unknown name rather than silently store a none.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::codes FOR utf8 AS uint4");
	t.admin("CREATE TABLE test::t { id: int4, code: Option(utf8) with { dictionary: test::codes } }");
	let rql = "INSERT test::t [{ id: 1, code: undefined }]";

	let result = t.inner().command_as(TestEngine::identity(), rql, Params::None);

	let Some(err) = result.error else {
		panic!("undefined must be an error, not a stored none: {:?}", result.frames);
	};
	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert_eq!(err.fragment.text(), "undefined", "the error must name undefined");
	assert!(matches!(err.fragment, Fragment::Statement { .. }), "got: {:?}", err.fragment);
	assert_eq!(TestEngine::row_count(&t.query("FROM test::t")), 0, "a failed insert must store no row");
}
