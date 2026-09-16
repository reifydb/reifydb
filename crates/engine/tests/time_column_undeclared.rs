// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, fragment::Fragment, params::Params};

const RQL: &str = "FROM test::t | map { #time }";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");
	t.command("INSERT test::t [{ id: 1 }]");
	t
}

fn time_error(t: &TestEngine) -> Diagnostic {
	let result = t.inner().query_as(TestEngine::identity(), RQL, Params::None);
	let Some(err) = result.error else {
		panic!("#time on an object without a time declaration must be an error, got {:?}", result.frames);
	};
	err.diagnostic()
}

#[test]
fn reading_time_from_an_object_without_a_time_declaration_reports_column_not_found() {
	// A none #time compares equal to another none, so a test reading it would pass without proving anything.
	let t = engine();

	let err = time_error(&t);

	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
}

#[test]
fn a_time_column_not_found_error_points_at_the_time_reference_in_the_query() {
	// The fragment must carry the #time position, otherwise a client cannot underline the reference.
	let t = engine();

	let err = time_error(&t);

	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert!(
		matches!(err.fragment, Fragment::Statement { .. }),
		"the fragment must carry a position, got: {:?}",
		err.fragment
	);
	let offset = RQL.find("#time").unwrap();
	let column = *err.fragment.column() as usize;
	assert!(
		column == offset + 1 || column == offset + 2,
		"the fragment must point at #time (column {} or {}), got column {column}",
		offset + 1,
		offset + 2
	);
}
