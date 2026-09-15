// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, params::Params};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: int4 }");
	t.command("INSERT test::t [{ a: 1, b: 10 }, { a: 2, b: 20 }]");
	t
}

fn query_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
	}
}

#[test]
fn a_hash_join_on_an_unknown_right_key_column_is_column_not_found() {
	// The nested loop already reports QUERY_001 for s.nope, so the hash join must too, never panic.
	let t = engine();

	let err = query_err(&t, "FROM test::t INNER JOIN { FROM test::t } AS s USING (a, s.nope)");

	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert_eq!(err.fragment.text(), "s.nope", "got: {err:?}");
}

#[test]
fn a_hash_join_on_an_unknown_left_key_column_is_column_not_found() {
	// Any name can be typed on the left of a USING pair, so a missing one must be QUERY_001, never a panic.
	let t = engine();

	let err = query_err(&t, "FROM test::t INNER JOIN { FROM test::t } AS s USING (nope, s.a)");

	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert_eq!(err.fragment.text(), "nope", "got: {err:?}");
}
