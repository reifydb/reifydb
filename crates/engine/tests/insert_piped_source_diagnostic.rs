// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_piped_insert_source_is_not_blamed_on_an_as_clause() {
	// The statement has no AS clause, so the diagnostic must never send the user to fix one.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4 }");
	t.command("INSERT test::t [{ g: 1 }]");

	let rql = "INSERT test::t FROM test::t | map { g }";
	let result = t.inner().admin_as(TestEngine::identity(), rql, Params::None);
	let blamed_as_clause = result.error.as_ref().is_some_and(|e| e.0.message.contains("AS clause"));
	assert!(!blamed_as_clause, "{rql} must not report an AS clause it does not have, got: {:?}", result.error);
}
