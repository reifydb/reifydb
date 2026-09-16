// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_procedure_body_accepts_a_pipe_like_a_test_body() {
	// A piped query parses at top level and in a test body, so a procedure body must not reject the pipe.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4 }");
	t.command("INSERT test::t [{ g: 1 }, { g: 2 }]");

	let create = "CREATE PROCEDURE test::piped AS { FROM test::t | filter { g == 2 } }";
	let result = t.inner().admin_as(TestEngine::identity(), create, Params::None);
	assert!(result.error.is_none(), "a piped procedure body must be accepted, got: {:?}", result.error);

	let frames = t.query("CALL test::piped()");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 1, "the pipe must feed the filter, got: {}", frames[0]);
	assert_eq!(rows[0].get::<i32>("g").unwrap().unwrap(), 2, "got: {}", frames[0]);
}
