// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_test_body_error_quotes_the_failing_body_line() {
	// Without the body attached as RQL the error renders an empty source line and hides what failed.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE TABLE s::t { a: int4 }");
	t.admin("CREATE TEST s::broken { FROM s::t MAP { nope } }");
	let result = t.inner().admin_as(TestEngine::identity(), "RUN TESTS s", Params::None);
	assert!(result.error.is_none(), "RUN TESTS itself must not fail, got: {:?}", result.error);
	let rows: Vec<_> = result.frames[0].rows().collect();
	assert_eq!(rows.len(), 1, "exactly the one test must run, got: {}", result.frames[0]);
	assert_eq!(rows[0].get::<String>("outcome").unwrap().unwrap(), "error", "got: {}", result.frames[0]);
	let message = rows[0].get::<String>("message").unwrap().unwrap();
	assert!(
		message.lines().any(|line| line.contains('│') && line.contains("MAP { nope }")),
		"the error must quote the body line it points at, got: {message}"
	);
}
