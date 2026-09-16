// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE EVENT test::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE test::t { g: int4 }");
	t.admin("CREATE TABLE test::sink { g: int4 }");
	t.command("INSERT test::t [{ g: 1 }, { g: 2 }]");
	t
}

#[test]
fn a_handler_body_accepts_a_pipe_like_a_procedure_body() {
	// A handler body is parsed by its own loop, so a pipe must not be rejected there either.
	let t = engine();

	let create = "CREATE HANDLER test::piped ON test::evt::Foo { FROM test::t | filter { g == 2 } }";
	let result = t.inner().admin_as(TestEngine::identity(), create, Params::None);

	assert!(result.error.is_none(), "a piped handler body must be accepted, got: {:?}", result.error);
}

#[test]
fn a_handler_body_accepts_several_piped_steps() {
	// The loop must keep parsing after every pipe, otherwise a chain is cut short at the first one.
	let t = engine();

	let create =
		"CREATE HANDLER test::piped_chain ON test::evt::Foo { FROM test::t | filter { g == 2 } | map { g } }";
	let result = t.inner().admin_as(TestEngine::identity(), create, Params::None);

	assert!(result.error.is_none(), "a piped handler body must be accepted, got: {:?}", result.error);
}
