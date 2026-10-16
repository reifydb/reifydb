// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: Option(int4) }");
	t.command("INSERT test::t [{ id: 1, a: 5 }, { id: 2, a: none }, { id: 3, a: 20 }]");
	t
}

fn ids(t: &TestEngine, rql: &str) -> Vec<i32> {
	let frames = t.query(rql);
	let mut ids: Vec<i32> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	ids.sort();
	ids
}

#[test]
fn a_prefix_plus_lowers_with_the_rest_of_its_expression() {
	// A plus left unclaimed sends the whole filter to the old path, where between over a nullable column errors.
	let t = engine();

	assert_eq!(ids(&t, "FROM test::t FILTER +a between 2 and 10"), vec![1]);
}
