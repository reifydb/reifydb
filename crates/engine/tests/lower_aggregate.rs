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

#[test]
fn a_computed_slot_input_takes_the_lowered_path() {
	// A computed slot input must answer between per row over a nullable column, or a none row fails the batch.
	// The none row stays none, so count sees only the two defined rows.
	let t = engine();

	let frames = t.query("FROM test::t aggregate { n: math::count(a between 2 and 10) }");
	let n = frames[0].rows().next().unwrap().get::<i64>("n").unwrap().unwrap();

	assert_eq!(n, 2);
}

#[test]
fn a_post_aggregate_projection_keeps_its_type_value_and_error() {
	// Every node a map entry allows answers alike on both paths, so this pins the result, not the path.
	let t = engine();

	let frames = t.query("FROM test::t aggregate { v: math::sum(a) + 1 }");
	let column = frames[0].column("v").unwrap().expect("column v");
	assert_eq!(column.get_type().to_string(), "Int8");
	assert_eq!(column.get_value(0).to_string(), "26");

	let err = t.query_err("FROM test::t aggregate { v: math::sum(a) / 0 }");
	assert!(err.contains("NUMBER_007"), "{err}");
}
