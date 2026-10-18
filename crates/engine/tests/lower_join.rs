// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t1 { id: int4, a: Option(int4) }");
	t.admin("CREATE TABLE test::t2 { id: int4, a: Option(int4) }");
	t.command("INSERT test::t1 [{ id: 1, a: 5 }, { id: 2, a: none }]");
	t.command("INSERT test::t2 [{ id: 1, a: 5 }, { id: 2, a: none }]");
	t
}

fn joined_ids(t: &TestEngine, rql: &str) -> Vec<i32> {
	let frames = t.query(rql);
	let mut ids: Vec<i32> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	ids.sort();
	ids
}

#[test]
fn a_join_condition_sees_the_column_type_on_a_value_row() {
	// Without the column's field, a value row of an Option column reads as its inner type.
	let t = engine();
	let frames = t.query("FROM test::t1 filter id == 1 map { t: meta::type(a) }");
	let map_type = frames[0].rows().next().unwrap().get::<String>("t").unwrap().unwrap();

	let ids = joined_ids(
		&t,
		&format!(
			"FROM test::t1 filter id == 1 join {{ from test::t2 }} as other using (id, other.id) and (meta::type(a), \"{map_type}\")"
		),
	);

	assert_eq!(ids, vec![1]);
}

#[test]
fn a_none_and_a_value_row_join_alike() {
	// A none on either side must reject the pair, an equal value must keep it.
	let t = engine();

	let ids = joined_ids(&t, "FROM test::t1 join { from test::t2 } as other using (id, other.id) and (a, other.a)");

	assert_eq!(ids, vec![1]);
}

#[test]
fn a_hash_join_residual_takes_the_lowered_path() {
	// A hash residual must answer between per pair over a nullable column, or a none row fails the join.
	let t = engine();

	let ids = joined_ids(
		&t,
		"FROM test::t1 join { from test::t2 } as other using (id, other.id) and (a between 2 and 10, true)",
	);

	assert_eq!(ids, vec![1]);
}

#[test]
fn a_nested_loop_condition_takes_the_lowered_path() {
	// An or leaves no equi key, so the nested loop must answer the between per pair over a nullable column.
	let t = engine();

	let ids = joined_ids(
		&t,
		"FROM test::t1 join { from test::t2 } as other using (id, other.id) or (a between 2 and 10, true)",
	);

	assert_eq!(ids, vec![1, 1, 2]);
}

#[test]
fn a_hash_residual_skips_a_failing_leaf_after_a_rejecting_one() {
	// The hash residual must stop at the first rejecting leaf, so a later division by zero never runs.
	let t = engine();

	let ids = joined_ids(
		&t,
		"FROM test::t1 join { from test::t2 } as other using (id, other.id) and (a, 99) and (a / 0, 1)",
	);

	assert_eq!(ids, Vec::<i32>::new());
}
