// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn ids(t: &TestEngine) -> Vec<String> {
	let frames = t.query("FROM test::t");
	let column = frames[0].columns.iter().find(|c| c.name == "id").expect("column id");
	(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect()
}

#[test]
fn a_plain_from_returns_rows_newest_first() {
	// Without a sort, a plain from must return rows in descending order, newest row first.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, v: int4 }");
	t.command("INSERT test::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]");

	assert_eq!(ids(&t), ["3", "2", "1"]);
}

#[test]
fn an_update_keeps_the_descending_order_of_a_plain_from() {
	// An update must not reorder rows, so a plain from stays in descending order after it.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, v: int4 }");
	t.command("INSERT test::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]");
	let before = ids(&t);

	t.command("UPDATE test::t { v: 21 } FILTER { id == 2 }");
	let after = ids(&t);

	let descending = ["3", "2", "1"].map(String::from).to_vec();
	assert_eq!((before, after), (descending.clone(), descending));
}
