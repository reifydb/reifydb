// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	value::{Value, frame::frame::Frame},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: int4 }");
	t.admin("CREATE TABLE test::u { c: int4, e: int4 }");
	t.command("INSERT test::t [{ a: 1, b: 10 }, { a: 2, b: 20 }]");
	t.command("INSERT test::u [{ c: 2, e: 200 }, { c: 3, e: 300 }]");
	t
}

fn rows(frames: &[Frame], names: &[&str]) -> Vec<Vec<Value>> {
	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	let frame = &frames[0];
	let columns: Vec<_> = names
		.iter()
		.map(|name| {
			frame.columns
				.iter()
				.find(|c| c.name == *name)
				.unwrap_or_else(|| panic!("no column {name} in\n{frame}"))
		})
		.collect();
	let row_count = columns.first().map(|c| c.data.len()).unwrap_or(0);
	(0..row_count).map(|row| columns.iter().map(|c| c.data.get_value(row)).collect()).collect()
}

#[test]
fn natural_join_on_a_column_written_in_two_places_matches_the_equivalent_using_join() {
	// Each extend names k at its own position, so matching names by position would find no common column.
	let t = engine();
	let names = ["a", "b", "k", "s_c", "s_e"];

	let natural = t.query("FROM test::t | extend { k: a } NATURAL JOIN { FROM test::u | extend { k: c } } AS s");
	let using = t.query(
		"FROM test::t | extend { k: a } INNER JOIN { FROM test::u | extend { k: c } } AS s USING (k, s.k)",
	);

	let expected = vec![vec![Value::Int4(2), Value::Int4(20), Value::Int4(2), Value::Int4(2), Value::Int4(200)]];
	assert_eq!(rows(&using, &names), expected, "using join:\n{}", using[0]);
	assert_eq!(rows(&natural, &names), expected, "natural join:\n{}", natural[0]);
	assert!(
		natural[0].columns.iter().all(|c| c.name != "s_k"),
		"a natural join must emit its common column once:\n{}",
		natural[0]
	);
}

