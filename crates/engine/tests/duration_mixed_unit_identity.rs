// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

fn ids(frames: &[Frame]) -> Vec<String> {
	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	let column = frames[0].columns.iter().find(|c| c.name == "id").expect("column id");
	(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect()
}

fn row_count(frames: &[Frame]) -> usize {
	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	frames[0].columns.first().map(|c| c.data.len()).unwrap_or(0)
}

#[test]
fn group_by_distinct_sort_and_join_agree_that_a_month_is_not_thirty_days() {
	// Duration identity compares months, then days, then nanos; every operator must follow that one rule.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::d { id: int4, d: duration }");
	t.admin("CREATE TABLE test::k { k: duration }");
	t.command(
		"INSERT test::d [{ id: 1, d: duration::months(1) }, { id: 2, d: duration::days(30) }, { id: 3, d: duration::days(40) }]",
	);
	t.command("INSERT test::k [{ k: duration::days(30) }]");

	let groups = row_count(&t.query("FROM test::d | aggregate { n: math::count(id) } by { d }"));
	let distinct = row_count(&t.query("FROM test::d | distinct { d }"));
	let sorted = ids(&t.query("FROM test::d | sort { d: ASC }"));
	let hash_join =
		ids(&t.query("FROM test::d | inner join { from test::k } as o using (d, o.k) | sort { id: ASC }"));
	let loop_join = ids(&t.query(
		"FROM test::d | inner join { from test::k } as o using (d, o.k) or (d, o.k) | sort { id: ASC }",
	));

	assert_eq!(
		(groups, distinct, sorted, hash_join, loop_join),
		(
			3,
			3,
			vec!["2".to_string(), "3".to_string(), "1".to_string()],
			vec!["2".to_string()],
			vec!["2".to_string()]
		)
	);
}
