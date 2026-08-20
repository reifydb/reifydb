// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn seeded() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::rows { id: int4 }");
	t.command("INSERT test::rows [{id: 1}, {id: 2}, {id: 3}, {id: 4}, {id: 5}]");
	t
}

fn ids(frames: &[reifydb_value::value::frame::frame::Frame]) -> Vec<i32> {
	frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect()
}

#[test]
fn sort_by_a_system_column_survives_fusion_with_take() {
	// `sort | take` compiles to a top-k node, which must resolve system columns exactly as a bare sort does.
	let t = seeded();

	let sorted = t.query("FROM test::rows | sort {rownum:ASC}");
	let fused = t.query("FROM test::rows | sort {rownum:ASC} | take 5");

	assert_eq!(ids(&sorted), ids(&fused), "adding a take must not change which rows a system-column sort returns");
}

#[test]
fn top_k_orders_by_created_at_when_it_is_the_sort_key() {
	// Without system-column resolution the top-k node reports the key as an unknown column and the query fails.
	let t = seeded();

	let frames = t.query("FROM test::rows | sort {created_at:DESC, rownum:DESC} | take 2");

	assert_eq!(ids(&frames), vec![5, 4], "tied created_at must fall back to the highest row numbers");
}

#[test]
fn top_k_below_its_limit_takes_the_same_path_for_system_columns() {
	// A row count under the limit routes through sort_all instead of the heap, and it must resolve keys the same
	// way.
	let t = seeded();

	let frames = t.query("FROM test::rows | sort {rownum:DESC} | take 100");

	assert_eq!(ids(&frames), vec![5, 4, 3, 2, 1], "the under-limit path must honour the same system-column key");
}
