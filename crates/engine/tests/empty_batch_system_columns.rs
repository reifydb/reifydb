// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn empty_table() -> TestEngine {
	// No INSERT: the scan still yields one batch, with zero rows and therefore zero-length system arrays.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::rows { id: int4, kind: utf8 }");
	t
}

#[test]
fn sort_by_created_at_over_an_empty_table_returns_no_rows() {
	// A zero-length system array still covers the batch, so created_at must resolve rather than report not-found.
	let t = empty_table();

	let frames = t.query("FROM test::rows | sort {created_at:DESC}");

	assert_eq!(TestEngine::row_count(&frames), 0);
}

#[test]
fn sort_by_rownum_over_an_empty_table_returns_no_rows() {
	// rownum takes a different branch than the DateTime columns and must apply the same coverage rule.
	let t = empty_table();

	let frames = t.query("FROM test::rows | sort {rownum:DESC}");

	assert_eq!(TestEngine::row_count(&frames), 0);
}

#[test]
fn sort_take_fusion_over_an_empty_table_returns_no_rows() {
	// This is the shape hydration pushes down for a take, so an empty source must not fail the subscription.
	let t = empty_table();

	let frames = t.query("FROM test::rows | sort {created_at:DESC, rownum:DESC} | take 5");

	assert_eq!(TestEngine::row_count(&frames), 0);
}

#[test]
fn filter_on_a_system_column_over_an_empty_table_returns_no_rows() {
	// Expression evaluation resolves system columns through the same path as sort and must not diverge from it.
	let t = empty_table();

	let frames = t.query("FROM test::rows | filter {rownum > 0}");

	assert_eq!(TestEngine::row_count(&frames), 0);
}

#[test]
fn sort_by_created_at_still_resolves_when_rows_are_present() {
	// The empty-batch fix must not change which rows a populated system-column sort returns.
	let t = empty_table();
	t.command("INSERT test::rows [{id: 1, kind: 'a'}, {id: 2, kind: 'b'}, {id: 3, kind: 'c'}]");

	let frames = t.query("FROM test::rows | sort {created_at:DESC, rownum:DESC}");

	assert_eq!(TestEngine::row_count(&frames), 3);
}

#[test]
fn sort_by_created_at_over_an_aggregate_reports_not_found() {
	// An aggregate emits rows without stamps, so a zero-length array no longer covers the batch and must stay
	// unresolvable.
	let t = empty_table();
	t.command("INSERT test::rows [{id: 1, kind: 'a'}, {id: 2, kind: 'a'}, {id: 3, kind: 'b'}]");

	let err = t.query_err("FROM test::rows | aggregate {n: math::count(id)} by {kind} | sort {created_at:DESC}");

	assert!(err.contains("QUERY_001"), "expected column-not-found, got {err}");
}
