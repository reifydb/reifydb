// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::config::ConfigKey};
use reifydb_engine::test_prelude::*;

fn insert_wide_rows(t: &TestEngine, count: usize) {
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::wide { n: int4 }");
	let rows: Vec<String> = (0..count).map(|i| format!("{{n:{i}}}")).collect();
	t.command(&format!("INSERT test::wide [{}]", rows.join(",")));
}

fn set_query_memory_limit(t: &TestEngine, bytes: u64) {
	t.inner()
		.catalog()
		.cache()
		.set_config(ConfigKey::QueryMemoryLimit, CommitVersion(1), Value::Uint8(bytes))
		.expect("failed to set QUERY_MEMORY_LIMIT");
}

#[test]
fn sort_over_query_memory_limit_surfaces_query_006() {
	let t = TestEngine::new();
	insert_wide_rows(&t, 50);
	set_query_memory_limit(&t, 8);

	let err = t.query_err("FROM test::wide SORT {n}");
	assert!(err.contains("QUERY_006"), "expected QUERY_006, got: {}", err);
	assert!(err.contains("exceeded its memory limit"), "expected memory-limit message, got: {}", err);
}

#[test]
fn distinct_over_query_memory_limit_surfaces_query_006() {
	let t = TestEngine::new();
	insert_wide_rows(&t, 50);
	set_query_memory_limit(&t, 8);

	let err = t.query_err("FROM test::wide DISTINCT {n}");
	assert!(err.contains("QUERY_006"), "expected QUERY_006, got: {}", err);
	assert!(err.contains("exceeded its memory limit"), "expected memory-limit message, got: {}", err);
}

#[test]
fn top_k_over_query_memory_limit_surfaces_query_006() {
	let t = TestEngine::new();
	insert_wide_rows(&t, 50);
	set_query_memory_limit(&t, 8);

	let err = t.query_err("FROM test::wide SORT {n} TAKE 5");
	assert!(err.contains("QUERY_006"), "expected QUERY_006, got: {}", err);
	assert!(err.contains("exceeded its memory limit"), "expected memory-limit message, got: {}", err);
}

#[test]
fn sort_under_query_memory_limit_still_succeeds() {
	let t = TestEngine::new();
	insert_wide_rows(&t, 5);
	set_query_memory_limit(&t, 1024 * 1024 * 1024);

	let frames = t.query("FROM test::wide SORT {n}");
	assert_eq!(TestEngine::row_count(&frames), 5);
}

#[test]
fn raising_query_memory_limit_lets_previously_failing_query_succeed() {
	let t = TestEngine::new();
	insert_wide_rows(&t, 50);

	set_query_memory_limit(&t, 8);
	let err = t.query_err("FROM test::wide SORT {n}");
	assert!(err.contains("QUERY_006"), "expected QUERY_006, got: {}", err);

	// query_budget() reads the config key fresh at query start, so raising the limit
	// must let the very same query succeed without a restart.
	set_query_memory_limit(&t, 1024 * 1024 * 1024);
	let frames = t.query("FROM test::wide SORT {n}");
	assert_eq!(TestEngine::row_count(&frames), 50);
}
