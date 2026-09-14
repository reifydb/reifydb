// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::config::ConfigKey};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::Value;

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
fn aggregate_over_query_memory_limit_surfaces_query_006() {
	// Taking one row keeps the result under the limit, so only uncharged aggregate state may exceed it.
	let t = TestEngine::new();
	insert_wide_rows(&t, 2000);
	set_query_memory_limit(&t, 4096);

	let err = t.query_err("FROM test::wide | aggregate { total: math::sum(n) } by { n } | take 1");
	assert!(err.contains("QUERY_006"), "expected QUERY_006, got: {}", err);
	assert!(err.contains("exceeded its memory limit"), "expected memory-limit message, got: {}", err);
}

#[test]
fn aggregate_counts_the_state_of_every_function_against_the_limit() {
	// Counting only the group table lets a query with many aggregates grow far past the limit unnoticed.
	let t = TestEngine::new();
	insert_wide_rows(&t, 2000);
	set_query_memory_limit(&t, 2 * 1024 * 1024);

	let one = t.query("FROM test::wide | aggregate { s0: math::sum(n) } by { n } | take 1");
	assert_eq!(TestEngine::row_count(&one), 1, "one aggregate over 2000 groups must fit under 2 MiB");

	let many: Vec<String> = (0..50).map(|i| format!("s{i}: math::sum(n)")).collect();
	let err = t.query_err(&format!("FROM test::wide | aggregate {{ {} }} by {{ n }} | take 1", many.join(", ")));
	assert!(err.contains("QUERY_006"), "50 aggregates over 2000 groups must exceed 2 MiB, got: {err}");
}

#[test]
fn aggregate_under_query_memory_limit_still_succeeds() {
	// Charging must stay proportional, or a normal limit would reject every grouped query.
	let t = TestEngine::new();
	insert_wide_rows(&t, 50);
	set_query_memory_limit(&t, 1024 * 1024 * 1024);

	let frames = t.query("FROM test::wide | aggregate { total: math::sum(n) } by { n }");
	assert_eq!(TestEngine::row_count(&frames), 50);
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
