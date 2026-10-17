// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn assert_code(err: &str, code: &str) {
	assert!(err.contains(code), "expected {code}, got: {err}");
}

#[test]
fn a_filter_without_a_source_is_query_015() {
	// Without a source the operator has nothing to read, so it must be refused instead of panicking the engine.
	let engine = TestEngine::new();
	assert_code(&engine.query_err("FILTER { a > 1 }"), "QUERY_015");
}

#[test]
fn a_take_without_a_source_is_query_015() {
	// Every required-input operator must refuse the same way, not only the first one checked.
	let engine = TestEngine::new();
	assert_code(&engine.query_err("TAKE 1"), "QUERY_015");
}

#[test]
fn a_map_without_a_source_still_answers() {
	// A map with no input is a scalar map, so the required-input check must not reach it.
	let engine = TestEngine::new();
	engine.query("MAP { x: 1 + 1 }");
}

#[test]
fn an_append_with_an_empty_block_is_query_015() {
	// An empty append block has nothing to add on the right, so it must be refused instead of panicking the engine.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE mi");
	engine.admin("CREATE TABLE mi::t { id: int4 }");
	assert_code(&engine.query_err("FROM mi::t APPEND { }"), "QUERY_015");
}

#[test]
fn a_join_with_an_empty_block_is_query_015() {
	// An empty join block has no right side to match against, so it must be refused instead of panicking the
	// engine.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE mi");
	engine.admin("CREATE TABLE mi::t { id: int4 }");
	assert_code(&engine.query_err("FROM mi::t INNER JOIN { } AS j USING (id, j.id)"), "QUERY_015");
}
