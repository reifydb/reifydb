// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine() -> TestEngine {
	// Both sides are plain tables and the right one is partitioned, so only the part under test is wrong.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE lk");
	engine.admin("CREATE TABLE lk::level { id: int4, mint: utf8 }");
	engine.admin("CREATE TABLE lk::price { mint: utf8, usd: float8 } WITH { partition: { by: { mint } } }");
	engine
}

fn lookup(block: &str, using: &str, with: &str) -> String {
	format!("FROM lk::level INNER LOOKUP {block} AS p USING {using}{with}")
}

const BLOCK: &str = "{ FROM lk::price }";
const USING: &str = "(mint, p.mint)";
const WITH: &str = " WITH { retention: { left: 10s } }";

fn assert_code(err: &str, code: &str) {
	assert!(err.contains(code), "expected {code}, got: {err}");
}

#[test]
fn inner_lookup_in_a_plain_query_is_lookup_004() {
	// A plain query has no lease and no producer tracker, so reading the right side at a version cannot be exact.
	let engine = engine();
	assert_code(&engine.query_err(&lookup(BLOCK, USING, WITH)), "LOOKUP_004");
}

#[test]
fn left_lookup_in_a_plain_query_is_lookup_004() {
	// The left form reads the right side the same way, so it must be refused the same way.
	let engine = engine();
	let rql = format!("FROM lk::level LEFT LOOKUP {BLOCK} AS p USING {USING}{WITH}");
	assert_code(&engine.query_err(&rql), "LOOKUP_004");
}

#[test]
fn lookup_under_a_map_in_a_plain_query_is_lookup_004() {
	// A projection on top must not hide the lookup from the check.
	let engine = engine();
	let rql = format!("{} MAP {{ id, p_usd }}", lookup(BLOCK, USING, WITH));
	assert_code(&engine.query_err(&rql), "LOOKUP_004");
}

#[test]
fn lookup_in_a_transactional_view_is_lookup_004() {
	// A transactional view has no producer flow and no lease, so it cannot host a lookup.
	let engine = engine();
	let rql = format!(
		"CREATE TRANSACTIONAL VIEW lk::v {{ id: int4, p_usd: float8 }} AS {{ {} MAP {{ id, p_usd }} }}",
		lookup(BLOCK, USING, WITH)
	);
	assert_code(&engine.admin_err(&rql), "LOOKUP_004");
}

#[test]
fn a_filter_in_the_block_is_lookup_005() {
	// The lookup reads the right object's partition directly, so a filter in the block would be silently ignored.
	let engine = engine();
	let block = "{ FROM lk::price | FILTER { usd > 0 } }";
	assert_code(&engine.query_err(&lookup(block, USING, WITH)), "LOOKUP_005");
}

#[test]
fn an_inline_block_is_lookup_005() {
	// Inline rows have no partition layout to read, so only a stored object may sit in the block.
	let engine = engine();
	let block = "{ FROM [{ mint: 'a', usd: 1.0 }] }";
	assert_code(&engine.query_err(&lookup(block, USING, WITH)), "LOOKUP_005");
}

#[test]
fn an_empty_block_is_lookup_005() {
	// An empty block names no right object; it must be an error, never a panic.
	let engine = engine();
	assert_code(&engine.query_err(&lookup("{ }", USING, WITH)), "LOOKUP_005");
}

#[test]
fn an_or_connector_is_lookup_002() {
	// A partition is one key prefix, so alternatives joined by 'or' cannot be answered by one partition read.
	let engine = engine();
	let using = "(mint, p.mint) or (id, p.usd)";
	assert_code(&engine.query_err(&lookup(BLOCK, using, WITH)), "LOOKUP_002");
}

#[test]
fn a_retention_without_left_is_ast_005() {
	// An empty retention block still leaves the left side unbounded.
	let engine = engine();
	assert_code(&engine.query_err(&lookup(BLOCK, USING, " WITH { retention: { } }")), "AST_005");
}

#[test]
fn a_right_retention_is_ast_005() {
	// A lookup keeps no right rows, so a right retention has nothing to expire.
	let engine = engine();
	let with = " WITH { retention: { left: 10s, right: 10s } }";
	assert_code(&engine.query_err(&lookup(BLOCK, USING, with)), "AST_005");
}

#[test]
fn a_join_only_key_is_ast_005() {
	// snapshot, latest and earliest shape a join's right copy, which a lookup does not have.
	let engine = engine();
	for key in ["snapshot: true", "latest: true", "earliest: true"] {
		let with = format!(" WITH {{ retention: {{ left: 10s }}, {key} }}");
		assert_code(&engine.query_err(&lookup(BLOCK, USING, &with)), "AST_005");
	}
}

#[test]
fn a_remote_right_side_is_lookup_001() {
	// A remote object has no local partition to read, and the lookup never pushes down to a remote.
	let engine = engine();
	engine.admin("CREATE REMOTE NAMESPACE far WITH { grpc: 'localhost:50051' }");
	let block = "{ FROM far::price }";
	assert_code(&engine.query_err(&lookup(block, USING, WITH)), "LOOKUP_001");
}

#[test]
fn a_lookup_with_no_left_input_is_an_error() {
	// With nothing on the left there is no row to look up; it must be an error, never a panic.
	let engine = engine();
	let rql = format!("INNER LOOKUP {BLOCK} AS p USING {USING}{WITH}");
	assert_code(&engine.query_err(&rql), "AST_009");
}
