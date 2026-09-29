// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine() -> TestEngine {
	// The left side runs on event time so the required left retention is valid and only the shape varies.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE lk");
	engine.admin(
		"CREATE TABLE lk::level { id: int4, at: datetime, mint: utf8, pool: utf8 } WITH { time: event(at) }",
	);
	engine.admin("CREATE TABLE lk::price { mint: utf8, usd: float8 } WITH { partition: { by: { mint } } }");
	engine.admin(
		"CREATE TABLE lk::pool { pool: utf8, pair: utf8, fee: float8 } WITH { partition: { by: { pool, pair } } }",
	);
	engine
}

fn view(name: &str, block: &str, using: &str, with: &str) -> String {
	format!("CREATE DEFERRED VIEW lk::{name} {{ id: int4, usd: float8 }} AS {{ \
		 FROM lk::level \
		 INNER LOOKUP {block} AS p USING {using} WITH {{ {with} }} \
		 MAP {{ id: id, usd: p_usd }} }}")
}

const RETENTION: &str = "retention: { left: 10s }";

fn assert_code(err: &str, code: &str) {
	assert!(err.contains(code), "expected {code}, got: {err}");
}

#[test]
fn a_lookup_on_a_partitioned_table_is_accepted() {
	// Positive control: without it every rejection below could come from a broken base shape.
	let engine = engine();
	engine.admin(&view("ok", "{ FROM lk::price }", "(mint, p.mint)", RETENTION));
}

#[test]
fn a_lookup_on_an_unsorted_partitioned_view_is_accepted() {
	// A table-backed view is the other allowed right side (MD2), so it must not be caught by LOOKUP_001.
	let engine = engine();
	engine.admin(
		"CREATE DEFERRED VIEW lk::pv { mint: utf8, usd: float8 } WITH { partition: { by: { mint } } } AS { FROM lk::price }",
	);
	engine.admin(&view("ok", "{ FROM lk::pv }", "(mint, p.mint)", RETENTION));
}

#[test]
fn a_ringbuffer_right_side_is_lookup_001() {
	// A ringbuffer has no partition layout the lookup can read.
	let engine = engine();
	engine.admin("CREATE RINGBUFFER lk::rb { mint: utf8, usd: float8 } WITH { capacity: 10 }");
	assert_code(&engine.admin_err(&view("v", "{ FROM lk::rb }", "(mint, p.mint)", RETENTION)), "LOOKUP_001");
}

#[test]
fn a_series_right_side_is_lookup_001() {
	// A series is keyed by its series key, not by a partition, so a partition read would miss its rows.
	let engine = engine();
	engine.admin("CREATE SERIES lk::s { ts: int8, mint: utf8, usd: float8 } WITH { key: ts }");
	assert_code(&engine.admin_err(&view("v", "{ FROM lk::s }", "(mint, p.mint)", RETENTION)), "LOOKUP_001");
}

#[test]
fn a_sorted_view_right_side_is_lookup_001() {
	// A sorted view is keyed by its sort columns, so the partition key prefix does not hold its rows.
	let engine = engine();
	engine.admin(
		"CREATE DEFERRED VIEW lk::sv { mint: utf8, usd: float8 } WITH { partition: { by: { mint } } } AS { FROM lk::price SORT { usd } }",
	);
	assert_code(&engine.admin_err(&view("v", "{ FROM lk::sv }", "(mint, p.mint)", RETENTION)), "LOOKUP_001");
}

#[test]
fn using_a_non_partition_column_is_lookup_002() {
	// MD13: a key other than the partition columns cannot be answered by one partition read.
	let engine = engine();
	assert_code(&engine.admin_err(&view("v", "{ FROM lk::price }", "(id, p.usd)", RETENTION)), "LOOKUP_002");
}

#[test]
fn using_only_part_of_the_partition_is_lookup_002() {
	// Half a partition key hashes to a different partition, so the lookup would silently match nothing.
	let engine = engine();
	assert_code(&engine.admin_err(&view("v", "{ FROM lk::pool }", "(pool, p.pool)", RETENTION)), "LOOKUP_002");
}

#[test]
fn using_an_unpartitioned_table_is_lookup_002() {
	// An unpartitioned table has no partition columns, so no using clause can equal them.
	let engine = engine();
	engine.admin("CREATE TABLE lk::flat { mint: utf8, usd: float8 }");
	assert_code(&engine.admin_err(&view("v", "{ FROM lk::flat }", "(mint, p.mint)", RETENTION)), "LOOKUP_002");
}

#[test]
fn using_the_partition_columns_in_another_order_is_accepted() {
	// G1: 'and' order carries no meaning, so a reordered using clause is the same key.
	let engine = engine();
	engine.admin(&view("v", "{ FROM lk::pool }", "(pool, p.pair) and (mint, p.pool)", RETENTION));
}

#[test]
fn a_with_key_other_than_retention_is_ast_005() {
	// A lookup keeps no right copy, so the join's snapshot, latest and earliest keys have no meaning.
	let engine = engine();
	for key in ["snapshot: true", "latest: true", "earliest: true"] {
		let with = format!("{RETENTION}, {key}");
		assert_code(&engine.admin_err(&view("v", "{ FROM lk::price }", "(mint, p.mint)", &with)), "AST_005");
	}
}

#[test]
fn a_right_retention_is_ast_005() {
	// IC3: there are no right rows to expire.
	let engine = engine();
	let with = "retention: { left: 10s, right: 10s }";
	assert_code(&engine.admin_err(&view("v", "{ FROM lk::price }", "(mint, p.mint)", with)), "AST_005");
}

#[test]
fn a_missing_left_retention_is_lookup_006() {
	// IC3: without a left retention the lease never moves and GC keeps every version.
	let engine = engine();
	assert_code(
		&engine.admin_err(&view("v", "{ FROM lk::price }", "(mint, p.mint)", "retention: { }")),
		"LOOKUP_006",
	);
}

#[test]
fn a_filter_in_the_block_is_lookup_005() {
	// The lookup reads the partition directly, so a filter in the block would be silently dropped.
	let engine = engine();
	let block = "{ FROM lk::price | FILTER { usd > 0 } }";
	assert_code(&engine.admin_err(&view("v", block, "(mint, p.mint)", RETENTION)), "LOOKUP_005");
}

#[test]
fn a_map_in_the_block_is_lookup_005() {
	// A projection in the block would change the right columns the lookup never computes.
	let engine = engine();
	let block = "{ FROM lk::price | MAP { mint, usd } }";
	assert_code(&engine.admin_err(&view("v", block, "(mint, p.mint)", RETENTION)), "LOOKUP_005");
}

#[test]
fn dropping_a_table_a_lookup_reads_is_refused() {
	// LT12 (MD35): the lookup reads the table at every step, so it must not vanish under the flow.
	let engine = engine();
	engine.admin(&view("v", "{ FROM lk::price }", "(mint, p.mint)", RETENTION));
	assert_code(&engine.admin_err("DROP TABLE lk::price"), "CA_035");
}

#[test]
fn dropping_a_view_a_lookup_reads_is_refused() {
	// LT12 (MD35): the table-backed view case, like price::usd.
	let engine = engine();
	engine.admin(
		"CREATE DEFERRED VIEW lk::pv { mint: utf8, usd: float8 } WITH { partition: { by: { mint } } } AS { FROM lk::price }",
	);
	engine.admin(&view("v", "{ FROM lk::pv }", "(mint, p.mint)", RETENTION));
	assert_code(&engine.admin_err("DROP VIEW lk::pv"), "CA_036");
}

#[test]
fn dropping_the_right_table_after_the_lookup_view_is_gone_is_allowed() {
	// Control: the refusal must come from the lookup, not from the table being unusable to drop.
	let engine = engine();
	engine.admin(&view("v", "{ FROM lk::price }", "(mint, p.mint)", RETENTION));
	engine.admin("DROP VIEW lk::v");
	engine.admin("DROP TABLE lk::price");
}

#[test]
fn dropping_the_right_namespace_while_another_namespace_looks_it_up_is_refused() {
	// LT12 through a namespace drop: the lookup's right table dies with its namespace.
	let engine = engine();
	engine.admin("CREATE NAMESPACE rn");
	engine.admin("CREATE TABLE rn::price { mint: utf8, usd: float8 } WITH { partition: { by: { mint } } }");
	engine.admin(&view("v", "{ FROM rn::price }", "(mint, p.mint)", RETENTION));
	let err = engine.admin_err("DROP NAMESPACE rn");
	assert!(err.contains("CA_"), "expected a catalog in-use error, got: {err}");
}
