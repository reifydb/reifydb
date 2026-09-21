// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

fn has_rownum(frames: &[Frame]) -> bool {
	assert_eq!(frames.len(), 1, "a query answers with exactly one frame");
	frames[0].has_row_numbers()
}

fn assert_rownum_both_ways(t: &TestEngine, rql: &str, insert: &str, expected: bool) {
	// Agreement alone would pass if both lost #rownum, so each side is pinned to the expectation.
	let empty = t.query(rql);
	assert_eq!(TestEngine::row_count(&empty), 0, "`{rql}` must start empty");
	assert_eq!(has_rownum(&empty), expected, "empty `{rql}` disagrees on #rownum");

	t.command(insert);
	let full = t.query(rql);
	assert!(TestEngine::row_count(&full) > 0, "`{rql}` must return rows after the insert");
	assert_eq!(has_rownum(&full), expected, "non-empty `{rql}` disagrees on #rownum");
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t
}

fn table() -> TestEngine {
	let t = engine();
	t.admin("CREATE TABLE test::t { id: int4, kind: utf8 }");
	t
}

const INSERT_T: &str = "INSERT test::t [{id: 1, kind: 'a'}, {id: 2, kind: 'b'}]";

#[test]
fn a_table_scan_keeps_rownum_when_empty() {
	// The row scan emits a 0-row batch for an empty table, which must carry the bit its full batches do.
	let t = table();
	assert_rownum_both_ways(&t, "FROM test::t", INSERT_T, true);
}

#[test]
fn a_series_scan_keeps_rownum_when_empty() {
	// Series take a separate scan node, so its empty batch must be marked on its own.
	let t = engine();
	t.admin("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }");
	assert_rownum_both_ways(&t, "FROM test::s", "INSERT test::s [{k: 1, value: 1.5}]", true);
}

#[test]
fn a_ringbuffer_scan_keeps_rownum_when_empty() {
	// A ringbuffer builds its empty batch apart from its row batches, so the two can drift apart.
	let t = engine();
	t.admin("CREATE RINGBUFFER test::r { id: int4 } WITH { capacity: 10 }");
	assert_rownum_both_ways(&t, "FROM test::r", "INSERT test::r [{id: 1}]", true);
}

#[test]
fn a_dictionary_scan_never_shows_rownum() {
	// Dictionary entries carry no row numbers, so an empty dictionary must not invent a #rownum.
	let t = engine();
	t.admin("CREATE DICTIONARY test::codes FOR utf8 AS uint4");
	assert_rownum_both_ways(&t, "FROM test::codes", "INSERT test::codes [{ value: 'a' }]", false);
}

#[test]
fn a_map_over_an_empty_table_keeps_rownum() {
	// Map passes its input's row numbers through, so its header bit must follow the input, never default off.
	let t = table();
	assert_rownum_both_ways(&t, "FROM test::t | map { id }", INSERT_T, true);
}

#[test]
fn an_extend_over_an_empty_table_keeps_rownum() {
	// Extend builds its own headers from the input's, which must carry the bit along.
	let t = table();
	assert_rownum_both_ways(&t, "FROM test::t | extend { twice: id * 2 }", INSERT_T, true);
}

#[test]
fn a_patch_over_an_empty_table_keeps_rownum() {
	// Patch merges its names into the input headers, which must not drop the bit on the way.
	let t = table();
	assert_rownum_both_ways(&t, "FROM test::t | patch { kind: 'z' }", INSERT_T, true);
}

#[test]
fn an_empty_join_keeps_rownum() {
	// A join takes #rownum from its left side, so zero matches must not drop it while the left rows still carry it.
	let t = engine();
	t.admin("CREATE TABLE test::l { k: int4, a: utf8 }");
	t.admin("CREATE TABLE test::r { k: int4, b: utf8 }");
	t.command("INSERT test::l [{k: 1, a: 'x'}]");
	t.command("INSERT test::r [{k: 9, b: 'y'}]");
	assert_rownum_both_ways(
		&t,
		"FROM test::l INNER JOIN { FROM test::r } AS r USING (k, r.k)",
		"INSERT test::r [{k: 1, b: 'z'}]",
		true,
	);
}

#[test]
fn an_empty_aggregate_has_no_rownum() {
	// Aggregate rows are new rows with no row number, so the empty answer must not show one either.
	let t = table();
	assert_rownum_both_ways(&t, "FROM test::t | aggregate { n: math::count(id) } by { kind }", INSERT_T, false);
}
