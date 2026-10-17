// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine() -> TestEngine {
	// One row divides by zero, so any map that sees it must fail.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE pd");
	engine.admin("CREATE TABLE pd::t { id: int4, a: int4 }");
	engine.command("INSERT pd::t [{ id: 1, a: 0 }, { id: 2, a: 5 }]");
	engine
}

#[test]
fn a_map_over_the_zero_row_errors() {
	// Without the filter the map sees a = 0, so the error must stay; pushdown never hides it.
	let engine = engine();
	let err = engine.query_err("FROM pd::t MAP { a, x: 10 / a }");
	assert!(err.contains("NUMBER_007"), "expected NUMBER_007, got: {err}");
}

#[test]
fn a_filter_after_the_map_drops_the_zero_row_first() {
	// The filter moves below the map, so the zero row is gone before the division; a deliberate behaviour change.
	let engine = engine();
	let frames = engine.query("FROM pd::t MAP { a, x: 10 / a } FILTER { a != 0 }");
	let text = frames.iter().map(|f| f.to_string()).collect::<String>();
	assert!(text.contains("|  5  |  2  |"), "expected the a = 5 row with x = 2, got:\n{text}");
	assert!(!text.contains("|  0  |"), "the a = 0 row must be filtered out, got:\n{text}");
}

#[test]
fn an_update_with_filter_true_still_updates_every_row() {
	// Dropping the always-true filter must keep every row in the update, never fewer.
	let engine = engine();
	engine.command("UPDATE pd::t { a: 7 } FILTER { true }");
	let text = engine.query("FROM pd::t FILTER { a == 7 }").iter().map(|f| f.to_string()).collect::<String>();
	assert!(
		text.contains("|  1   |  7  |") && text.contains("|  2   |  7  |"),
		"both rows must be updated, got:\n{text}"
	);
}

#[test]
fn a_delete_with_filter_true_still_deletes_every_row() {
	// Dropping the always-true filter must keep every row in the delete, never fewer.
	let engine = engine();
	engine.command("DELETE pd::t FILTER { true }");
	let text = engine.query("FROM pd::t").iter().map(|f| f.to_string()).collect::<String>();
	assert!(!text.contains("|  1   |") && !text.contains("|  2   |"), "no row may survive, got:\n{text}");
}
