// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

fn row_count(frames: &[Frame]) -> usize {
	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	frames[0].columns.first().map(|c| c.data.len()).unwrap_or(0)
}

#[test]
fn distinct_over_all_columns_keeps_rows_whose_text_only_matches_when_concatenated() {
	// Joining rendered values without a boundary makes ("ab", "c") and ("a", "bc") one row.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::s { x: utf8, y: utf8 }");
	t.command(r#"INSERT test::s [{ x: "ab", y: "c" }, { x: "a", y: "bc" }]"#);

	let frames = t.query("FROM test::s | distinct {}");

	assert_eq!(row_count(&frames), 2, "two different rows must both survive distinct:\n{}", frames[0]);
}

#[test]
fn distinct_on_named_columns_keeps_rows_whose_text_only_matches_when_concatenated() {
	// The named-key path renders and joins values the same way, so it must keep the boundary too.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::s { x: utf8, y: utf8 }");
	t.command(r#"INSERT test::s [{ x: "ab", y: "c" }, { x: "a", y: "bc" }]"#);

	let frames = t.query("FROM test::s | distinct { x, y }");

	assert_eq!(row_count(&frames), 2, "two different rows must both survive distinct:\n{}", frames[0]);
}

#[test]
fn distinct_keeps_a_none_apart_from_the_text_none() {
	// A missing value and the four letters "none" render alike but are different values.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::s { x: Option(utf8) }");
	t.command(r#"INSERT test::s [{ x: none }, { x: "none" }]"#);

	let frames = t.query("FROM test::s | distinct { x }");

	assert_eq!(row_count(&frames), 2, "none and \"none\" must both survive distinct:\n{}", frames[0]);
}
