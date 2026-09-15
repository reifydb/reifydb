// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::latency { g: int4, duration: int4 }");
	t.command("INSERT test::latency [{ g: 1, duration: 5 }, { g: 1, duration: 7 }]");
	t
}

fn column_text(frames: &[Frame], name: &str) -> Vec<String> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	(0..column.data.len()).map(|i| column.data.as_string(i)).collect()
}

#[test]
fn an_aggregate_over_a_column_named_like_a_type_reads_the_column() {
	// A column named duration is common for latency data, and reading it as the type makes the sum fail.
	let t = engine();

	let frames = t.query("FROM test::latency | aggregate { total: math::sum(duration) } by { g }");

	assert_eq!(column_text(&frames, "total"), vec!["12"]);
}

#[test]
fn a_type_argument_stays_a_type_next_to_a_column_of_the_same_name() {
	// is::type takes a value then a type, so only its second position may read a name as a type.
	let t = engine();

	let frames = t.query(
		"FROM test::latency | map { is_int: is::type(duration, int4), is_dur: is::type(duration, duration) }",
	);

	assert_eq!(
		column_text(&frames, "is_int"),
		vec!["true", "true"],
		"the first argument must read the int4 column"
	);
	assert_eq!(
		column_text(&frames, "is_dur"),
		vec!["false", "false"],
		"the second argument must stay the duration type even though a duration column exists"
	);
}

#[test]
fn a_script_call_reads_a_variable_named_like_a_type_at_a_value_position() {
	// Scripts compile calls to VM instructions, so a name at a value position must load the variable, never the
	// type.
	let t = TestEngine::new();

	let frames = t.query("let $duration = 'slow'; let $r = is::type(duration, utf8); map { r: $r }");

	assert_eq!(column_text(&frames, "r"), vec!["true"]);
}

#[test]
fn a_script_type_argument_stays_a_type_next_to_a_variable_of_the_same_name() {
	// Only is::type position 1 reads a name as a type, even when a variable of that name exists.
	let t = TestEngine::new();

	let frames = t.query("let $duration = 'slow'; let $r = is::type(duration, duration); map { r: $r }");

	assert_eq!(column_text(&frames, "r"), vec!["false"]);
}
