// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, a: int4, b: int4 }");
	t.command("INSERT test::t [{ g: 1, a: 2, b: 100 }, { g: 1, a: 3, b: 100 }, { g: 2, a: 10, b: 100 }]");
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
fn a_map_entry_over_an_aggregate_is_computed_per_group() {
	// Flow already accepts arithmetic over an aggregate, so a batch panic here means the same view query dies ad
	// hoc.
	let t = engine();

	let frames = t.query("FROM test::t | aggregate { total: math::sum(a) + 1 } by { g }");

	let mut pairs: Vec<(String, String)> =
		column_text(&frames, "g").into_iter().zip(column_text(&frames, "total")).collect();
	pairs.sort();
	assert_eq!(
		pairs,
		vec![("1".to_string(), "6".to_string()), ("2".to_string(), "11".to_string())],
		"each group must add 1 to its own sum"
	);
}

#[test]
fn a_map_entry_without_an_aggregate_reports_an_error_instead_of_panicking() {
	// A bare column has no single value per group, and the statement worker must not die finding that out.
	let t = engine();

	let err = t.query_err("FROM test::t | aggregate { x: a } by { g }");

	assert!(err.contains("AGGREGATE_009"), "a map entry with no aggregate must report AGGREGATE_009, got: {err}");
}

#[test]
fn a_non_column_group_key_reports_an_error_instead_of_panicking() {
	// A panic on the statement worker takes the whole process down for every client.
	let t = engine();

	let err = t.query_err("FROM test::t | aggregate { n: math::count(a) } by { g + 1 }");

	assert!(err.contains("AGGREGATE_007"), "a non-column group key must report AGGREGATE_007, got: {err}");
}

#[test]
fn extra_arguments_to_an_aggregate_report_too_many_arguments() {
	// Dropping b silently returns sum(a), a plausible but wrong answer the caller never learns about.
	let t = engine();

	let err = t.query_err("FROM test::t | aggregate { s: math::sum(a, b) } by { g }");

	assert!(err.contains("FUNCTION_003"), "math::sum(a, b) must report FUNCTION_003, got: {err}");
}

#[test]
fn an_unknown_aggregate_is_reported_as_an_aggregate() {
	// Calling a missing aggregate a generator sends the reader looking in the wrong registry.
	let t = engine();

	let err = t.query_err("FROM test::t | aggregate { s: math::nope(a) } by { g }");

	assert!(err.contains("FUNCTION_009"), "a missing aggregate must report FUNCTION_009, got: {err}");
	assert!(err.contains("Aggregate function"), "the message must name an aggregate function, got: {err}");
	assert!(!err.contains("Generator"), "the message must not name a generator function, got: {err}");
}

#[test]
fn grouping_by_a_list_column_reports_an_error_instead_of_panicking() {
	// A list cannot be encoded as a key, and the key serializer treats reaching it as unreachable.
	let t = engine();

	let err = t.query_err("FROM test::t | extend { l: [a, b] } | aggregate { n: math::count(a) } by { l }");

	assert!(err.contains("AGGREGATE_008"), "a list group key must report AGGREGATE_008, got: {err}");
}

#[test]
fn two_aggregates_in_one_map_entry_each_feed_their_own_operand() {
	// Swapped slot results turn sum(a) - sum(b) into sum(b) - sum(a) with no error to notice.
	let t = engine();

	let frames = t.query("FROM test::t | aggregate { d: math::sum(a) - math::sum(b) } by { g }");

	let mut pairs: Vec<(String, String)> =
		column_text(&frames, "g").into_iter().zip(column_text(&frames, "d")).collect();
	pairs.sort();
	assert_eq!(
		pairs,
		vec![("1".to_string(), "-195".to_string()), ("2".to_string(), "-90".to_string())],
		"each group must subtract its own sum(b) from its own sum(a)"
	);
}

#[test]
fn grouping_by_an_optional_list_column_reports_an_error_instead_of_panicking() {
	// Wrapping a list in Option does not make it a key, so the check must look inside the Option.
	let t = engine();

	let err = t.query_err(
		"FROM test::t | extend { l: if a > 2 { [a, b] } else { none } } | aggregate { n: math::count(a) } by { l }",
	);

	assert!(err.contains("AGGREGATE_008"), "an optional list group key must report AGGREGATE_008, got: {err}");
}
