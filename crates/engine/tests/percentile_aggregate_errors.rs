// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{frame::frame::Frame, identity::IdentityId};

fn create_table(t: &TestEngine) {
	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE TABLE test::t { g: int4, f: Option(float8), a: int4, d: Option(duration), ts: Option(datetime) }",
	);
}

fn empty_engine() -> TestEngine {
	let t = TestEngine::new();
	create_table(&t);
	t
}

fn engine() -> TestEngine {
	let t = empty_engine();
	t.command(
		"INSERT test::t [{ g: 1, f: 1.5, a: 1, d: 5ms, ts: '2024-01-01T00:00:00Z' }, { g: 2, f: 2.5, a: 2, d: 7ms, ts: '2024-01-02T00:00:00Z' }]",
	);
	t
}

fn assert_error(err: &str, code: &str, message: &str) {
	assert!(err.contains(&format!("code: \"{code}\"")), "expected {code}, got: {err}");
	assert!(err.contains(&format!("message: \"{message}\"")), "expected message {message:?}, got: {err}");
}

fn help_of(t: &TestEngine, rql: &str, code: &str) -> Option<String> {
	let error = t
		.query_as(IdentityId::system(), rql, Default::default())
		.error
		.unwrap_or_else(|| panic!("the query must fail with {code}: {rql}"));
	assert_eq!(error.0.code, code, "{rql}: {error:?}");
	error.0.help.clone()
}

fn row_count(frames: &[Frame]) -> usize {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	TestEngine::row_count(frames)
}

#[test]
fn p_above_one_fails_before_any_row_is_read() {
	// On an empty table no row is read, so the range error must come from the literal alone.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, 1.5, 0.01) } by { g }");

	assert_error(&err, "AGGREGATE_020", "p must be between 0 and 1");
	assert!(err.contains("\"1.5\""), "the error must point at the p literal, got: {err}");
}

#[test]
fn a_negative_p_reports_the_range_error_instead_of_the_literal_error() {
	// -0.5 is written as a literal, so it must never be reported as a non-literal argument.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, -0.5, 0.01) }");

	assert_error(&err, "AGGREGATE_020", "p must be between 0 and 1");
	assert!(err.contains("\"-0.5\""), "the error must point at the whole negated literal, got: {err}");
}

#[test]
fn p_out_of_range_on_the_merge_form_fails_before_any_row_is_read() {
	// The two-argument form must check p too, otherwise a bad p only fails once a digest reaches the read.
	let t = empty_engine();

	let err = t.query_err(
		"FROM test::t | aggregate { lat: stats::digest(d, 0.01) } by { g } | aggregate { p: stats::approx_percentile(lat, 2) }",
	);

	assert_error(&err, "AGGREGATE_020", "p must be between 0 and 1");
}

#[test]
fn a_column_p_fails_before_any_row_is_read() {
	// A per-row p can never be read from one shared digest slot, so a column p must fail.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, a, 0.01) } by { g }");

	assert_error(
		&err,
		"AGGREGATE_010",
		"argument 2 of aggregate function stats::approx_percentile must be a literal",
	);
}

#[test]
fn an_expression_p_fails_before_any_row_is_read() {
	// A literal wrapped in arithmetic is still not a literal, so it must not be folded into one silently.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, a / 2, 0.01) }");

	assert_error(
		&err,
		"AGGREGATE_010",
		"argument 2 of aggregate function stats::approx_percentile must be a literal",
	);
}

#[test]
fn a_text_p_fails_before_any_row_is_read() {
	// A quoted '0.99' must not be parsed as a number the user never wrote as one.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, '0.99', 0.01) }");

	assert_error(&err, "AGGREGATE_021", "p must be a number");
}

#[test]
fn an_accuracy_out_of_range_fails_before_any_row_is_read() {
	// The accuracy on approx_percentile must be checked like the one on stats::digest, before any row.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, 0.99, 0.5) } by { g }");

	assert_error(&err, "AGGREGATE_011", "accuracy must be between 0.001 and 0.1");
	assert!(err.contains("\"0.5\""), "the error must point at the accuracy literal, got: {err}");
}

#[test]
fn an_accuracy_that_is_not_a_whole_ppm_fails_before_any_row_is_read() {
	// 0.0000005 must never round to a whole ppm and silently share a slot with another accuracy.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, 0.99, 0.0000005) }");

	assert_error(&err, "AGGREGATE_012", "accuracy must be a whole number of parts per million");
}

#[test]
fn a_column_accuracy_fails_before_any_row_is_read() {
	// A per-row accuracy can never become one digest type, so a column accuracy must fail at position 3.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, 0.99, a) } by { g }");

	assert_error(
		&err,
		"AGGREGATE_010",
		"argument 3 of aggregate function stats::approx_percentile must be a literal",
	);
}

#[test]
fn a_text_accuracy_fails_before_any_row_is_read() {
	// A quoted accuracy must fail as not a number, never be parsed into a ppm.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, 0.99, '0.01') }");

	assert_error(&err, "AGGREGATE_013", "accuracy must be a number");
}

#[test]
fn a_fourth_argument_fails_before_any_row_is_read() {
	// An extra argument must fail, otherwise a typo would run with an argument silently ignored.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, 0.99, 0.01, 0.01) }");

	assert!(err.contains("FUNCTION_003"), "expected FUNCTION_003, got: {err}");
	assert!(err.contains("stats::approx_percentile"), "the error must name the called function, got: {err}");
}

#[test]
fn a_missing_p_fails_before_any_row_is_read() {
	// Without p there is nothing to read from the digest, so the call must not build a slot and fail later.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f) }");

	assert!(err.contains("FUNCTION_002"), "expected FUNCTION_002, got: {err}");
	assert!(err.contains("stats::approx_percentile"), "the error must name the called function, got: {err}");
}

#[test]
fn a_p_error_wins_over_an_accuracy_error_in_the_same_call() {
	// p is checked first, so a call with both wrong must report p and never the accuracy.
	let t = empty_engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, 1.5, 0.5) }");

	assert_error(&err, "AGGREGATE_020", "p must be between 0 and 1");
}

#[test]
fn a_datetime_input_fails_on_the_first_value_naming_approx_percentile() {
	// The user wrote approx_percentile, so the hidden stats::digest slot must never leak into the message.
	let t = engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(ts, 0.99, 0.01) }");

	assert_error(&err, "AGGREGATE_016", "stats::approx_percentile not supported for DateTime");
}

#[test]
fn a_raw_input_without_accuracy_fails_on_the_first_value() {
	// Without an accuracy there is no digest type to build, so a raw value must not pick a default.
	let t = engine();

	let err = t.query_err("FROM test::t | aggregate { p: stats::approx_percentile(f, 0.99) } by { g }");

	assert_error(&err, "AGGREGATE_014", "accuracy is required for a non-digest input");
}

#[test]
fn a_digest_input_with_accuracy_fails_on_the_first_value() {
	// The digest type already fixes the accuracy, so a written one must fail instead of being ignored.
	let t = engine();

	let err = t.query_err(
		"FROM test::t | aggregate { lat: stats::digest(d, 0.01) } by { g } | aggregate { p: stats::approx_percentile(lat, 0.99, 0.01) }",
	);

	assert_error(&err, "AGGREGATE_015", "accuracy comes from the digest type, remove the argument");
}

#[test]
fn type_errors_do_not_fire_on_an_empty_table_until_the_typed_plan_pass_exists() {
	// These queries must fail once types are known at plan time; a passing run here means P has not landed yet.
	let t = empty_engine();

	let datetime = t.query("FROM test::t | aggregate { p: stats::approx_percentile(ts, 0.99, 0.01) } by { g }");
	let no_accuracy = t.query("FROM test::t | aggregate { p: stats::approx_percentile(f, 0.99) } by { g }");
	let digest_with_accuracy = t.query(
		"FROM test::t | aggregate { lat: stats::digest(d, 0.01) } by { g } | aggregate { p: stats::approx_percentile(lat, 0.99, 0.01) } by { g }",
	);

	assert_eq!(row_count(&datetime), 0, "a grouped aggregate over no rows must give no row");
	assert_eq!(row_count(&no_accuracy), 0, "a grouped aggregate over no rows must give no row");
	assert_eq!(row_count(&digest_with_accuracy), 0, "a grouped aggregate over no rows must give no row");
}

#[test]
fn a_type_error_on_an_empty_table_without_by_passes_like_any_aggregate_until_the_typed_plan_pass_exists() {
	// The percentile must give the same rows as math::count over no rows, never an error or an invented row.
	let t = empty_engine();

	let frames = t.query("FROM test::t | aggregate { p: stats::approx_percentile(ts, 0.99, 0.01) }");
	let counted = t.query("FROM test::t | aggregate { n: math::count(a) }");

	assert_eq!(row_count(&frames), row_count(&counted), "the percentile must follow the aggregate row rule");
	assert!(frames[0].columns.iter().any(|c| c.name == "p"), "the read column must still come back");
}

#[test]
fn a_digest_key_from_an_upstream_aggregate_already_fails_on_an_empty_table() {
	// The upstream aggregate types its empty digest column, so by and sort must reject it even with no row read.
	let t = empty_engine();

	let by = t.query_err(
		"FROM test::t | aggregate { lat: stats::digest(d, 0.01) } by { g } | aggregate { n: math::count(g) } by { lat }",
	);
	let sort = t.query_err("FROM test::t | aggregate { lat: stats::digest(d, 0.01) } by { g } | sort { lat }");

	assert!(by.contains("code: \"AGGREGATE_008\""), "expected AGGREGATE_008, got: {by}");
	assert!(sort.contains("code: \"SORT_002\""), "expected SORT_002, got: {sort}");
}

#[test]
fn accuracy_help_shows_the_syntax_of_the_function_the_user_wrote() {
	// The help must show the call the user wrote, otherwise a percentile user is told to write stats::digest.
	let t = engine();
	let digests = "FROM test::t | aggregate { lat: stats::digest(f, 0.01) } by { g } | aggregate";
	let cases = [
		(
			"FROM test::t | aggregate { p: stats::approx_percentile(f, 0.99) } by { g }".to_string(),
			"AGGREGATE_014",
			"Add an accuracy argument, e.g., 'stats::approx_percentile(latency, 0.99, 0.01)'",
		),
		(
			"FROM test::t | aggregate { d: stats::digest(f) } by { g }".to_string(),
			"AGGREGATE_014",
			"Add an accuracy argument, e.g., 'stats::digest(latency, 0.01)'",
		),
		(
			format!("{digests} {{ p: stats::approx_percentile(lat, 0.99, 0.01) }} by {{ g }}"),
			"AGGREGATE_015",
			"Merge digests without an accuracy, e.g., 'stats::approx_percentile(lat, 0.99)'",
		),
		(
			format!("{digests} {{ d: stats::digest(lat, 0.01) }} by {{ g }}"),
			"AGGREGATE_015",
			"Merge digests without an accuracy, e.g., 'stats::digest(lat)'",
		),
	];
	for (rql, code, help) in cases {
		assert_eq!(help_of(&t, &rql, code).as_deref(), Some(help), "{rql}");
	}
}
