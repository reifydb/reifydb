// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;

const TIMEOUT: StdDuration = StdDuration::from_secs(5);

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, latency: Option(float8), weight: float8 }");
	db
}

fn create_view_error(db: &TestDb, accuracy: &str) -> String {
	db.try_admin(&format!(
		"CREATE DEFERRED VIEW app::v {{ g: int4, d: Option(float8) }} AS {{ FROM app::t | aggregate {{ d: stats::digest(latency, {accuracy}) }} by {{ g }} }}"
	))
	.expect_err("a bad digest accuracy must be refused when the view is created")
	.to_string()
}

fn text(db: &TestDb, rql: &str, name: &str) -> Vec<String> {
	let frames = db.query(rql);
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	column_values(&frames[0], name).iter().map(|v| v.to_string()).collect()
}

fn groups(db: &TestDb, rql: &str) -> Vec<(String, String)> {
	text(db, rql, "g").into_iter().zip(text(db, rql, "x")).collect()
}

fn view_matches_batch(aggregate: &str, column: &str, rows: &str) -> (Vec<(String, String)>, Vec<(String, String)>) {
	let db = setup();
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::v {{ g: int4, x: {column} }} AS {{ FROM app::t | aggregate {{ x: {aggregate} }} by {{ g }} }}"
	));
	db.command(&format!("INSERT app::t [{rows}]"));
	assert!(db.await_all_flows(TIMEOUT), "the flow must process every insert");
	let view = groups(&db, "FROM app::v | sort { g: ASC }");
	let batch =
		groups(&db, &format!("FROM app::t | aggregate {{ x: {aggregate} }} by {{ g }} | sort {{ g: ASC }}"));
	(view, batch)
}

#[test]
fn an_accuracy_that_is_not_a_literal_is_refused_at_view_create() {
	// A per-row accuracy would give every group a differently shaped digest that can never be merged.
	let db = setup();
	for accuracy in ["weight", "0.01 + 0.0"] {
		let message = create_view_error(&db, accuracy);
		assert!(message.contains("FLOW_052"), "accuracy {accuracy} must be a literal error, got: {message}");
	}
	assert!(db.try_query("FROM app::v").is_err(), "a refused view must not exist");
}

#[test]
fn an_accuracy_outside_one_thousandth_to_one_tenth_is_refused_at_view_create() {
	// An accuracy the digest cannot build would only fail on the first row, long after the view was accepted.
	let db = setup();
	for accuracy in ["0", "0.0009", "0.5", "1", "-0.01"] {
		let message = create_view_error(&db, accuracy);
		assert!(message.contains("FLOW_053"), "accuracy {accuracy} must be a range error, got: {message}");
	}
}

#[test]
fn an_accuracy_that_is_not_a_whole_ppm_is_refused_at_view_create() {
	// Rounding a sub-ppm accuracy would silently store a digest at an accuracy nobody asked for.
	let db = setup();
	let message = create_view_error(&db, "0.0100005");
	assert!(message.contains("FLOW_054"), "a sub-ppm accuracy must be a whole-ppm error, got: {message}");
}

#[test]
fn a_view_sum_emits_an_all_none_group_exactly_as_a_batch_query_does() {
	// A view that drops the all-none group disagrees with the same aggregate run as a query.
	let (view, batch) = view_matches_batch(
		"math::sum(latency)",
		"Option(float8)",
		"{ id: 1, g: 1, latency: none, weight: 1.0 }, { id: 2, g: 1, latency: none, weight: 1.0 }, { id: 3, g: 2, latency: 4.5, weight: 1.0 }",
	);
	assert_eq!(batch.len(), 2, "the batch query must emit both groups, got: {batch:?}");
	assert_eq!(view, batch, "the view must emit the all-none group with a none sum");
}

#[test]
fn a_view_count_emits_an_all_none_group_with_zero_exactly_as_a_batch_query_does() {
	// A view that drops the all-none group reports no row where a query reports a count of zero.
	let (view, batch) = view_matches_batch(
		"math::count(latency)",
		"int8",
		"{ id: 1, g: 1, latency: none, weight: 1.0 }, { id: 2, g: 2, latency: 4.5, weight: 1.0 }",
	);
	assert_eq!(batch, vec![("1".to_string(), "0".to_string()), ("2".to_string(), "1".to_string())]);
	assert_eq!(view, batch, "the view must emit the all-none group with a count of zero");
}

#[test]
fn a_view_digest_emits_an_all_none_group_with_a_none_digest_exactly_as_a_batch_query_does() {
	// A digest slot that stays none must still keep its group, otherwise the view loses a group the query shows.
	let (view, batch) = view_matches_batch(
		"stats::digest(latency, 0.01)",
		"Option(float8)",
		"{ id: 1, g: 1, latency: none, weight: 1.0 }, { id: 2, g: 3, latency: none, weight: 1.0 }",
	);
	assert_eq!(batch, vec![("1".to_string(), "none".to_string()), ("3".to_string(), "none".to_string())]);
	assert_eq!(view, batch, "the view must emit every all-none group with a none digest");
}

#[test]
fn retracting_every_row_of_an_all_none_group_removes_the_group_from_the_view() {
	// Keeping all-none groups must not keep a group whose rows are gone, otherwise deletes leave ghosts.
	let db = setup();
	db.admin(
		"CREATE DEFERRED VIEW app::v { g: int4, s: Option(float8), n: int8, d: Option(float8) } AS { FROM app::t | aggregate { s: math::sum(latency), n: math::count(latency), d: stats::digest(latency, 0.01) } by { g } }",
	);
	db.command(
		"INSERT app::t [{ id: 1, g: 1, latency: none, weight: 1.0 }, { id: 2, g: 1, latency: none, weight: 1.0 }, { id: 3, g: 2, latency: none, weight: 1.0 }]",
	);
	db.await_exact_row_count("FROM app::v", 2, TIMEOUT);

	db.command("DELETE app::t FILTER { id == 1 }");
	assert!(db.await_all_flows(TIMEOUT), "the flow must process the first delete");
	assert_eq!(db.row_count("FROM app::v"), 2, "group 1 still has a row, so it must stay");

	db.command("DELETE app::t FILTER { id == 2 }");
	db.await_exact_row_count("FROM app::v", 1, TIMEOUT);
	assert_eq!(text(&db, "FROM app::v", "g"), vec!["2"], "only the group with a live row may remain");
}
