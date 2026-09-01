// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// window::start, window::end, window::duration and window::last name the bucket a windowed row
// aggregated. These tests pin what each one answers and which window kinds may ask.

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::{column_values, timed_rows};
use reifydb_value::value::Value;

const TIMEOUT: StdDuration = StdDuration::from_secs(5);

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

fn source(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
}

fn insert(db: &TestDb, id: i32, g: i32, v: i32, ts: &str) {
	db.command(&format!(r#"INSERT app::t [{{ id: {id}, g: {g}, v: {v}, ts: "{ts}" }}]"#));
}

fn text(values: &[Value]) -> Vec<String> {
	values.iter().map(|value| value.to_string()).collect()
}

#[test]
fn start_and_end_report_the_bucket_the_rows_fell_into() {
	// Two trades at 00:01:10 and 00:01:50 share the 60s bucket that runs [00:01:00, 00:02:00); the
	// trade at 00:02:30 belongs to the next one. Start and end must name that bucket, not the first
	// and last trade inside it, and one window's end must be the next window's start with no gap and
	// no overlap - a boundary that drifts to the data would leave holes between consecutive buckets.
	let db = setup();
	source(&db);
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, n: int8, s: datetime, e: datetime } AS {
			FROM app::t
				| window tumbling { n: math::count(), s: window::start(), e: window::end() }
					by { g } with { duration: 60s, lateness: 0s }
		}"#);

	insert(&db, 1, 1, 7, "2026-01-01T00:01:10Z");
	insert(&db, 2, 1, 7, "2026-01-01T00:01:50Z");
	insert(&db, 3, 1, 7, "2026-01-01T00:02:30Z");
	db.await_exact_row_count("FROM app::w", 2, TIMEOUT);

	let frames = db.query("FROM app::w");
	let mut buckets: Vec<(String, String, Value)> = text(&column_values(&frames[0], "s"))
		.into_iter()
		.zip(text(&column_values(&frames[0], "e")))
		.zip(column_values(&frames[0], "n"))
		.map(|((start, end), n)| (start, end, n))
		.collect();
	buckets.sort();

	assert_eq!(
		buckets,
		vec![
			(
				"2026-01-01T00:01:00.000000000Z".to_string(),
				"2026-01-01T00:02:00.000000000Z".to_string(),
				Value::Int8(2)
			),
			(
				"2026-01-01T00:02:00.000000000Z".to_string(),
				"2026-01-01T00:03:00.000000000Z".to_string(),
				Value::Int8(1)
			),
		]
	);
}

#[test]
fn a_partial_window_reports_its_whole_span_not_the_part_it_has_filled() {
	// A window emits before it seals, so this row is read while only 10 seconds of a 60 second bucket
	// have happened. End must still be the boundary. Deriving it from the newest row would make every
	// pre-seal read report a shorter bucket, and a consumer bucketing on it would double-count.
	let db = setup();
	source(&db);
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, n: int8, s: datetime, e: datetime } AS {
			FROM app::t
				| window tumbling { n: math::count(), s: window::start(), e: window::end() }
					by { g } with { duration: 60s, lateness: 300s }
		}"#);

	insert(&db, 1, 1, 7, "2026-01-01T00:01:10Z");
	db.await_exact_row_count("FROM app::w", 1, TIMEOUT);

	let frames = db.query("FROM app::w");
	assert_eq!(text(&column_values(&frames[0], "s")), vec!["2026-01-01T00:01:00.000000000Z".to_string()]);
	assert_eq!(text(&column_values(&frames[0], "e")), vec!["2026-01-01T00:02:00.000000000Z".to_string()]);
}

#[test]
fn duration_is_measured_from_the_boundary_not_read_from_the_configured_size() {
	// duration must come out of the emitted span. Returning the configured literal instead would look
	// identical here and be wrong for a session window, whose length is decided by its data.
	let db = setup();
	source(&db);
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, n: int8, d: duration } AS {
			FROM app::t
				| window tumbling { n: math::count(), d: window::duration() }
					by { g } with { duration: 45s, lateness: 0s }
		}"#);

	insert(&db, 1, 1, 7, "2026-01-01T00:01:10Z");
	db.await_exact_row_count("FROM app::w", 1, TIMEOUT);

	let frames = db.query("FROM app::w");
	let durations = column_values(&frames[0], "d");
	assert_eq!(durations.len(), 1);
	assert!(
		durations[0].to_string().contains("45"),
		"a 45s window must report a 45 second duration, got {}",
		durations[0]
	);
}

#[test]
fn window_last_reports_the_newest_event_in_the_bucket_not_the_boundary() {
	// The three answers a bucket can give are its start (00:01:00), its end (00:02:00) and the newest
	// trade it holds (00:01:50). Only the third one changes when a later trade lands, which is the
	// whole reason to ask for it, and it must never run past the end.
	let db = setup();
	source(&db);
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, n: int8, l: datetime } AS {
			FROM app::t
				| window tumbling { n: math::count(), l: window::last() }
					by { g } with { duration: 60s, lateness: 300s }
		}"#);

	insert(&db, 1, 1, 7, "2026-01-01T00:01:50Z");
	insert(&db, 2, 1, 7, "2026-01-01T00:01:10Z");
	db.await_exact_row_count("FROM app::w", 1, TIMEOUT);
	let rows = db.await_row_count("FROM app::w | filter { n == 2 }", 1, TIMEOUT);
	assert_eq!(rows, 1, "both trades must land in the same bucket");

	let frames = db.query("FROM app::w");
	assert_eq!(
		text(&column_values(&frames[0], "l")),
		vec!["2026-01-01T00:01:50.000000000Z".to_string()],
		"the out-of-order trade at 00:01:10 must not rewind the newest event time"
	);
}

#[test]
fn time_still_stamps_the_window_start() {
	// #time carried the window start before window::start() existed and downstream views bucket on it.
	// Threading the span through the emit path must not change what lands in the stamp.
	let db = setup();
	source(&db);
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, n: int8, s: datetime } AS {
			FROM app::t
				| window tumbling { n: math::count(), s: window::start() }
					by { g } with { duration: 60s, lateness: 0s }
		}"#);

	insert(&db, 1, 1, 7, "2026-01-01T00:01:10Z");
	db.await_exact_row_count("FROM app::w", 1, TIMEOUT);

	let frames = db.query("FROM app::w");
	let stamped: Vec<String> = timed_rows(&frames).into_iter().map(|row| row.time.to_string()).collect();
	assert_eq!(stamped, vec!["2026-01-01T00:01:00.000000000Z".to_string()]);
	assert_eq!(text(&column_values(&frames[0], "s")), stamped);
}

#[test]
fn a_rolling_window_is_refused_a_boundary_at_define_time() {
	// A rolling window has no boundary at all - it stamps every emission with the commit time. Letting
	// the view be created would hand every row a none where a bucket start was expected, discovered
	// only by whoever read the view.
	let db = setup();
	source(&db);
	let err = db
		.try_admin(
			r#"CREATE DEFERRED VIEW app::w { g: int4, n: int8, s: datetime } AS {
				FROM app::t
					| window rolling { n: math::count(), s: window::start() }
						by { g } with { duration: 60s }
			}"#,
		)
		.expect_err("a rolling window must not accept window::start");
	let message = err.to_string();
	assert!(message.contains("FLOW_015"), "expected a boundary diagnostic, got: {message}");
}

#[test]
fn a_rolling_window_still_answers_window_last() {
	// window::last needs an event time, not a boundary, so the rolling rejection must not swallow it.
	let db = setup();
	source(&db);
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, n: int8, l: datetime } AS {
			FROM app::t
				| window rolling { n: math::count(), l: window::last() }
					by { g } with { duration: 60s }
		}"#);

	insert(&db, 1, 1, 7, "2026-01-01T00:01:10Z");
	db.await_exact_row_count("FROM app::w", 1, TIMEOUT);

	let frames = db.query("FROM app::w");
	assert_eq!(text(&column_values(&frames[0], "l")), vec!["2026-01-01T00:01:10.000000000Z".to_string()]);
}

#[test]
fn a_grouped_aggregate_is_refused_a_window_function() {
	// An aggregate view has no window, so there is no bucket to name. Accepting it would emit a none
	// column that reads as "this bucket has no start" rather than "you cannot ask that here".
	let db = setup();
	source(&db);
	let err = db
		.try_admin(
			r#"CREATE DEFERRED VIEW app::w { g: int4, n: int8, s: datetime } AS {
				FROM app::t | aggregate { n: math::count(), s: window::start() } by { g }
			}"#,
		)
		.expect_err("a grouped aggregate must not accept window::start");
	let message = err.to_string();
	assert!(message.contains("FLOW_013") || message.contains("FLOW_015"), "expected a rejection, got: {message}");
}
