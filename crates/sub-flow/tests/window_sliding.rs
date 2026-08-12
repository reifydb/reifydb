// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A sliding window is identified by the coordinate it starts at, not by its position in the
// slide sequence. These tests pin the three places that distinction is observable: which windows
// a row lands in, what a window stamps #time with, and when a window seals.

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::{column_values, timed_rows};
use reifydb_value::value::Value;

const TIMEOUT: StdDuration = StdDuration::from_secs(5);

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

/// One table feeding a sliding window whose size is a whole number of slides, so the window a
/// coordinate belongs to can be worked out by hand. Window starts are multiples of the slide in
/// absolute epoch millis, so a row early in a day is still covered by windows from the previous one.
fn sliding_window(db: &TestDb, size: &str, slide: &str, seal: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(&format!(r#"CREATE DEFERRED VIEW app::w {{ g: int4, total: int8 }} AS {{
				FROM app::t
					| window sliding {{ total: math::sum(v) }}
						with {{ interval: "{size}", slide: "{slide}", seal: "{seal}" }}
						by {{ g }}
			}}"#));
}

#[test]
fn a_row_lands_in_every_window_that_covers_it() {
	// Overlap is the whole point: at size 60s and slide 15s the windows starting 15s, 30s, 45s and
	// 60s cover t=70s while the one starting at 0s ends before it. The 120s seal is load-bearing -
	// every wrongly-covering window is an older one, and at zero seal it has already sealed.
	let db = setup();
	sliding_window(&db, "60s", "15s", "120s");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 7, ts: "2026-01-01T00:01:10Z" }]"#);
	db.await_all_flows(TIMEOUT);

	let rows = db.await_exact_row_count("FROM app::w", 4, TIMEOUT);
	assert_eq!(
		rows,
		4,
		"a row at 70s must land in exactly the four 60s windows that cover it (starting at 15s, 30s, \
		 45s, 60s); view now: {:?}",
		timed_rows(&db.query_as_root("FROM app::w", ()).expect("query view"))
	);

	let frames = db.query_as_root("FROM app::w | map { total }", ()).expect("query view");
	assert_eq!(
		column_values(&frames[0], "total"),
		vec![Value::Int8(7); 4],
		"every covering window holds the same single contribution"
	);
}

#[test]
fn a_sliding_window_stamps_time_with_its_start_not_its_index() {
	// #time is where a window identified by its slide index rather than its start coordinate is
	// directly observable: an index is a small integer everything downstream reads as millis, so
	// the stamps would collapse to within four milliseconds of the epoch.
	let db = setup();
	sliding_window(&db, "60s", "15s", "0s");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 7, ts: "2026-01-01T00:01:10Z" }]"#);
	db.await_exact_row_count("FROM app::w", 4, TIMEOUT);

	let frames = db.query_as_root("FROM app::w | map { g }", ()).expect("query view");
	let mut stamped: Vec<String> = timed_rows(&frames).into_iter().map(|row| row.time.to_string()).collect();
	stamped.sort();

	assert_eq!(
		stamped,
		vec![
			"2026-01-01T00:00:15.000000000Z".to_string(),
			"2026-01-01T00:00:30.000000000Z".to_string(),
			"2026-01-01T00:00:45.000000000Z".to_string(),
			"2026-01-01T00:01:00.000000000Z".to_string(),
		],
		"each sliding window must stamp #time with its own start coordinate"
	);
}

#[test]
fn a_sliding_window_seals_size_plus_seal_after_its_start() {
	// Each window's seal horizon is its own start plus size plus seal, so overlapping windows
	// holding the identical row seal at different times. A global seal cannot produce a partial
	// result, so the 7-versus-107 split is what a count check would miss.
	let db = setup();
	sliding_window(&db, "60s", "15s", "30s");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 7, ts: "2026-01-01T00:00:00Z" }]"#);
	let covering = db.await_exact_row_count("FROM app::w", 4, TIMEOUT);
	assert_eq!(
		covering,
		4,
		"a row at T0 is covered by the four 60s windows starting T0-45s through T0; view now: {:?}",
		timed_rows(&db.query_as_root("FROM app::w", ()).expect("query view"))
	);

	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 5, ts: "2026-01-01T00:01:10Z" }]"#);
	db.await_exact_row_count("FROM app::w", 8, TIMEOUT);

	db.command(r#"INSERT app::t [{ id: 3, g: 1, v: 100, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_all_flows(TIMEOUT);

	let frames = db.query_as_root("FROM app::w | map { total }", ()).expect("query view");
	let mut totals = column_values(&frames[0], "total");
	totals.sort_by_key(|value| format!("{value:?}"));
	assert_eq!(
		totals,
		vec![
			Value::Int8(107),
			Value::Int8(107),
			Value::Int8(5),
			Value::Int8(5),
			Value::Int8(5),
			Value::Int8(5),
			Value::Int8(7),
			Value::Int8(7)
		],
		"two of the first row's four windows must still admit a row at T0 while the other two refuse it, \
		 because their starts are 30s apart and the watermark landed between their horizons"
	);

	let open = db.query_as_root("FROM app::w FILTER { total == 107 } | map { g }", ()).expect("query view");
	let mut stamped: Vec<String> = timed_rows(&open).into_iter().map(|row| row.time.to_string()).collect();
	stamped.sort();
	assert_eq!(
		stamped,
		vec!["2025-12-31T23:59:45.000000000Z".to_string(), "2026-01-01T00:00:00.000000000Z".to_string(),],
		"the two that took the row must be exactly the latest-starting windows, the ones whose horizons \
		 the watermark has not yet reached"
	);
}
