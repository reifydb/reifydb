// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A sliding window is identified by the coordinate it starts at, not by its position in the
// slide sequence. These tests pin the three places that distinction is observable: which windows
// a row lands in, what a window stamps #time with, and when a window seals.

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::{
	assert::{column_values, timed_rows},
	db::TestDb,
};
use reifydb_value::value::Value;

const TIMEOUT: StdDuration = StdDuration::from_secs(5);

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

/// One table feeding a sliding window whose size is a whole number of slides, so the window a
/// coordinate belongs to can be worked out by hand.
///
/// Window starts are multiples of the slide in absolute epoch milliseconds, NOT offsets from
/// whatever date the test picked. 2026-01-01T00:00:00Z happens to be a multiple of 15s, which is
/// why the offsets below read cleanly - a row early in that day is still covered by windows that
/// started on the previous one.
fn sliding_window(db: &TestDb, size: &str, slide: &str, grace: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin(&format!(r#"CREATE DEFERRED VIEW app::w {{ g: int4, total: int8 }} with {{ time: event }} AS {{
				FROM app::t
					| window sliding {{ total: math::sum(v) }}
						with {{ interval: "{size}", slide: "{slide}", grace: "{grace}" }}
						by {{ g }}
			}}"#));
}

#[test]
fn a_row_lands_in_every_window_that_covers_it() {
	// Intent: overlap is the entire point of a sliding window. With size 60s and slide 15s the
	// windows starting at 15s, 30s, 45s and 60s all cover t=70s, while the one starting at 0s
	// ends at 60s and does not. Four rows, one per covering window, all carrying the same total.
	// Mutation: drop the containment filter in sliding_window_anchors and the window starting at
	// 0s appears too, making it five. Emit only one window and it collapses to tumbling.
	let db = setup();
	sliding_window(&db, "60s", "15s", "0s");

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
	// Intent: THE regression that sliding's coordinate fix exists for. A window used to be
	// identified by its slide index, so span.start was a small integer that everything
	// downstream read as milliseconds. #time is where that is directly observable: the four
	// windows covering t=70s must be stamped 15s, 30s, 45s and 60s.
	// Mutation: identify a window by its index again and every #time collapses to within four
	// milliseconds of the Unix epoch, because the indices are 1, 2, 3 and 4.
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
fn a_sliding_window_seals_size_plus_grace_after_its_start() {
	// Intent: each window's seal horizon is its OWN start plus size plus grace, so overlapping
	// windows holding the identical row must seal at different times. That is what makes this
	// stronger than a count check: a global seal cannot produce a partial survival.
	// The row at T0 lands in the four windows starting T0-45s, T0-30s, T0-15s and T0. With
	// size 60s and grace 30s they close at T0+46s, T0+61s, T0+76s and T0+91s respectively. The
	// second row lifts the watermark to T0+70s, which is past the first two instants and short
	// of the last two, so exactly two of the four must survive.
	// Mutation: measure the horizon from the slide index instead of the start coordinate and
	// every window seals on the first watermark past size + grace, so all four vanish together
	// and only the second row's windows remain.
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
	db.await_exact_row_count("FROM app::w", 6, TIMEOUT);

	let frames = db.query_as_root("FROM app::w | map { total }", ()).expect("query view");
	let mut totals = column_values(&frames[0], "total");
	totals.sort_by_key(|value| format!("{value:?}"));
	assert_eq!(
		totals,
		vec![Value::Int8(5), Value::Int8(5), Value::Int8(5), Value::Int8(5), Value::Int8(7), Value::Int8(7)],
		"two of the first row's four windows must outlive the other two, because their starts are \
		 30s apart and the watermark landed between their horizons"
	);

	let survivors = db.query_as_root("FROM app::w FILTER { total == 7 } | map { g }", ()).expect("query view");
	let mut stamped: Vec<String> = timed_rows(&survivors).into_iter().map(|row| row.time.to_string()).collect();
	stamped.sort();
	assert_eq!(
		stamped,
		vec!["2025-12-31T23:59:45.000000000Z".to_string(), "2026-01-01T00:00:00.000000000Z".to_string(),],
		"the two survivors must be exactly the latest-starting windows, the ones whose horizons the \
		 watermark has not yet reached"
	);
}
