// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Windows seal off the flow watermark, the minimum over every source feeding the flow.

use std::{thread::sleep, time::Duration as StdDuration};

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::{
	assert::{assert_same_timed_rows, timed_rows},
	db::TestDb,
};

const TIMEOUT: StdDuration = StdDuration::from_secs(5);

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

/// Two tables feeding one tumbling window through APPEND, so the flow watermark is a real
/// min-merge rather than a single source's own progress.
fn two_source_window(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::fast { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin("CREATE TABLE app::slow { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, total: int8 } with { time: event } AS {
			FROM app::fast APPEND { FROM app::slow }
				| window tumbling { total: math::sum(v) }
					with { interval: "1s", grace: "0s" }
					by { g }
		}"#);
}

#[test]
fn a_window_cannot_seal_ahead_of_the_slowest_source_feeding_it() {
	// Sealing consumes the flow watermark, the min over sources, so a source racing ahead must not
	// close a bucket a quiet sibling still feeds. A sealed window keeps the aggregate it already
	// published, so admission - offering the bucket another row - is the only observable.
	let db = setup();
	two_source_window(&db);

	db.command(r#"INSERT app::fast [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::w FILTER { total == 5 }", 1, TIMEOUT);

	// app::fast runs past bucket 0's horizon, but min(10s, 0) is still 0, so nothing may seal.
	db.command(r#"INSERT app::fast [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:10Z" }]"#);
	db.await_row_count("FROM app::w FILTER { total == 7 }", 1, TIMEOUT);

	db.command(r#"INSERT app::fast [{ id: 3, g: 1, v: 100, ts: "2026-01-01T00:00:00.500Z" }]"#);
	let admitted = db.await_exact_row_count("FROM app::w FILTER { total == 105 }", 1, TIMEOUT);
	assert_eq!(
		admitted,
		1,
		"bucket 0 refused a row while app::slow was still at zero, so it sealed ahead of the slowest \
		 source; view now: {:?}",
		db.query_as_root("FROM app::w", ())
	);

	// The slow source finally reports, lifting the min past bucket 0's seal instant.
	db.command(r#"INSERT app::slow [{ id: 4, g: 1, v: 9, ts: "2026-01-01T00:00:10Z" }]"#);
	db.await_row_count("FROM app::w FILTER { total == 16 }", 1, TIMEOUT);

	db.command(r#"INSERT app::fast [{ id: 5, g: 1, v: 1000, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_all_flows(TIMEOUT);

	let sealed = db.await_exact_row_count("FROM app::w FILTER { total == 105 }", 1, TIMEOUT);
	assert_eq!(
		sealed,
		1,
		"bucket 0 must seal once every source has passed its horizon, refusing the 1000 instead of \
		 folding it in; view now: {:?}",
		db.query_as_root("FROM app::w", ())
	);
}

#[test]
fn a_source_that_never_reports_holds_every_window_open() {
	// A source hydrates its watermark to zero, not to now, so a flow holding a source nobody writes
	// must keep every window open rather than sealing on the sources that are moving - otherwise a
	// misconfigured pipeline looks healthy while publishing partial aggregates.
	let db = setup();
	two_source_window(&db);

	db.command(r#"INSERT app::fast [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::w FILTER { total == 5 }", 1, TIMEOUT);

	for (id, at) in [(2, "00:01:00"), (3, "00:05:00"), (4, "01:00:00")] {
		db.command(&format!(r#"INSERT app::fast [{{ id: {id}, g: 1, v: 1, ts: "2026-01-01T{at}Z" }}]"#));
	}
	db.await_all_flows(TIMEOUT);

	let live = db.await_exact_row_count("FROM app::w FILTER { total == 5 }", 1, TIMEOUT);
	assert_eq!(
		live,
		1,
		"an hour of progress on one source sealed a window while a sibling source had never reported; \
		 view now: {:?}",
		db.query_as_root("FROM app::w", ())
	);
}

/// One table feeding a tumbling window, with the grace left to the caller so a corpus can be run
/// either entirely inside the windows' horizons or past them.
fn ordering_pair(grace: &str) -> (TestDb, TestDb) {
	let pair = (setup(), setup());
	for db in [&pair.0, &pair.1] {
		db.admin("CREATE NAMESPACE app");
		db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
		db.admin(&format!(
			r#"CREATE DEFERRED VIEW app::w {{ g: int4, total: int8 }} with {{ time: event }} AS {{
				FROM app::t
					| window tumbling {{ total: math::sum(v) }}
						with {{ interval: "1s", grace: "{grace}" }}
						by {{ g }}
			}}"#
		));
	}
	pair
}

const ORDERING_CORPUS: [(i32, i32, i32, &str); 4] = [
	(1, 1, 3, "2026-01-01T00:00:00.100Z"),
	(2, 1, 4, "2026-01-01T00:00:00.900Z"),
	(3, 1, 5, "2026-01-01T00:00:01.100Z"),
	(4, 2, 6, "2026-01-01T00:00:01.900Z"),
];

fn insert_row(db: &TestDb, (id, g, v, ts): (i32, i32, i32, &str)) {
	db.command(&format!(r#"INSERT app::t [{{ id: {id}, g: {g}, v: {v}, ts: "{ts}" }}]"#));
}

fn view_rows(db: &TestDb) -> Vec<reifydb::Frame> {
	db.query_as_root("FROM app::w | map { g, total }", ()).expect("query view")
}

#[test]
fn two_arrival_orders_of_the_same_corpus_produce_the_same_open_windows() {
	// Bucketing, grouping, aggregation and the #time stamp must be pure functions of the data, so
	// a different arrival order lands the same windows with the same stamps. The 10s grace holds
	// every horizon open, so admission cannot depend on order either.
	let (forward, reverse) = ordering_pair("10s");

	for row in ORDERING_CORPUS {
		insert_row(&forward, row);
	}
	for row in ORDERING_CORPUS.into_iter().rev() {
		insert_row(&reverse, row);
	}
	forward.await_all_flows(TIMEOUT);
	reverse.await_all_flows(TIMEOUT);

	assert_eq!(
		forward.await_exact_row_count("FROM app::w", 3, TIMEOUT),
		3,
		"the corpus must leave three windows for the comparison below to mean anything; got {:?}",
		timed_rows(&view_rows(&forward))
	);
	assert_eq!(
		forward.await_exact_row_count("FROM app::w FILTER { total == 7 }", 1, TIMEOUT),
		1,
		"bucket 0 must hold both of its contributions, or the two-contributor case this test rests on \
		 never happened; got {:?}",
		timed_rows(&view_rows(&forward))
	);
	assert_same_timed_rows(&view_rows(&forward), &view_rows(&reverse));
}

#[test]
fn a_transaction_is_one_arrival_so_no_row_in_it_is_late_against_its_own_siblings() {
	// A commit is one unit of arrival: its rows carry no order against each other, so admission
	// must read the watermark as it stood before the commit. That is what lets a bulk load carry
	// an hour of history into a 1s window instead of keeping only its newest bucket.
	let (batched, split) = ordering_pair("0s");

	batched.command(
		r#"INSERT app::t [
			{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00.500Z" },
			{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:10Z" }
		]"#,
	);
	batched.await_all_flows(TIMEOUT);

	assert_eq!(
		batched.await_exact_row_count("FROM app::w FILTER { total == 5 }", 1, TIMEOUT),
		1,
		"both rows arrived in one transaction, so the 10s row must not have sealed its sibling's \
		 bucket; view now: {:?}",
		timed_rows(&view_rows(&batched))
	);

	insert_row(&split, (1, 1, 7, "2026-01-01T00:00:10Z"));
	split.await_all_flows(TIMEOUT);
	insert_row(&split, (2, 1, 5, "2026-01-01T00:00:00.500Z"));
	split.await_all_flows(TIMEOUT);

	assert_eq!(
		split.await_exact_row_count("FROM app::w FILTER { total == 5 }", 0, TIMEOUT),
		0,
		"committed after the 10s row, the same row is genuinely late and must be refused, or the \
		 batched half above proves nothing; view now: {:?}",
		timed_rows(&view_rows(&split))
	);
}

#[test]
fn whether_a_sealed_bucket_was_published_at_all_depends_on_arrival_order() {
	// A sealed window keeps the aggregate it already published, so whether a bucket was published
	// before its horizon passed follows the commit log. Determinism is per-log: a different commit
	// order is a different input, and only late data makes that difference visible.
	let (forward, reverse) = ordering_pair("0s");

	for row in ORDERING_CORPUS {
		insert_row(&forward, row);
	}
	for row in ORDERING_CORPUS.into_iter().rev() {
		insert_row(&reverse, row);
	}
	forward.await_all_flows(TIMEOUT);
	reverse.await_all_flows(TIMEOUT);

	assert_eq!(
		forward.await_exact_row_count("FROM app::w FILTER { total == 7 }", 1, TIMEOUT),
		1,
		"forward admitted bucket 0 before its 1.001s horizon, so its sealed window must still hold 7; \
		 view now: {:?}",
		timed_rows(&view_rows(&forward))
	);
	assert_eq!(
		reverse.await_exact_row_count("FROM app::w FILTER { total == 7 }", 0, TIMEOUT),
		0,
		"reverse saw bucket 0's rows only after the 1.9s row had sealed it, so the bucket must never \
		 have been published; view now: {:?}",
		timed_rows(&view_rows(&reverse))
	);

	let open = "FROM app::w FILTER { total == 5 or total == 6 } | map { g, total }";
	assert_eq!(
		forward.await_exact_row_count(open, 2, TIMEOUT),
		2,
		"the bucket at 1s never sealed, so both its groups must be present in forward; view now: {:?}",
		timed_rows(&view_rows(&forward))
	);
	assert_same_timed_rows(
		&forward.query_as_root(open, ()).expect("query view"),
		&reverse.query_as_root(open, ()).expect("query view"),
	);
}

#[test]
fn a_processing_window_rolls_on_arrival_time_while_an_event_window_stays_open() {
	// Two views over the same table and the same rows, differing only in declared domain, so any
	// divergence between them is the domain and nothing else. A processing-time watermark derives
	// from the rows' arrival stamps - it holds while idle and moves when rows arrive - while an
	// event-time one moves only on the declared ts and must stay open.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::p { g: int4, total: int8 } with { time: processing } AS {
			FROM app::t
				| window tumbling { total: math::sum(v) }
					with { interval: "2s", grace: "0s" }
					by { g }
		}"#);
	db.admin(r#"CREATE DEFERRED VIEW app::e { g: int4, total: int8 } with { time: event } AS {
			FROM app::t
				| window tumbling { total: math::sum(v) }
					with { interval: "2s", grace: "0s" }
					by { g }
		}"#);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::e", 1, TIMEOUT);

	// Both windows must materialize first, or the counts below pass for a window never there.
	assert_eq!(
		db.await_row_count("FROM app::p", 1, TIMEOUT),
		1,
		"the processing window never published a live row, so the roll assertion would be vacuous"
	);

	// Not a synchronisation wait - the quiet interval separates the two arrivals by more than the
	// 2s window, so the second row's arrival stamp lands in a different bucket than the first's.
	sleep(StdDuration::from_millis(2_500));

	// Same event time as the first row: still inside the event window, but its arrival stamp is
	// 2.5s past the first row's, outside the processing one.
	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_all_flows(TIMEOUT);

	let rolled = db.await_exact_row_count("FROM app::p", 2, TIMEOUT);
	assert_eq!(
		rolled,
		2,
		"a processing-time window must stop accepting once the arrival watermark leaves it, so the \
		 second row belongs to a new window rather than the first one; view now: {:?}",
		db.query_as_root("FROM app::p", ())
	);

	let held = db.await_exact_row_count("FROM app::e FILTER { total == 12 }", 1, TIMEOUT);
	assert_eq!(
		held,
		1,
		"an event-time window whose source stopped reporting must stay open however long the wall clock \
		 runs, and still admit a row carrying its own event time; view now: {:?}",
		db.query_as_root("FROM app::e", ())
	);
}

#[test]
fn a_session_that_keeps_extending_seals_only_after_its_final_gap() {
	// A session's seal instant moves every time the session extends, so the armed timer must be
	// disarmed; a stale one fires mid-stream and publishes a partial aggregate. Every row here
	// lands inside the previous row's 2s gap, so all four must survive as one window.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::s { g: int4, total: int8 } with { time: event } AS {
			FROM app::t
				| window session { total: math::sum(v) }
					with { gap: "2s", grace: "0s" }
					by { g }
		}"#);

	for (id, at) in [(1, "00:00:00"), (2, "00:00:01"), (3, "00:00:02"), (4, "00:00:03")] {
		db.command(&format!(r#"INSERT app::t [{{ id: {id}, g: 1, v: 10, ts: "2026-01-01T{at}Z" }}]"#));
		db.await_all_flows(TIMEOUT);
	}

	let whole = db.await_exact_row_count("FROM app::s FILTER { total == 40 }", 1, TIMEOUT);
	assert_eq!(
		whole,
		1,
		"four rows one second apart inside a two second gap are ONE session totalling 40; a split means \
		 a superseded seal timer fired; view now: {:?}",
		db.query_as_root("FROM app::s", ())
	);
}

#[test]
fn a_row_at_the_epoch_is_refused_once_its_window_has_sealed() {
	// Zero is a legitimate coordinate, not a "no value" marker. A row at the Unix epoch reaching a
	// window that holds no state yet must be refused like any other late row, not waved through by
	// a zero-means-unknown shortcut. Its window [0s, 1s) sealed at 4.001s, long before it arrives.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, total: int8 } with { time: event } AS {
			FROM app::t
				| window tumbling { total: math::sum(v) }
					with { interval: "1s", grace: "3s" }
					by { g }
		}"#);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 10, ts: "1970-01-01T00:00:20Z" }]"#);
	db.await_row_count("FROM app::w FILTER { total == 10 }", 1, TIMEOUT);

	// Carries the watermark past the 20s window's 24.001s seal instant, so it actually advances.
	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 20, ts: "1970-01-01T00:00:30Z" }]"#);
	db.await_exact_row_count("FROM app::w FILTER { total == 10 }", 0, TIMEOUT);

	db.command(r#"INSERT app::t [{ id: 3, g: 1, v: 99, ts: "1970-01-01T00:00:00Z" }]"#);
	db.await_all_flows(TIMEOUT);

	let epoch_window = db.await_exact_row_count("FROM app::w FILTER { total == 99 }", 0, TIMEOUT);
	assert_eq!(
		epoch_window,
		0,
		"a row at coordinate 0 belongs to a window that sealed at 4.001s and must be refused like any \
		 other late row; view now: {:?}",
		timed_rows(&db.query_as_root("FROM app::w", ()).expect("query view"))
	);
}
