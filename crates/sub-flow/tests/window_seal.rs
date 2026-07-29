// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Windows seal off the flow watermark, which is the MINIMUM over every source feeding the flow.
// These tests pin the consequences that a node-local watermark would get wrong, and the determinism
// that makes a replay reproduce byte-identical output.

use std::time::Duration as StdDuration;

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
	// Intent: THE property phase 3 exists to make structural (Q5). Sealing consumes the flow
	// watermark, which is the min over sources, so a fast source racing ahead must NOT seal a
	// window whose other source has not caught up - the window would otherwise publish an
	// aggregate over truncated input and then refuse the rows that were still in flight.
	// app::slow is never written in the first half, so its watermark sits at zero and pins the
	// flow watermark at zero however far app::fast runs.
	// The assertion is on the window's EMITTED OUTPUT, not on a watermark: a watermark-only
	// check would still pass if the window sealed early and published a partial aggregate.
	// Mutation: seal off the node's own max event time instead of the flow watermark (what the
	// pre-timer code did) and bucket 0 is withdrawn by the second insert, so total == 5
	// disappears while app::slow is still empty.
	let db = setup();
	two_source_window(&db);

	db.command(r#"INSERT app::fast [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::w FILTER { total == 5 }", 1, TIMEOUT);

	// Runs app::fast far past bucket 0's horizon. Its own watermark is now 10s, but the flow
	// watermark is min(10s, 0) = 0, so nothing may seal.
	db.command(r#"INSERT app::fast [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:10Z" }]"#);
	db.await_row_count("FROM app::w FILTER { total == 7 }", 1, TIMEOUT);

	let live = db.await_exact_row_count("FROM app::w FILTER { total == 5 }", 1, TIMEOUT);
	assert_eq!(
		live,
		1,
		"bucket 0 sealed while app::slow was still at zero, so the window sealed ahead of the slowest \
		 source; view now: {:?}",
		db.query_as_root("FROM app::w", ())
	);

	// The slow source finally reports, lifting the min past bucket 0's seal instant.
	db.command(r#"INSERT app::slow [{ id: 3, g: 1, v: 9, ts: "2026-01-01T00:00:10Z" }]"#);

	let sealed = db.await_exact_row_count("FROM app::w FILTER { total == 5 }", 0, TIMEOUT);
	assert_eq!(
		sealed,
		0,
		"bucket 0 must seal once every source has passed its horizon; view now: {:?}",
		db.query_as_root("FROM app::w", ())
	);
}

#[test]
fn a_source_that_never_reports_holds_every_window_open() {
	// Intent: the sharpest regression trap in the min-merge. A source hydrates its watermark to
	// zero, not to now, so a flow that gains a source nobody writes must hold every window open
	// forever rather than silently sealing on the sources that ARE moving. If this inverts, a
	// misconfigured pipeline looks healthy while quietly publishing partial aggregates.
	// Mutation: make flow_watermark skip sources with no persisted watermark (treat "never
	// written" as "no constraint") and bucket 0 seals here.
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

#[test]
fn two_arrival_orders_of_the_same_corpus_produce_the_same_windows() {
	// Intent: replay determinism. Timers fire in (at, node, kind, key) order off a watermark
	// that is a pure function of the data, and a bucket stamps #time with its start rather than
	// with whatever arrived in it. So the same rows delivered in a different ORDER must land the
	// same windows with the same totals - otherwise a replay diverges from the original run and
	// retention decisions stop being reproducible.
	// The corpus spans two buckets and the watermark ends past bucket 0's horizon, so bucket 0
	// seals and buckets 1 stay live. Both halves matter: a reordering that leaked arrival order
	// into bucketing would change a total, and one that leaked it into sealing would change
	// WHICH bucket survives.
	// Mutation: stamp a bucket with its max contributor's event time instead of its start, or
	// seal off arrival order, and the two views stop matching.
	let forward = setup();
	let reverse = setup();
	for db in [&forward, &reverse] {
		db.admin("CREATE NAMESPACE app");
		db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
		db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, total: int8 } with { time: event } AS {
				FROM app::t
					| window tumbling { total: math::sum(v) }
						with { interval: "1s", grace: "0s" }
						by { g }
			}"#);
	}

	let rows = [
		(1, 1, 3, "2026-01-01T00:00:00.100Z"),
		(2, 1, 4, "2026-01-01T00:00:00.900Z"),
		(3, 1, 5, "2026-01-01T00:00:01.100Z"),
		(4, 2, 6, "2026-01-01T00:00:01.900Z"),
	];
	let insert = |db: &TestDb, (id, g, v, ts): (i32, i32, i32, &str)| {
		db.command(&format!(r#"INSERT app::t [{{ id: {id}, g: {g}, v: {v}, ts: "{ts}" }}]"#));
	};

	for row in rows {
		insert(&forward, row);
	}
	for row in rows.into_iter().rev() {
		insert(&reverse, row);
	}
	forward.await_all_flows(TIMEOUT);
	reverse.await_all_flows(TIMEOUT);

	let live = |db: &TestDb| db.query_as_root("FROM app::w | map { g, total }", ()).expect("query view");
	assert_eq!(
		forward.await_exact_row_count("FROM app::w", 2, TIMEOUT),
		2,
		"the corpus must leave two live windows for the comparison below to mean anything; got {:?}",
		timed_rows(&live(&forward))
	);
	assert_same_timed_rows(&live(&forward), &live(&reverse));
}

#[test]
fn a_processing_window_drains_while_idle_and_an_event_window_holds() {
	// Intent: both directions of what the domain means for an idle flow, in one place because
	// each is the other's control.
	// A processing-time flow's watermark IS the clock, so its windows must keep sealing after the
	// input goes quiet - that only works because process_tick pumps the timer wheel. An
	// event-time flow's watermark only moves on data, so an identical window over identical rows
	// must stay open indefinitely; sealing it would mean time was being read from the wall clock
	// somewhere in the event path, which is exactly the class of bug this phase removes.
	// Mutation: drop the dispatch_due_timers call from process_tick and the processing window
	// never drains; make the event path fall back to the clock and the event window drains too.
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

	// Both windows must actually materialize first, or "drained to zero" below would pass for a
	// window that was never there.
	assert_eq!(
		db.await_row_count("FROM app::p", 1, TIMEOUT),
		1,
		"the processing window never published a live row, so the drain assertion would be vacuous"
	);

	let drained = db.await_exact_row_count("FROM app::p", 0, TIMEOUT);
	assert_eq!(
		drained,
		0,
		"a processing-time window must keep sealing once its input goes quiet, because its watermark is \
		 the clock; view now: {:?}",
		db.query_as_root("FROM app::p", ())
	);

	let held = db.await_exact_row_count("FROM app::e", 1, TIMEOUT);
	assert_eq!(
		held,
		1,
		"an event-time window whose source stopped reporting must stay open however long the wall clock \
		 runs; view now: {:?}",
		db.query_as_root("FROM app::e", ())
	);
}

#[test]
fn a_session_that_keeps_extending_seals_only_after_its_final_gap() {
	// Intent: a session's seal instant MOVES every time the session extends, so the previously
	// armed timer has to be disarmed. Without the disarm the stale instant still fires and seals
	// the session mid-stream, cutting it short and publishing a partial aggregate while more
	// rows for it were still arriving.
	// Each row here lands inside the previous row's 2s gap, so the session keeps extending and
	// its seal instant keeps moving; the session must survive all of them as one window.
	// Mutation: drop the disarm in gate_and_arm_seals and the timer armed for the first row
	// fires at its original instant, splitting the session.
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
