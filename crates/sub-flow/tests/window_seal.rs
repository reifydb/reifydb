// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Windows seal off the flow watermark, which is the MINIMUM over every source feeding the flow.
// These tests pin the consequences that a node-local watermark would get wrong, and the determinism
// that makes a replay reproduce byte-identical output.

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
	// Intent: sealing consumes the flow watermark, which is the min over sources, so a fast
	// source racing ahead must NOT seal a window whose other source has not caught up - the
	// window would otherwise refuse the rows that were still in flight and leave a published
	// aggregate over truncated input.
	// app::slow is never written in the first half, so its watermark sits at zero and pins the
	// flow watermark at zero however far app::fast runs.
	// The assertion is on ADMISSION, not on a watermark and not on a row count. Sealing closes a
	// window to new rows and reclaims its accumulator but leaves the aggregate it already
	// published standing, so the only way to observe whether bucket 0 sealed is to offer it
	// another row for the same bucket and see whether the total moves.
	// Mutation: seal off the node's own max event time instead of the flow watermark (what the
	// pre-timer code did) and the 10s insert closes bucket 0 while app::slow is still empty, so
	// the 100 that was still in flight is refused and total stays 5.
	let db = setup();
	two_source_window(&db);

	db.command(r#"INSERT app::fast [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::w FILTER { total == 5 }", 1, TIMEOUT);

	// Runs app::fast far past bucket 0's horizon. Its own watermark is now 10s, but the flow
	// watermark is min(10s, 0) = 0, so nothing may seal.
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
	// Intent: replay determinism, over the windows that are still open. Bucketing, grouping,
	// aggregation and the #time stamp must all be pure functions of the data, so the same rows
	// delivered in a different ORDER land the same windows with the same totals and the same
	// stamps. A ten second grace holds every horizon past the corpus's 1.9s watermark, so nothing
	// seals while it runs and admission cannot depend on order either.
	// Bucket 0 taking two rows is what makes this non-vacuous: forward delivers 0.100 then 0.900,
	// reverse the other way round, so anything that stamped a window with its first or last
	// arrival rather than its start, or that let arrival order pick the bucket, diverges here.
	// Sealed windows are deliberately NOT compared - see
	// whether_a_sealed_bucket_was_published_at_all_depends_on_arrival_order.
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
	// Intent: a commit is a unit of simultaneous arrival. The rows in it carry no order relative
	// to each other, so none of them may seal a bucket that another one still needs, and the
	// admission gate must read the watermark as it stood BEFORE the commit rather than after.
	// This is what makes a bulk load work: one transaction carrying an hour of history into a 1s
	// window lands in full, where reading the post-commit watermark would jump the frontier to
	// the newest row and refuse every bucket but the last.
	// The two halves here are the same two rows and differ only in transaction boundary. Batched,
	// the 0.5s row is a sibling of the 10s row and is admitted. Split with the 10s row first, it
	// is genuinely late and is refused - that difference is inherent, because the split order is
	// a different log, and the point of this test is that the BATCHED half stays permissive.
	// Mutation: move set_flow_watermark in process_version below the loop that advances the
	// source watermarks and the batched half starts refusing its own sibling, so total 5
	// disappears and every multi-bucket bulk load silently keeps only its newest bucket.
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
	// Intent: pin an ACCEPTED trade-off so it cannot change silently. Sealing a window closes it
	// to new rows and reclaims its accumulator but leaves the aggregate it already published
	// standing. That makes "was this bucket published before its horizon passed" observable, and
	// that answer depends on arrival order: forward delivers bucket 0's rows while the watermark
	// is still under 1s so it publishes 7 and later seals holding it, while reverse delivers the
	// 1.9s row first and both of bucket 0's rows are then refused as late, so bucket 0 never
	// exists at all. The open bucket at 1s is identical either way.
	// Withdrawing a sealed window used to hide this, because a bucket that sealed left no trace
	// whether or not it had been published. Retaining it is the deliberate choice; the cost is
	// that a replay in a different order can differ on sealed buckets.
	// If this test ever starts finding the two views equal, retraction on seal is back and
	// two_arrival_orders_of_the_same_corpus_produce_the_same_open_windows should be widened to
	// cover sealed buckets again.
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
fn a_processing_window_rolls_on_the_clock_while_an_event_window_stays_open() {
	// Intent: both directions of what the domain means for an idle flow, in one place because
	// each is the other's control. Two views over the SAME table and the SAME two rows, differing
	// only in declared domain, so any divergence between them is the domain and nothing else.
	// A processing-time flow's watermark IS the wall clock, so a quiet interval carries it past
	// the open window's horizon and the second row can only land in a new window. An event-time
	// flow's watermark only moves on data, so an identical window over identical rows must stay
	// open however long the wall clock runs, and the second row - carrying the first row's event
	// time - must still fold into it. If it did not, time was being read from the wall clock
	// somewhere in the event path.
	// Sealing no longer withdraws a published row, so "the processing window drained" is not
	// observable here any more; what is observable is that it stopped accepting. The timer-wheel
	// half of the old assertion (process_tick keeps dispatching while idle, so a quiet flow still
	// reclaims state) has no view-level consequence left and is covered by
	// event_silence_holds_while_processing_silence_advances in flow/src/transaction/watermark.rs.
	// Mutation: make the event path fall back to the clock and app::e rolls to two windows just
	// like app::p; freeze the processing path at the last coordinate and app::p folds into one.
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

	// Both windows must actually materialize first, or the counts below would pass for a window
	// that was never there.
	assert_eq!(
		db.await_row_count("FROM app::p", 1, TIMEOUT),
		1,
		"the processing window never published a live row, so the roll assertion would be vacuous"
	);

	// Not a synchronisation wait - it is the quiet interval the test is about. A processing-time
	// window can only be seen to close by letting the wall clock, which IS its watermark, cross
	// the 2s interval; nothing arrives to advance it and no view row changes when it does.
	sleep(StdDuration::from_millis(2_500));

	// The same event time as the first row. The event window that holds it is still open, the
	// processing window that held it closed while the clock ran.
	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_all_flows(TIMEOUT);

	let rolled = db.await_exact_row_count("FROM app::p", 2, TIMEOUT);
	assert_eq!(
		rolled,
		2,
		"a processing-time window must stop accepting once the clock leaves it, so the second row \
		 belongs to a new window rather than the first one; view now: {:?}",
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

#[test]
fn a_row_at_the_epoch_is_refused_once_its_window_has_sealed() {
	// Intent: zero is a legitimate coordinate, not a "no value" marker. The admission gate used to
	// derive a bucket's event time as max(prior_last, batch_last) and skip the seal check entirely
	// when that came out zero - which is exactly what a row at the Unix epoch produces in a window
	// that has no state yet. Such a row was admitted into a window that had sealed long before,
	// and the view published an aggregate for it.
	// The window here is [0s, 1s) with a seal instant of 0 + 1s + 3s + 1ms. The first two rows
	// carry the watermark past that instant, so by the time the epoch row arrives its window is
	// unambiguously closed.
	// Mutation: restore the `if last == 0 { continue }` guard in gate_and_arm_seals and total 99
	// appears in the view.
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

	// Carries the watermark to 30s, past the 24.001s seal instant of the 20s window, so the ledger
	// actually advances rather than sitting at zero.
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
