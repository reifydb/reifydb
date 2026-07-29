// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The rolling window operator keeps its engine (running accumulators + group meta caches) on the
// operator instance across batches instead of rebuilding it cold on every apply. Each INSERT below
// is its own commit, so each is a separate batch through the flow: the totals after batch N+1 are
// only correct if the state carried over from batch N (warm cache or store) agrees exactly with
// what batch N committed. A double-merge (event applied to both the cached running accumulator and
// re-scanned from the store), a stale meta high_water (event silently dropped as sealed), or a
// missed eviction would all surface here as a wrong total or a wrong row count.

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

const TIMEOUT: StdDuration = StdDuration::from_secs(5);

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

fn rejection(db: &TestDb, rql: &str) -> Option<String> {
	match db.try_admin(rql) {
		Ok(_) => None,
		Err(err) => Some(err.diagnostic().code),
	}
}

#[test]
fn rolling_sum_accumulates_correctly_across_separate_commits() {
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::r { g: int4, total: float8 } with { time: event } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m" }
					by { g }
		}"#);

	let insert = |g: i32, v: f64, ts: &str| {
		db.command(&format!("INSERT app::t [{{ g: {g}, v: {v}, ts: \"{ts}\" }}]"));
	};
	let await_total = |g: i32, total: f64| {
		let rql = format!("FROM app::r | filter {{ g == {g} and total == {total} }}");
		let got = db.await_row_count(&rql, 1, StdDuration::from_secs(5));
		assert_eq!(
			got,
			1,
			"group {g} must roll up to {total} from state carried across commits; view now: {:?}",
			db.query_as_root("FROM app::r", ())
		);
	};

	// Batch 1 creates the group's running state from scratch.
	insert(1, 10.0, "2026-01-01T00:00:00Z");
	await_total(1, 10.0);

	// Batch 2 must fold into the state left behind by batch 1, not restart from zero
	// (missed carry-over) and not count batch 1 twice (double-merge on reload).
	insert(1, 5.0, "2026-01-01T00:01:00Z");
	await_total(1, 15.0);

	// An unrelated group gets its own accumulator without disturbing group 1.
	insert(2, 7.0, "2026-01-01T00:01:00Z");
	await_total(2, 7.0);
	await_total(1, 15.0);

	// A third batch for group 1 keeps compounding on the twice-carried state.
	insert(1, 3.0, "2026-01-01T00:02:00Z");
	await_total(1, 18.0);

	// One materialized row per group: updates must rewrite the group's row, not append.
	let rows = db.row_count("FROM app::r");
	assert_eq!(rows, 2, "rolling view must hold exactly one row per group, got {rows}");
}

#[test]
fn a_processing_domain_rolling_window_rolls_up_over_the_rows_own_times() {
	// A processing-time rolling window used to run a whole separate engine: it bucketed on the row
	// NUMBER and stashed a batch-wide wall-clock read inside the accumulator, because
	// resolve_event_timestamps replaced every row's #time with one `now` per apply and a shared
	// coordinate would have collapsed the batch into a single buffer slot.
	// The substrate already stamps #time on every row in both domains (a processing row's #time is
	// its arrival), so that path was working around a problem it created for itself. It now routes
	// through the same engine as event time.
	// This is the only processing-domain coverage the rolling operator has; without it the reroute
	// is unguarded and a carry-over or double-merge fault on that path shows up only in production.
	// Mutation: send WindowKind::Rolling back through the stamped engine and the multi-group and
	// multi-batch totals below stop agreeing.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8 }");
	db.admin(r#"CREATE DEFERRED VIEW app::p { g: int4, total: float8 } with { time: processing } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m" }
					by { g }
		}"#);

	let insert = |g: i32, v: f64| {
		db.command(&format!("INSERT app::t [{{ g: {g}, v: {v} }}]"));
	};
	let await_total = |g: i32, total: f64| {
		let rql = format!("FROM app::p | filter {{ g == {g} and total == {total} }}");
		let got = db.await_row_count(&rql, 1, StdDuration::from_secs(5));
		assert_eq!(
			got,
			1,
			"group {g} must roll up to {total} on the unified engine; view now: {:?}",
			db.query_as_root("FROM app::p", ())
		);
	};

	insert(1, 10.0);
	await_total(1, 10.0);

	// The second batch must fold into the first's state. On the old stamped engine this was the
	// path with no test at all.
	insert(1, 5.0);
	await_total(1, 15.0);

	// A second group must get its own accumulator without disturbing the first.
	insert(2, 7.0);
	await_total(2, 7.0);
	await_total(1, 15.0);

	// Several rows in ONE commit: the interesting case, because this is the batch that used to
	// share a single coordinate. All three must be counted, not collapsed to one contribution.
	db.command("INSERT app::t [{ g: 1, v: 1.0 }, { g: 1, v: 2.0 }, { g: 1, v: 3.0 }]");
	await_total(1, 21.0);

	let rows = db.row_count("FROM app::p");
	assert_eq!(rows, 2, "rolling view must hold exactly one row per group, got {rows}");
}

#[test]
fn a_processing_view_over_an_event_source_buckets_by_arrival_not_by_the_declared_column() {
	// The combination the previous attempt at this got wrong, and the one the other test in this
	// file does not reach: a view may declare `time: processing` over a table that declares
	// `ts: ts`, and that is a deliberate re-timing boundary - the view is saying "ignore what the
	// row claims happened, bucket it by when it got here".
	// The table's ts is deliberately years in the past while the rows arrive now. A processing
	// flow's watermark IS the clock, so a window coordinate taken from `ts` sits far behind the
	// eviction cutoff and the row is evicted on the first timer, leaving the view empty. Taking it
	// from arrival keeps the row live. The gap has to be larger than the window interval for the
	// two answers to differ, hence 2020 against a 1h window.
	// Mutation: drop the processing-domain re-stamp in seed_entry_nodes and this view drains to
	// zero while the event view below still holds, because only one of them reads `ts`.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::p { g: int4, total: float8 } with { time: processing } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m" }
					by { g }
		}"#);
	db.admin(r#"CREATE DEFERRED VIEW app::e { g: int4, total: float8 } with { time: event } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m" }
					by { g }
		}"#);

	db.command(r#"INSERT app::t [{ g: 1, v: 4.0, ts: "2020-03-01T00:00:00Z" }]"#);

	// Both must materialize first, or "still holds" below would pass for a view that was never
	// populated at all.
	assert_eq!(
		db.await_row_count("FROM app::p | filter { total == 4.0 }", 1, StdDuration::from_secs(5)),
		1,
		"the processing view must bucket the row by its arrival, not by a 2020 ts; view now: {:?}",
		db.query_as_root("FROM app::p", ())
	);
	assert_eq!(
		db.await_row_count("FROM app::e | filter { total == 4.0 }", 1, StdDuration::from_secs(5)),
		1,
		"the event view must bucket the row by its declared ts; view now: {:?}",
		db.query_as_root("FROM app::e", ())
	);

	// The processing view must KEEP the row: a coordinate taken from `ts` would fall behind the
	// clock-driven cutoff and be evicted, which is exactly how the wrong time shows up.
	let held = db.await_exact_row_count("FROM app::p | filter { total == 4.0 }", 1, StdDuration::from_secs(2));
	assert_eq!(
		held,
		1,
		"a row that arrived just now must stay inside a 1h rolling window; view now: {:?}",
		db.query_as_root("FROM app::p", ())
	);
}

#[test]
fn an_event_view_over_an_event_source_buckets_by_the_declared_column_not_by_arrival() {
	// The control for the test above, and the other legal cell of the domain grid. Same table, same
	// window, only the view's declared domain differs - so a rule that re-stamped every flow rather
	// than only processing-domain flows passes the previous test and fails this one.
	// Both rows arrive in ONE commit, so their arrival times are identical, but their declared ts
	// values are five hours apart. Bucketing by ts puts them outside a shared 1h rolling span and
	// the older one ages out; bucketing by arrival keeps both. The totals therefore disagree, which
	// is what makes this an assertion about WHICH time was used rather than about liveness.
	// Mutation: re-stamp unconditionally in seed_entry_nodes instead of only for Processing, and
	// both rows stay in span so the total settles at 9.0 instead of 5.0.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::e { g: int4, total: float8 } with { time: event } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m" }
					by { g }
		}"#);

	db.command(
		r#"INSERT app::t [
			{ g: 1, v: 4.0, ts: "2020-03-01T00:00:00Z" },
			{ g: 1, v: 5.0, ts: "2020-03-01T05:00:00Z" }
		]"#,
	);

	let settled = db.await_row_count("FROM app::e | filter { total == 5.0 }", 1, StdDuration::from_secs(5));
	assert_eq!(
		settled,
		1,
		"the 00:00 row sits more than 1h+5m behind the 05:00 row's event time and must age out of \
		 the rolling span, leaving 5.0; a total of 9.0 means both rows were bucketed by arrival \
		 instead of by ts. view now: {:?}",
		db.query_as_root("FROM app::e", ())
	);
}

#[test]
fn a_processing_view_over_a_processing_source_keeps_its_rows_live() {
	// The third legal cell: the source declares no ts, so its #time is already arrival and the
	// boundary re-stamp is a no-op. That is exactly why it is worth asserting - a re-stamp that
	// read the wrong field, or read an unpopulated one, would write epoch here rather than arrival
	// and every row would sit ~56 years behind the clock-driven eviction cutoff.
	// So this test fails loudly on the one failure mode a no-op cannot otherwise reveal: it checks
	// the rows stay LIVE, which only holds if the stamped value really is close to the clock.
	// Mutation: stamp from a default/empty DateTime instead of created_at and the view drains to
	// zero on the first timer.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8 }");
	db.admin(r#"CREATE DEFERRED VIEW app::p { g: int4, total: float8 } with { time: processing } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m" }
					by { g }
		}"#);

	db.command("INSERT app::t [{ g: 1, v: 6.0 }]");
	assert_eq!(
		db.await_row_count("FROM app::p | filter { total == 6.0 }", 1, StdDuration::from_secs(5)),
		1,
		"view now: {:?}",
		db.query_as_root("FROM app::p", ())
	);

	let held = db.await_exact_row_count("FROM app::p | filter { total == 6.0 }", 1, StdDuration::from_secs(2));
	assert_eq!(
		held,
		1,
		"a row stamped with its arrival must stay inside a 1h rolling window; draining to zero means \
		 the stamp was epoch, not arrival. view now: {:?}",
		db.query_as_root("FROM app::p", ())
	);
}

#[test]
fn an_event_view_over_a_processing_source_is_refused() {
	// The fourth cell of the grid is not legal, and refusing it is what makes the other three
	// meaningful: a source that declares no ts has no event time to offer, so a view that claims
	// `time: event` over it would silently bucket by whatever the substrate happened to stamp.
	// reconcile_time_domain rejects (Some(Event), Processing) outright rather than letting the flow
	// register and mis-bucket, so this asserts the DDL fails rather than asserting on any data.
	// Mutation: relax that arm to Ok(()) and the view registers, then buckets arrival-stamped rows
	// as though they were event times - the exact confusion the whole domain check exists to stop.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8 }");

	assert_eq!(
		rejection(
			&db,
			r#"CREATE DEFERRED VIEW app::e { g: int4, total: float8 } with { time: event } AS {
				FROM app::t
					| window rolling { total: math::sum(v) }
						with { interval: "1h", grace: "5m" }
						by { g }
			}"#
		)
		.as_deref(),
		Some("FLOW_040"),
		"an event-time view over a source with no declared ts must be refused at creation"
	);
}

#[test]
fn a_row_too_late_to_admit_does_not_delete_the_group_it_belongs_to() {
	// Intent: a late row is supposed to be IGNORED. It used to withdraw the whole group instead.
	// apply_rolling drops a late row's bucket before the engine runs, but the group stayed in
	// `touched`, and finish_rolling_results read "in touched, produced no result" as "this group
	// is now empty" and emitted a Diff::remove for its live aggregate. A single stray old
	// timestamp could therefore delete a healthy group's rolling total.
	// The 1h window with 5m grace admits anything at or after (watermark - 65m); the row at
	// 09:00 is well outside that once the watermark reaches 12:00, so it is refused - and group 1
	// must be exactly as it was.
	// Mutation: drop the `touched.retain(...)` after the sealed-bucket sweep in apply_rolling and
	// group 1 disappears from the view entirely.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: float8, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::r { g: int4, total: float8 } with { time: event } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m" }
					by { g }
		}"#);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 10.0, ts: "2026-01-01T12:00:00Z" }]"#);
	db.await_row_count("FROM app::r | filter { g == 1 and total == 10.0 }", 1, TIMEOUT);

	// Three hours behind the watermark, so past interval + grace and refused as late.
	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 99.0, ts: "2026-01-01T09:00:00Z" }]"#);
	db.await_all_flows(TIMEOUT);

	let held = db.await_exact_row_count("FROM app::r | filter { g == 1 and total == 10.0 }", 1, TIMEOUT);
	assert_eq!(
		held,
		1,
		"a refused row must leave the group's rolling total untouched, not withdraw it; view now: {:?}",
		db.query_as_root("FROM app::r", ())
	);
}

#[test]
fn retracting_a_row_that_has_already_left_the_window_leaves_the_group_intact() {
	// Intent: the same "no result means gone" confusion reached through the other door. Grace is
	// wider than the interval, so a coordinate can be new enough to ADMIT (>= watermark - 65m)
	// while already being older than the trailing window (< watermark - 60m). Retracting such a
	// row is a genuine no-op: the engine returns no result because nothing in the window changed.
	// finish_rolling_results used to withdraw the group on exactly that silence.
	// The row at 11:30 is admitted at watermark 12:00, then the row at 13:00 pushes the window to
	// [12:00, 13:00) so 11:30 falls out of it; deleting 11:30 afterwards must change nothing.
	// Mutation: restore the `for hash in touched { ... Diff::remove ... }` fallback in
	// finish_rolling_results and the group vanishes on the delete.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: float8, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW app::r { g: int4, total: float8 } with { time: event } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { interval: "1h", grace: "5m" }
					by { g }
		}"#);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 4.0, ts: "2026-01-01T11:30:00Z" }]"#);
	db.await_row_count("FROM app::r | filter { g == 1 and total == 4.0 }", 1, TIMEOUT);

	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 6.0, ts: "2026-01-01T13:00:00Z" }]"#);
	let rolled = db.await_exact_row_count("FROM app::r | filter { g == 1 and total == 6.0 }", 1, TIMEOUT);
	assert_eq!(
		rolled,
		1,
		"the 11:30 contribution must fall out of a 1h window ending at 13:00, leaving 6.0; view now: {:?}",
		db.query_as_root("FROM app::r", ())
	);

	// 11:30 is still inside interval + grace of the 13:00 watermark, so this delete is admitted
	// and routed - but it targets a coordinate the window no longer holds.
	db.command("DELETE app::t FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	let intact = db.await_exact_row_count("FROM app::r | filter { g == 1 and total == 6.0 }", 1, TIMEOUT);
	assert_eq!(
		intact,
		1,
		"retracting a contribution that already left the window is a no-op, so the group must keep \
		 its total rather than be withdrawn; view now: {:?}",
		db.query_as_root("FROM app::r", ())
	);
}
