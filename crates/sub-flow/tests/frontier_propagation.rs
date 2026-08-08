// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A view that carries no rows must still lift its readers' watermark through the frontier it publishes.

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};

const TIMEOUT: StdDuration = StdDuration::from_secs(5);

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

/// One commit plus a barrier on the flow consumer watermark. A frontier crosses exactly one hop per
/// round, because a flow only runs a slice when a new version arrives and `resolve` withholds an
/// entry stamped at the reader's own version, so a chain needs one of these per view hop before its
/// consumer can seal against anything.
fn settle_round(db: &TestDb, table: &str, id: i32, g: i32) {
	db.command(&format!(r#"INSERT {table} [{{ id: {id}, g: {g}, v: 1, ts: "2026-01-01T00:00:10Z" }}]"#));
	assert!(
		db.await_all_flows(TIMEOUT),
		"every flow must reach the committed version, or this is not a barrier and the chain may still \
		 be a hop behind"
	);
}

/// A busy table and a table nobody writes to, each behind its own view, appended into one tumbling
/// window, so the window's watermark is a min over two view sources and the silent side can only
/// move through the frontier its producing view publishes.
fn window_behind_two_view_hops(db: &TestDb) {
	db.admin("CREATE NAMESPACE sil");
	db.admin("CREATE TABLE sil::busy { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE sil::quiet { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(
		"CREATE DEFERRED VIEW sil::mid_busy { id: int4, g: int4, v: int4, ts: datetime } AS { FROM sil::busy }",
	);
	db.admin(
		"CREATE DEFERRED VIEW sil::mid_quiet { id: int4, g: int4, v: int4, ts: datetime } AS { FROM sil::quiet }",
	);
	db.admin(r#"CREATE DEFERRED VIEW sil::w { g: int4, total: int8 } AS {
			FROM sil::mid_busy APPEND { FROM sil::mid_quiet }
				| window tumbling { total: math::sum(v) }
					with { interval: "1s", grace: "0s" }
					by { g }
		}"#);
}

/// The same window reading both tables directly, so a completeness assertion reaches it without a
/// view hop in between and isolates the hop from the assertion itself.
fn window_over_two_tables(db: &TestDb) {
	db.admin("CREATE NAMESPACE dir");
	db.admin("CREATE TABLE dir::busy { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE dir::quiet { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW dir::w { g: int4, total: int8 } AS {
			FROM dir::busy APPEND { FROM dir::quiet }
				| window tumbling { total: math::sum(v) }
					with { interval: "1s", grace: "0s" }
					by { g }
		}"#);
}

#[test]
fn a_complete_silent_table_lifts_the_window_reading_it_directly() {
	// Declaring a source complete must move its watermark exactly as a row at that instant would, otherwise a
	// source that legitimately has nothing to say holds every window it feeds open forever.
	let db = setup();
	window_over_two_tables(&db);

	db.command(r#"INSERT dir::busy [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM dir::w FILTER { total == 5 }", 1, TIMEOUT);

	db.command(r#"INSERT dir::busy [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:10Z" }]"#);
	db.await_row_count("FROM dir::w FILTER { total == 7 }", 1, TIMEOUT);

	db.command(r#"INSERT dir::busy [{ id: 3, g: 1, v: 100, ts: "2026-01-01T00:00:00.500Z" }]"#);
	assert_eq!(
		db.await_exact_row_count("FROM dir::w FILTER { total == 105 }", 1, TIMEOUT),
		1,
		"bucket 0 refused a row while dir::quiet was still at the epoch; view now: {:?}",
		db.query("FROM dir::w")
	);

	db.admin("call system::source::complete_through(dir::quiet, cast('2026-01-01T00:00:10Z', datetime))");
	db.command(r#"INSERT dir::busy [{ id: 4, g: 1, v: 1000, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.await_exact_row_count("FROM dir::w FILTER { total == 105 }", 1, TIMEOUT),
		1,
		"dir::quiet is complete through 10s, so bucket 0 must seal and refuse the 1000; view now: {:?}",
		db.query("FROM dir::w")
	);
}

#[test]
fn a_silent_view_source_lifts_its_reader_across_the_hop() {
	// A rowless view reports only through its published frontier, so without that hop its reader pins at the epoch
	// forever while the underlying table is complete; admission of a late row is the observable, since an open
	// bucket folds it in and a sealed one refuses it.
	let db = setup();
	window_behind_two_view_hops(&db);

	db.command(r#"INSERT sil::busy [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM sil::w FILTER { total == 5 }", 1, TIMEOUT);

	// sil::busy runs past bucket 0's horizon, but sil::mid_quiet has never reported, so the min stays 0.
	db.command(r#"INSERT sil::busy [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:10Z" }]"#);
	db.await_row_count("FROM sil::w FILTER { total == 7 }", 1, TIMEOUT);

	db.command(r#"INSERT sil::busy [{ id: 3, g: 1, v: 100, ts: "2026-01-01T00:00:00.500Z" }]"#);
	assert_eq!(
		db.await_exact_row_count("FROM sil::w FILTER { total == 105 }", 1, TIMEOUT),
		1,
		"bucket 0 refused a row while sil::mid_quiet had never reported, so it sealed on the busy side \
		 alone; view now: {:?}",
		db.query("FROM sil::w")
	);

	// The silent table emits no row when it goes complete, so only a frontier can carry that through
	// sil::mid_quiet.
	db.admin("call system::source::complete_through(sil::quiet, cast('2026-01-01T00:00:10Z', datetime))");
	// One view hop between the silent table and the window, so one round before the late row.
	settle_round(&db, "sil::busy", 5, 2);
	db.command(r#"INSERT sil::busy [{ id: 4, g: 1, v: 1000, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.await_exact_row_count("FROM sil::w FILTER { total == 105 }", 1, TIMEOUT),
		1,
		"sil::quiet is complete through 10s, so sil::mid_quiet's frontier must cross the hop and seal \
		 bucket 0 against the 1000; view now: {:?}",
		db.query("FROM sil::w")
	);
}

/// A silent table two views deep, so the frontier has to be republished by each hop in turn rather
/// than read once off the table by the window at the bottom.
fn window_behind_a_two_view_chain(db: &TestDb) {
	db.admin("CREATE NAMESPACE deep");
	db.admin("CREATE TABLE deep::busy { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE deep::quiet { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(
		"CREATE DEFERRED VIEW deep::mid_busy { id: int4, g: int4, v: int4, ts: datetime } AS { FROM deep::busy }",
	);
	db.admin("CREATE DEFERRED VIEW deep::q1 { id: int4, g: int4, v: int4, ts: datetime } AS { FROM deep::quiet }");
	db.admin("CREATE DEFERRED VIEW deep::q2 { id: int4, g: int4, v: int4, ts: datetime } AS { FROM deep::q1 }");
	db.admin(r#"CREATE DEFERRED VIEW deep::w { g: int4, total: int8 } AS {
			FROM deep::mid_busy APPEND { FROM deep::q2 }
				| window tumbling { total: math::sum(v) }
					with { interval: "1s", grace: "0s" }
					by { g }
		}"#);
}

#[test]
fn a_frontier_converges_down_a_two_hop_chain() {
	// Each hop must republish what it resolved rather than pass the table's own progress straight through, so a
	// chain converges only if every intermediate view folds its producer's frontier into its own hold.
	let db = setup();
	window_behind_a_two_view_chain(&db);

	db.command(r#"INSERT deep::busy [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM deep::w FILTER { total == 5 }", 1, TIMEOUT);

	db.command(r#"INSERT deep::busy [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:10Z" }]"#);
	db.await_row_count("FROM deep::w FILTER { total == 7 }", 1, TIMEOUT);

	db.command(r#"INSERT deep::busy [{ id: 3, g: 1, v: 100, ts: "2026-01-01T00:00:00.500Z" }]"#);
	assert_eq!(
		db.await_exact_row_count("FROM deep::w FILTER { total == 105 }", 1, TIMEOUT),
		1,
		"bucket 0 refused a row while deep::q2 had never reported; view now: {:?}",
		db.query("FROM deep::w")
	);

	db.admin("call system::source::complete_through(deep::quiet, cast('2026-01-01T00:00:10Z', datetime))");
	// Two view hops between the silent table and the window, so two rounds before the late row.
	settle_round(&db, "deep::busy", 5, 2);
	settle_round(&db, "deep::busy", 6, 3);
	db.command(r#"INSERT deep::busy [{ id: 4, g: 1, v: 1000, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.await_exact_row_count("FROM deep::w FILTER { total == 105 }", 1, TIMEOUT),
		1,
		"deep::quiet is complete through 10s, so the frontier must converge down both hops and seal \
		 bucket 0 against the 1000; view now: {:?}",
		db.query("FROM deep::w")
	);
}
